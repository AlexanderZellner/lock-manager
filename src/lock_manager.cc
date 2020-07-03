#include <unordered_map>
#include "moderndbs/lock_manager.h"

namespace moderndbs {

    // get lock from aquireLock
    // insert lock into vector
void Transaction::addLock(DataItem item, LockMode mode) {
    // get new Lock with mode
    std::shared_ptr<Lock> lock = lockManager->acquireLock(*this, item, mode);
    assert(lockManager->getLockMode(item) == mode);
    // add to lock list
    locks.push_back(lock);
}

Transaction::~Transaction() {
    // unlock everything
    for (auto lock : locks) {
        if (lock->ownership == LockMode::Exclusive) {
            lock->ownership = LockMode::Unlocked;
            lock->owners.clear();
            lock->lock.unlock();
        } else {
            std::find(lock->owners.begin(), lock->owners.end(), this);
            lock->ownership = LockMode::Unlocked;
            lock->lock.unlock_shared();
        }
    }
}

// 1. Add new edges to Graph
// 2. Check for cycle
// 3. throw error if needed
void WaitsForGraph::addWaitsFor(const Transaction &transaction, const Lock &lock) {
    latch.lock();
    if (current_nodes.find(&transaction) == current_nodes.end()) {
        // create new node if needed
        Node f_node = Node(++num_nodes, transaction);
        current_nodes.insert(std::make_pair(&transaction, f_node));
        adj.resize(num_nodes);
        adj.insert(adj.begin() + num_nodes - 1, std::list<Node>());
    }

    Node from_node = current_nodes.find(&transaction)->second;

    for(const Transaction* owner: lock.owners) {
        if (current_nodes.find(owner) == current_nodes.end()) {
            // to-node not in graph yet
            Node t_node = Node(++num_nodes, *owner);
            current_nodes.insert(std::make_pair(owner, t_node));
            adj.insert(adj.begin() + num_nodes - 1, std::list<Node>());
        }
        Node to_node = current_nodes.find(owner)->second;
        adj[from_node.transaction_id].push_back(to_node);

        if (checkForCycle()) {
            // reset graph to valid state
            removeTransaction(transaction);
            latch.unlock();
            throw DeadLockError();
        }
    }
    latch.unlock();
}

void WaitsForGraph::removeTransaction(const Transaction &transaction) {
    // remove from current_nodes
    auto to_erase = current_nodes.find(&transaction);
    current_nodes.erase(to_erase);
    // remove from adj
    adj.erase(adj.begin() + to_erase->second.transaction_id);
    num_nodes--;
}

// Runs DFS in graph -> check for cycle
/// @return true for cycle, false for no cycle
bool WaitsForGraph::checkForCycle() {
    std::shared_ptr<bool> visited (new bool[num_nodes]);
    std::shared_ptr<bool> recStack (new bool[num_nodes]);

    for (uint16_t i = 1; i <= num_nodes; ++i) {
        if (dfs(i, visited, recStack)) {
            return true;
        }
    }
    return false;
}

bool WaitsForGraph:: dfs(uint16_t id, std::shared_ptr<bool> visited, std::shared_ptr<bool> recStack) {
    if (!visited.get()[id]) {
        visited.get()[id] = true;
        recStack.get()[id] = true;

        std::list<Node>::iterator i;
        for (i = adj[id].begin(); i != adj[id].end(); ++i) {
            // Check if still a node
            if (current_nodes.find(&(*i).transaction) == current_nodes.end()) {
                adj[id].erase(i);
                continue;
            }
            auto to = (*i).transaction_id;
            if (!visited.get()[to] && dfs(to, visited, recStack)) {
                return true;
            } else if (recStack.get()[to]) {
                return true;
            }
        }
    }
    recStack.get()[id] = false;
    return false;
}

// check if item already locked
// use tryLock
// if yes check waitGraph -> wait for it
// else create lock -> insert into chain of dataItem
// call addLock on transaction
// return lock
std::shared_ptr<Lock> LockManager::acquireLock(Transaction &transaction, DataItem dataItem, LockMode mode) {
    Hash hash;
    auto bucket = hash(dataItem) % table.size();

    Chain& chain = const_cast<Chain &>(table.at(bucket));

    auto lock = chain.first;
    Lock* prevLock = nullptr;

    // lock chain
    chain.latch.lock();

    while (true) {
        if (lock == nullptr) {
            // add new lock
            lock = new Lock(dataItem);
            lock->ownership = LockMode::Unlocked;
            lock->next = nullptr;
            if (prevLock != nullptr) {
                prevLock->next = lock;
            } else {
                chain.first = lock;
            }
        } else if (lock->ownership == LockMode::Unlocked) {
            // lock is expired -> no active pointer
            if (prevLock == nullptr) {
                // first element in chain
                // TODO Delete lock
                if (lock->next == nullptr) {
                    // only one element in chain
                    lock = nullptr;
                    continue;
                }
                chain.first = lock->next;

            } else {
                if (lock->next = nullptr) {
                    // last element
                    prevLock->next = nullptr;
                    lock = nullptr;
                    continue;
                }
                prevLock->next = lock->next;
            }
        }
        // lock still active
        if (lock->item == dataItem) {
            // found correct item
            if (mode == lock->ownership) {
                // lock Item
                if (mode == LockMode::Exclusive) {
                    lock->lock.lock();
                } else {
                    lock->lock.lock_shared();
                }
                lock->ownership = mode;
                lock->owners.push_back(&transaction);
                lock->item = dataItem;
            } else {
                // need to wait
                wfg.addWaitsFor(transaction, *lock);
                // no throw -> no deadlock
                if (mode == LockMode::Exclusive) {
                    lock->lock.lock();
                } else {
                    lock->lock.lock_shared();
                }

                lock->ownership = mode;
                lock->owners.push_back(&transaction);
                lock->item = dataItem;
            }
            chain.latch.unlock();
            return std::shared_ptr<Lock>(lock);
        }
        prevLock = lock;
        lock = lock->next;
    }
}

// go to bucket in hashtable
// iterate over chain until dataItem found
// return ownership
LockMode LockManager::getLockMode(DataItem dataItem) const {
    Hash hash;
    auto bucket = hash(dataItem) % table.size();

    // Chain
    Chain& chain = const_cast<Chain &>(table.at(bucket));

    auto lock = chain.first;
    while (true) {
        if (lock->item == dataItem) {
            return lock->ownership;
        } else {
            // not correct dataItem
            lock = lock->next;
        }
    }
}

void LockManager::deleteLock(Lock *lock) {
  throw std::logic_error{"not implemented"};
}

} // namespace moderndbs
