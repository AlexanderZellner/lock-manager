#include <unordered_map>
#include "moderndbs/lock_manager.h"
#include "cassert"

namespace moderndbs {

    // get lock from aquireLock
    // insert lock into vector
void Transaction::addLock(DataItem item, LockMode mode) {
    // get new Lock with mode
    std::shared_ptr<Lock> lock = lockManager->acquireLock(*this, item, mode);
    assert(lockManager->getLockMode(item) == mode);
    // add to lock list
    assert(lock != nullptr);
    locks.push_back(lock);
}

Transaction::~Transaction() {
    while (!this->locks.empty()) {
        auto lock = locks.back();

        locks.pop_back();
        lockManager->save_destruct(*this,std::move(lock));
    }
}

void LockManager::save_destruct(const Transaction &transaction, std::shared_ptr<Lock> &&lock) {
    Hash hash;
    auto bucket = hash(lock->item) % 1024;

    Chain& chain = const_cast<Chain &>(table.at(bucket));
    chain.latch.lock();

    auto count_before = lock->owners.size();
    lock->owners.erase(std::find(lock->owners.begin(), lock->owners.end(), &transaction));
    assert(count_before > lock->owners.size());

    if (lock->ownership == LockMode::Exclusive) {
        lock->ownership = LockMode::Unlocked;
        lock->lock.unlock();
    } else {
        if (lock->owners.size() == 0) {
            lock->ownership = LockMode::Unlocked;
        }
        lock->lock.unlock_shared();
    }

    {
        auto scope_lock = std::move(lock);
    }
    chain.latch.unlock();
}
// 1. Add new edges to Graph
// 2. Check for cycle
// 3. throw error if needed
void WaitsForGraph::addWaitsFor(const Transaction &transaction, const Lock &lock) {
    std::unique_lock graph_latch(latch);
    if (current_nodes.find(&transaction) == current_nodes.end()) {
        // create new node if needed
        Node f_node = Node(num_nodes, transaction);
        current_nodes.insert(std::make_pair(&transaction, f_node));
        adj.resize(num_nodes + 1);
        adj.insert(adj.begin() + num_nodes++, std::list<Node>());
    }

    Node from_node = current_nodes.find(&transaction)->second;

    for(const Transaction* owner: lock.owners) {
        if (current_nodes.find(owner) == current_nodes.end()) {
            // to-node not in graph yet
            Node t_node = Node(num_nodes, *owner);
            current_nodes.insert(std::make_pair(owner, t_node));
            adj.insert(adj.begin() + num_nodes++, std::list<Node>());
        }
        Node to_node = current_nodes.find(owner)->second;
        adj[from_node.transaction_id].push_back(to_node);

        if (checkForCycle()) {
            // reset graph to valid state
            removeTransaction(transaction);
            throw DeadLockError();
        }
    }
}

void WaitsForGraph::removeTransaction(const Transaction &transaction) {
    // remove from current_nodes
    auto to_erase = current_nodes.find(&transaction);
    current_nodes.erase(to_erase);
    // remove from adj
    assert(adj.size() > to_erase->second.transaction_id);
    adj.erase(adj.begin() + to_erase->second.transaction_id);
    // delete occururance from waiting for lists
    for (auto node_list : adj) {
        auto i = node_list.begin();
        for (auto node : node_list) {
            if (node.transaction_id == to_erase->second.transaction_id) {
                // same -> need to be erased
                node_list.erase(i);
            }
            ++i;
        }
    }
    num_nodes--;
}

void WaitsForGraph::remove_save(const Transaction &transaction) {
    std::unique_lock graph_latch(latch);
    removeTransaction(transaction);
}

// Runs DFS in graph -> check for cycle
/// @return true for cycle, false for no cycle
bool WaitsForGraph::checkForCycle() {
    std::shared_ptr<bool> visited (new bool[num_nodes]);
    std::shared_ptr<bool> recStack (new bool[num_nodes]);

    for (uint16_t i = 1; i < num_nodes; ++i) {
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

bool WaitsForGraph::updateWaitsFor(const Transaction &waiting_t, Transaction &holding_t) {
    // The old holding transaction finished
    // -> old waiters need to wait for the currently active transaction
    std::unique_lock graph_latch (latch);
    auto position = current_nodes.find(&waiting_t);
    if (position == current_nodes.end()) {
        // node is not in graph yet
        return false;
    } else {
        auto from_node = position->second;
        auto to_pos = current_nodes.find(&holding_t);
        if (to_pos == current_nodes.end()) {
            // to node is not in graph
            Node t_node = Node(++num_nodes, holding_t);
            current_nodes.insert(std::make_pair(&holding_t, t_node));
            adj.insert(adj.begin() + num_nodes++, std::list<Node>());
            to_pos = current_nodes.find(&holding_t);
        }
        auto to_node = to_pos->second;
        adj[from_node.transaction_id].push_back(to_node);
    }

    if (checkForCycle()) {
        // Should not happen
        throw DeadLockError();
    }
    return true;
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
    std::unique_lock chain_latch(chain.latch);

    auto lock = chain.first;
    Lock* prevLock = nullptr;
    // lock chain
    while (true) {
        if (lock == nullptr) {
            // add new lock
            std::shared_ptr<Lock> new_lock = lock->construct();

            if (mode == LockMode::Exclusive) {
                new_lock->lock.lock();
            } else {
                new_lock->lock.lock_shared();
            }
            new_lock->ownership = mode;
            new_lock->item = dataItem;
            new_lock->owners.push_back(&transaction);
            new_lock->next = chain.first;

            chain.first = new_lock.get();

            return new_lock;
        }
        //assert(lock->ownership != LockMode::Unlocked && lock->owners.size() > 0 || lock->ownership == LockMode::Unlocked);
        if (lock->isExpired()) {
            // lock is expired -> no active pointer
            if (prevLock != nullptr) {
                // not start
                prevLock->next = lock->next;
                delete lock;
                lock = prevLock->next;
            } else {
                // at start
                chain.first = lock->next;
                delete lock;
                lock = chain.first;
            }
            continue;
        }
        // lock still active
        if (lock->item == dataItem) {
            auto new_ptr = lock->shared_from_this();
            assert(!lock->isExpired());

            //assert(lock->owners.size() > 0);
            // found correct item
            if (new_ptr->ownership == LockMode::Shared && mode == LockMode::Shared) {
                // shared | shared
                chain_latch.unlock();
                new_ptr->lock.lock_shared();
                chain_latch.lock();
                new_ptr->owners.push_back(&transaction);
                return new_ptr;

            } else if (new_ptr->ownership == LockMode::Exclusive && mode == LockMode::Shared){
                // exclusive | shared
                wfg.addWaitsFor(transaction, *lock);

                new_ptr->waiting_queue.insert(&transaction);
                chain_latch.unlock();
                new_ptr->lock.lock_shared();
                chain_latch.lock();

                new_ptr->waiting_queue.erase(&transaction);
                // we got the lock
                // check waiting queue -> update graph
                wfg.remove_save(transaction);
                for (auto* waiting_trans : new_ptr->waiting_queue) {
                    bool worked = false;
                    try {
                        worked = wfg.updateWaitsFor(*waiting_trans, transaction);
                    } catch (DeadLockError&) {
                        assert(false);
                    }
                    if (!worked) {
                        // node was not in graph yet
                        wfg.addWaitsFor(*waiting_trans, *lock);
                    }
                }

                new_ptr->ownership = mode;
                new_ptr->owners.push_back(&transaction);

            } else {
                // exclusive | exclusive
                //assert(mode == LockMode::Exclusive);
                // need to wait since exclusive
                wfg.addWaitsFor(transaction, *lock);
                // no throw -> no deadlock
                new_ptr->waiting_queue.insert(&transaction);
                chain_latch.unlock();
                new_ptr->lock.lock();
                chain_latch.lock();
                new_ptr->waiting_queue.erase(&transaction);

                // we got the lock -> remove our self
                // Problem: we (T1) -> T2
                //              T3 -> T2
                // T2 finished ==> T3 -> "nothing"
                // check waiting queue -> update graph
                wfg.remove_save(transaction);
                for (auto* waiting_trans : new_ptr->waiting_queue) {
                    bool worked = false;
                    try {
                        worked = wfg.updateWaitsFor(*waiting_trans, transaction);
                    } catch (DeadLockError&) {
                        assert(false);
                    }
                    if (!worked) {
                        // node was not in graph yet
                        wfg.addWaitsFor(*waiting_trans, *lock);
                    }
                }

                assert(new_ptr->owners.size() == 0);

                new_ptr->ownership = mode;
                new_ptr->owners.push_back(&transaction);
            }
            assert(!new_ptr->isExpired());
            return new_ptr;
        }
        // item != dataItem
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
}

} // namespace moderndbs
