#include <unordered_map>
#include "moderndbs/lock_manager.h"
#include "cassert"
#include <algorithm>

namespace moderndbs {

    // get lock from aquireLock
    // insert lock into vector
void Transaction::addLock(DataItem item, LockMode mode) {
    // get new Lock with mode
    std::shared_ptr<Lock> lock = lockManager->acquireLock(*this, item, mode);
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

    auto& chain = const_cast<Chain &>(table.at(bucket));
    std::unique_lock chain_latch(chain.latch);

    auto count_before = lock->owners.size();
    lock->owners.erase(std::find(lock->owners.begin(), lock->owners.end(), &transaction));
    assert(count_before > lock->owners.size());
    if (lock->ownership == LockMode::Exclusive) {
        assert(lock->owners.empty());
        lock->lock.unlock();
    } else if (lock->ownership == LockMode::Shared) {
        lock->lock.unlock_shared();
    } else {
        assert(lock->ownership != LockMode::Unlocked);
    }

    if (lock->owners.empty()) {
        lock->ownership = LockMode::Unlocked;
    }

    wfg.remove_save(transaction);
    {
        auto scope_lock = std::move(lock);
    }
}
// 1. Add new edges to Graph
// 2. Check for cycle
// 3. throw error if needed
void WaitsForGraph::addWaitsFor(const Transaction &transaction, const Lock &lock) {
    std::unique_lock graph_latch(latch);
    if (current_nodes.find(&transaction) == current_nodes.end()) {
        // create new node if needed
        assert(num_nodes == current_nodes.size());
        Node* f_node = new Node(num_nodes, transaction);
        current_nodes.insert(std::make_pair(&transaction, f_node));
        //consitencyCheck();
        adj.resize(num_nodes + 1);
        adj.insert(adj.begin() + num_nodes++,std::list<Node>());
    }

    Node* from_node = current_nodes.find(&transaction)->second;

    for(const Transaction* owner: lock.owners) {
        if (current_nodes.find(owner) == current_nodes.end()) {
            // to-node not in graph yet
            assert(num_nodes == current_nodes.size());
            Node* t_node = new Node(num_nodes, *owner);
            current_nodes.insert(std::make_pair(owner, t_node));
            //consitencyCheck();
            adj.resize(num_nodes + 1);
            adj.insert(adj.begin() + num_nodes++, std::list<Node>());
        }
        Node* to_node = current_nodes.find(owner)->second;
        assert(from_node != to_node);
        adj[from_node->transaction_id].push_back(*to_node);

        if (checkForCycle()) {
            // reset graph to valid state
            removeTransaction(transaction);
            delete from_node;
            throw DeadLockError();
        }
    }

    if (checkForCycle()) {
        // reset graph to valid state
        removeTransaction(transaction);
        throw DeadLockError();
    }
}

/// used to remove transactions causing a cycle
void WaitsForGraph::removeTransaction(const Transaction &transaction) {
    // remove from current_nodes
    auto to_erase = current_nodes.find(&transaction);
    if (to_erase == current_nodes.end()) {
        return;
    }
    assert(adj.size() > to_erase->second->transaction_id);
    uint16_t old_id = to_erase->second->transaction_id;
    current_nodes.erase(&transaction);
    for (auto node : current_nodes) {
        assert(node.second->transaction_id != old_id);
        if (node.second->transaction_id > old_id) {
            node.second->transaction_id -= 1;
        }
    }
    //consitencyCheck();
    // remove from adj
    adj.erase(adj.begin() + old_id);
    // delete occurrance from waiting for lists
    for (auto& node_list : adj) {
        auto i = node_list.begin();
        for (auto node : node_list) {
            if (node.transaction_id == old_id) {
                // same -> need to be erased
                node_list.erase(i);
            } else if (old_id < node.transaction_id) {
                node.transaction_id -= 1;
            }
            if (node_list.empty() || i == node_list.end()) {
                break;
            } else {
                ++i;
            }
        }
    }
    //consitencyCheck();
    num_nodes--;
    assert(current_nodes.size() == num_nodes);
}
/// only used when removing a transaction gracefully
void WaitsForGraph::remove_save(const Transaction &transaction) {
    std::unique_lock graph_latch(latch);
    // remove from current_nodes
    auto to_erase = current_nodes.find(&transaction);
    if (to_erase == current_nodes.end()) {
        return;
    }
    assert(adj.size() > to_erase->second->transaction_id);
    uint16_t old_id = to_erase->second->transaction_id;
    current_nodes.erase(&transaction);
    for (auto node : current_nodes) {
        assert(node.second->transaction_id != old_id);
        if (node.second->transaction_id > old_id) {
            node.second->transaction_id -= 1;
        }
    }
    //consitencyCheck();
    // remove from adj
    adj.erase(adj.begin() + old_id);
    // delete occurrance from waiting for lists
    for (auto& node_list : adj) {
        auto i = node_list.begin();
        for (auto node : node_list) {
            // update transaction ids
            // for every node we remove the id gets out of sync
            // T0 | T1 | T2 -> remove T0 = !T0! | !T1!
            //              -> remove T1 = T0 | !T1!
            // only do this when removed transaction id comes before current id
            if (node.transaction_id == old_id) {
                // same -> need to be erased
                node_list.erase(i);
            } else if (old_id < node.transaction_id) {
                node.transaction_id -= 1;
            }
            if (node_list.empty() || i == node_list.end()) {
                break;
            } else {
                ++i;
            }
        }
    }
    //consitencyCheck();
    num_nodes--;
    assert(current_nodes.size() == num_nodes);
}

// Runs DFS in graph -> check for cycle
/// @return true for cycle, false for no cycle
bool WaitsForGraph::checkForCycle() {
    std::shared_ptr<bool> visited (new bool[num_nodes] { false } );
    std::shared_ptr<bool> recStack (new bool[num_nodes] { false });

    for (uint16_t i = 0; i < num_nodes; ++i) {
        if (dfs(i, visited, recStack)) {
            return true;
        }
    }
    return false;
}

bool WaitsForGraph:: dfs(uint16_t id, const std::shared_ptr<bool>& visited, const std::shared_ptr<bool>& recStack) {
    if (!visited.get()[id]) {
        visited.get()[id] = true;
        recStack.get()[id] = true;

        std::list<Node>::iterator i;
        for (i = adj[id].begin(); i != adj[id].end(); ++i) {
            // Check if still a node
            if (current_nodes.find(&(*i).transaction) == current_nodes.end()) {
                continue;
            }
            auto to = current_nodes.find(&(*i).transaction)->second->transaction_id;
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
    //consitencyCheck();
    assert(!checkForCycle());
    auto position = current_nodes.find(&waiting_t);
    if (position == current_nodes.end()) {
        // create new node if needed
        assert(num_nodes == current_nodes.size());
        Node* f_node = new Node(num_nodes, waiting_t);
        current_nodes.insert(std::make_pair(&waiting_t, f_node));
        adj.resize(num_nodes + 1);
        adj.insert(adj.begin() + num_nodes++, std::list<Node>());
        position = current_nodes.find(&waiting_t);
    }
    //consitencyCheck();
    auto from_node = position->second;
    auto to_pos = current_nodes.find(&holding_t);
    if (to_pos == current_nodes.end()) {
        // to node is not in graph
        assert(num_nodes == current_nodes.size());
        Node* t_node = new Node(num_nodes, holding_t);
        current_nodes.insert(std::make_pair(&holding_t, t_node));
        adj.resize(num_nodes + 1);
        adj.insert(adj.begin() + num_nodes++, std::list<Node>());
        to_pos = current_nodes.find(&holding_t);
    }
    //consitencyCheck();
    auto to_node = to_pos->second;
    assert(&from_node != &to_node);
    assert(from_node->transaction_id == current_nodes.find(&waiting_t)->second->transaction_id);
    adj[from_node->transaction_id].push_back(*to_node);
    //consitencyCheck();
    if (checkForCycle()) {
        // Should not happen
        throw DeadLockError();
    }
    return true;
}


// for debugging
void WaitsForGraph::consitencyCheck() {
    std::vector<bool> found;
    found.resize(num_nodes + 1);
    for (auto node_outer: current_nodes) {
        for (auto node: current_nodes) {
            if (node.second->transaction_id == node_outer.second->transaction_id && node.first != node_outer.first) {
                assert(false);
            }
        }
        found.at(node_outer.second->transaction_id) = true;
    }

    for (uint16_t i = 0; i < current_nodes.size(); ++i) {
        // ensure no transaction is left out
        assert(found.at(i));
    }

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

    auto& chain = const_cast<Chain &>(table.at(bucket));
    std::unique_lock chain_latch(chain.latch);

    auto lock = chain.first;
    Lock* prevLock = nullptr;
    // lock chain
    while (true) {
        if (lock == nullptr) {
            // add new lock
            std::shared_ptr<Lock> new_lock = lock->construct();
            assert(new_lock->waiting_queue.empty());
            chain_latch.unlock();
            if (mode == LockMode::Exclusive) {
                new_lock->lock.lock();
            } else {
                new_lock->lock.lock_shared();
            }
            chain_latch.lock();
            assert(new_lock->waiting_queue.empty());
            new_lock->ownership = mode;
            new_lock->item = dataItem;
            new_lock->owners.push_back(&transaction);
            new_lock->next = chain.first;

            chain.first = new_lock.get();

            return new_lock;
        }
        //assert(lock->ownership != LockMode::Unlocked && lock->owners.size() > 0 || lock->ownership == LockMode::Unlocked);
        if (lock->isExpired()) {
            assert(lock->waiting_queue.empty() && lock->owners.empty());
            // lock is expired -> no active pointer
            if (prevLock != nullptr) {
                // not start
                prevLock->next = lock->next;
                lock = nullptr;
                delete lock;
                lock = prevLock->next;
            } else {
                // at start
                chain.first = lock->next;
                lock = nullptr;
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
                wfg.addWaitsFor(transaction, *lock);

                new_ptr->waiting_queue.insert(&transaction);
                chain_latch.unlock();
                new_ptr->lock.lock_shared();
                chain_latch.lock();
                new_ptr->waiting_queue.erase(&transaction);
                for (auto* waiting_trans : new_ptr->waiting_queue) {
                    try {
                        wfg.updateWaitsFor(*waiting_trans, transaction);
                    } catch (DeadLockError&) {
                        assert(false);
                    }
                }
                new_ptr->ownership = mode;
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
                //wfg.remove_save(transaction);
                for (auto* waiting_trans : new_ptr->waiting_queue) {
                    try {
                        wfg.updateWaitsFor(*waiting_trans, transaction);
                    } catch (DeadLockError&) {
                        assert(false);
                    }
                }
                new_ptr->ownership = mode;
                new_ptr->owners.push_back(&transaction);
            } else {
                // exclusive or shared | exclusive
                assert(mode != LockMode::Unlocked);
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
                //wfg.remove_save(transaction);
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
    auto&chain = const_cast<Chain &>(table.at(bucket));
    std::unique_lock chain_lock(chain.latch);

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
    Hash hash;
    auto bucket = hash(lock->item) % table.size();
    // Chain
    auto&chain = const_cast<Chain &>(table.at(bucket));
    std::unique_lock chain_latch(chain.latch);
    auto next = chain.first;
    while (next != nullptr) {
        if (next == lock) {
            // found lock
            delete next;
            return;
        }
        next = next->next;
    }
}

} // namespace moderndbs
