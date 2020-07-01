#include <unordered_map>
#include "moderndbs/lock_manager.h"

namespace moderndbs {

void Transaction::addLock(DataItem item, LockMode mode) {
  throw std::logic_error{"not implemented"};
}

Transaction::~Transaction() {
}

// 1. Add new edges to Graph
// 2. Check for cycle
// 3. throw error if needed
void WaitsForGraph::addWaitsFor(const Transaction &transaction, const Lock &lock) {
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
            throw DeadLockError();
        }
    }
}

void WaitsForGraph::removeTransaction(const Transaction &transaction) {
  throw std::logic_error{"not implemented"};
}

// Runs DFS in graph -> check for cycle
/// @return true for cycle, false for no cycle
bool WaitsForGraph::checkForCycle() {
    bool *visited = new bool[num_nodes];
    bool *recStack = new bool [num_nodes];

    for (uint16_t i = 1; i <= num_nodes; ++i) {
        if (dfs(i, visited, recStack)) {
            return true;
        }
    }
    return false;
}

bool WaitsForGraph::dfs(uint16_t id, bool visited[], bool *recStack) {
    if (!visited[id]) {
        visited[id] = true;
        recStack[id] = true;

        std::list<Node>::iterator i;
        for (i = adj[id].begin(); i != adj[id].end(); ++i) {
            auto to = (*i).transaction_id;
            if (!visited[to] && dfs(to, visited, recStack)) {
                return true;
            } else if (recStack[to]) {
                return true;
            }
        }
    }
    recStack[id] = false;
    return false;
}


std::shared_ptr<Lock> LockManager::acquireLock(Transaction &transaction, DataItem dataItem, LockMode mode) {
  throw std::logic_error{"not implemented"};
}

LockMode LockManager::getLockMode(DataItem dataItem) const {
  throw std::logic_error{"not implemented"};
}

void LockManager::deleteLock(Lock *lock) {
  throw std::logic_error{"not implemented"};
}

} // namespace moderndbs
