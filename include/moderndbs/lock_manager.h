#ifndef INCLUDE_MODERNDBS_LOCK_MANAGER_H
#define INCLUDE_MODERNDBS_LOCK_MANAGER_H
#include <memory>
#include <shared_mutex>
#include <vector>
#include <list>
#include <unordered_map>
#include <unordered_set>

namespace moderndbs {

/// A DataItem, usually a TID
using DataItem = uint64_t;
/// The locking mode
enum class LockMode { Unlocked, Shared, Exclusive };

struct Lock;
class LockManager;

/// A transaction
class Transaction {
private:
  /// The lock manager
  LockManager *lockManager = nullptr;
  /// The acquired locks
  std::vector<std::shared_ptr<Lock>> locks;

public:
  /// Constructor
  Transaction() = default;
  /// Constructor
  explicit Transaction(LockManager &lockManager) : lockManager(&lockManager) {}
  /// Destructor. Releases the locks
  ~Transaction();
  /// Add a lock for the data item
  void addLock(DataItem item, LockMode mode);
  /// Get the locks
  const auto &getLocks() { return locks; }
};

/// A lock on a DataItem
struct Lock : std::enable_shared_from_this<Lock> {
  /// The next lock in the chain
  Lock *next = nullptr;
  /// The data item to check
  DataItem item;
  /// The actual lock
  std::shared_mutex lock;
  /// The owners of the lock: One in case of Exclusive, multiple when Shared
  std::vector<const Transaction *> owners;
  /// The current locked state
  LockMode ownership = LockMode::Unlocked;

  std::unordered_set<const Transaction*> waiting_queue;

  /// Constructor
  explicit Lock(DataItem item) : item(item) {}

  /// Take a shared ownership of this lock
  std::shared_ptr<Lock> getAsSharedPtr() { return weak_from_this().lock(); }

  static std::shared_ptr<Lock> construct() {
      return std::shared_ptr<Lock>(new Lock(0), [](Lock* lock){ /**/});
  }

  bool isExpired() const {
      return weak_from_this().expired();
  }
};

/// An exception that signals an avoided deadlock
class DeadLockError : std::exception {
  const char *what() const noexcept override { return "deadlock detected"; }
};

struct Node {
    uint16_t transaction_id;
    const Transaction& transaction;

    Node(uint16_t id, const Transaction& tran):transaction_id(id), transaction(tran)  {}
};
/// A wait-for graph structure to detect deadlocks when transactions need to
/// wait on another
class WaitsForGraph {
private:
  // TODO: add your implementation here
  uint16_t num_nodes = 0;
  std::vector<std::list<Node>> adj;
  std::unordered_map<const Transaction*, Node*> current_nodes;
  std::mutex latch;
public:
  /// Add a wait-for relationship of the specified transaction on the specified
  /// lock, respectively the owners of the lock.
  /// Throws a DeadLockError in case waiting for the lock would result in a
  /// deadlock
  void addWaitsFor(const Transaction &transaction, const Lock &lock);

  /// Remove the waits-for dependencies *to and from* this transaction from the
  /// waits-for graph
  void removeTransaction(const Transaction &transaction);

  bool checkForCycle();

  bool dfs(uint16_t id, std::shared_ptr<bool> visited, std::shared_ptr<bool> recStack);

  bool updateWaitsFor(const Transaction& waiting_t, Transaction &holding_t);

  void remove_save(const Transaction &transaction);

  void consitencyCheck();
};

/// A lock manager for concurrency-safe acquiring and releasing locks
class LockManager {
private:
  /// A hash function to map a DataItem to a position in the table
  using Hash = std::hash<DataItem>;

  /// A Chain of locks for each bucket of the hash table
  struct Chain {
    /// The latch to modify the chain
    std::mutex latch;
    /// The chain
    Lock *first = nullptr;
    /// Constructor
    Chain() = default;
    /// Move-Constructor to store in standard containers
    Chain(Chain &&other) noexcept : first(other.first) {}
    /// Move-Assignment to store in standard containers
    Chain &operator=(Chain &&other) noexcept {
      first = other.first;
      return *this;
    }
  };

  /// The hashtable
  std::vector<Chain> table;
  /// The wait-for graph to check for deadlocks
  WaitsForGraph wfg;

public:
  /// Constructor
  /// @param bucketCount: The size of the table. Needs to be fixed to avoid
  /// rehashing
  explicit LockManager(size_t bucketCount) { table.resize(bucketCount); }

  /// Acquire a lock for the transaction on the specified dataItem, in requested
  /// mode.
  /// In case of a deadlock, throws an DeadLockError.
  std::shared_ptr<Lock> acquireLock(Transaction &transaction, DataItem dataItem,
                                    LockMode mode);

  /// Get the lock mode of the dataItem
  LockMode getLockMode(DataItem dataItem) const;

  /// Delete a lock.
  void deleteLock(Lock *lock);

  void save_destruct(const Transaction& transaction, std::shared_ptr<Lock>&& lock);
};

} // namespace moderndbs

#endif
