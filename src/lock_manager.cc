#include "moderndbs/lock_manager.h"

namespace moderndbs {

void Transaction::addLock(DataItem item, LockMode mode) {
  throw std::logic_error{"not implemented"};
}

Transaction::~Transaction() {
  throw std::logic_error{"not implemented"};
}

void WaitsForGraph::addWaitsFor(const Transaction &transaction, const Lock &lock) {
  throw std::logic_error{"not implemented"};
}

void WaitsForGraph::removeTransaction(const Transaction &transaction) {
  throw std::logic_error{"not implemented"};
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
