//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    LOG_ERROR("Transaction is in shrinking phase, should not acquire shared locks.");
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING};
  }

  txn->SetState(TransactionState::GROWING);
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    LOG_ERROR("Transaction is in shrinking phase, should not acquire shared locks.");
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING};
    return false;
  }

  if (shared_locks_hold_by_txn_.count(rid) != 0) {
    LOG_ERROR("this rid has already been used in shared mode");
    return false;
  }

  std::lock_guard<std::mutex> guard_exclusive(latch_);
  if (exclusive_locks_hold_by_txn_.count(rid) == 0){
    exclusive_locks_hold_by_txn_[rid] = txn;
    txn->SetState(TransactionState::GROWING);
    txn->GetExclusiveLockSet()->emplace(rid);
    return true;
  } else {
    // Checks if the younger transcation is holding it 
    Transaction* old_transcation = exclusive_locks_hold_by_txn_[rid];

    // rob the lock
    if (txn->GetTransactionId() < old_transcation->GetTransactionId()) {
      // deals with old transcation.
      old_transcation->SetState(TransactionState::ABORTED);
      old_transcation->GetExclusiveLockSet()->erase(rid);

      // deals with new transcation.
      txn->SetState(TransactionState::GROWING);
      txn->GetExclusiveLockSet()->emplace(rid);

      exclusive_locks_hold_by_txn_[rid] = txn;

      return true;
    } else { // wait until it finishs
      // TODO(to implement, since no test case catches in lock_manager_test)
      return false;
    }
  }
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::ABORTED){

  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    LOG_ERROR("Transaction is in shrinking phase, should not acquire lock upgrade.");
    throw TransactionAbortException{txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING};
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  if (txn->GetState() == TransactionState::COMMITTED){
    txn->GetSharedLockSet()->erase(rid);
    txn->GetExclusiveLockSet()->erase(rid);
    return true;
  }

  if (txn->GetState() != TransactionState::ABORTED){
    txn->SetState(TransactionState::SHRINKING);
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
