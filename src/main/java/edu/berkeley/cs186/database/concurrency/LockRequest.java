package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.Collections;
import java.util.List;

/**
 * 表示对事务队列的锁定请求，请求lock并释放releasedLocks中的所有内容。
 * releasedLocks中的所有东西都应该在事务被解锁之前被释放。
 * <p>
 * Represents a lock request on the queue for `transaction`, requesting `lock`
 * and releasing everything in `releasedLocks`. `lock` should be granted and
 * everything in `releasedLocks` should be released *before* the transaction is
 * unblocked.
 */
class LockRequest {
    TransactionContext transaction;
    Lock lock;
    List<Lock> releasedLocks;

    // Lock request for `lock`, that is not releasing anything.
    LockRequest(TransactionContext transaction, Lock lock) {
        this.transaction = transaction;
        this.lock = lock;
        this.releasedLocks = Collections.emptyList();
    }

    // Lock request for `lock`, in exchange for all the locks in `releasedLocks`.
    LockRequest(TransactionContext transaction, Lock lock, List<Lock> releasedLocks) {
        this.transaction = transaction;
        this.lock = lock;
        this.releasedLocks = releasedLocks;
    }

    @Override
    public String toString() {
        return "Request for " + lock.toString() + " (releasing " + releasedLocks.toString() + ")";
    }
}