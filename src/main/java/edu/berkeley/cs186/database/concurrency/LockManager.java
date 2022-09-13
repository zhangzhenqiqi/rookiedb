package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import javax.print.DocFlavor;
import java.util.*;

/**
 * LockManager 对象管理所有锁，将每个资源视为独立的,维护记录在哪些事务在哪些资源上具有哪些锁。
 * 并负责处理排队逻辑，在必要时阻塞/解除阻塞事务。LockManager通常不应该直接调用，代码中应该调用 {@link LockContext}
 * 的方法来获取/释放/提升/升级锁。
 * <p>
 * LockManager主要关心事务、资源和锁之间的映射，不关心多层次的粒度，多粒度由{@link LockContext} 处理。
 * <p>
 * LockManager管理的每个资源都有自己的{@link LockRequest}对象队列，表示获取(或提升、获取并释放)当时无法满足的锁的请求。
 * 每次对该资源的锁被释放时，都应该处理这个队列，从第一个请求开始，一直到无法满足请求为止。
 * <p>
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.

    /**
     * 包括资源上的锁列表以及资源上的请求队列。
     */
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * 检查·lockType·是否与此资源上已经存在的锁兼容。允许在trx_id为except的锁上产生冲突，这样做的好处是
         * 一个事务去替换他在资源上已经拥有的锁时很有用。
         * <p>
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement
            for (Lock lock : locks) {
                if (except != -1 && lock.transactionNum == except) continue;
                if (!LockType.compatible(lockType, lock.lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement
            for (int i = 0; i < locks.size(); i++) {
                //更新锁，一个事务在一个资源上只有一个锁
                if (locks.get(i).transactionNum == lock.transactionNum) {
                    locks.set(i, lock);
                    updateTransactionLocks(lock);
                    return;
                }
            }
            locks.add(lock);
            addTransactionLocks(lock);
            return;
        }

        /**
         * 移除此lock并处理等待队列，假设在之前已经得到了此lock。
         * <br>
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement
            locks.remove(lock);
            removeTransactionLocks(lock);
            processQueue();
            return;
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            // TODO(proj4_part1): implement
            while (!waitingQueue.isEmpty()) {
                LockRequest request = waitingQueue.peekFirst();
                if (checkCompatible(request.lock.lockType, request.transaction.getTransNum())) {
                    waitingQueue.pollFirst();
                    grantOrUpdateLock(request.lock);
                    for (Lock rLock : request.releasedLocks) {
                        release(request.transaction, rLock.name);
                    }
                    request.transaction.unblock();
                } else break;
            }
            return;
        }

        /**
         * 获取transaction在此资源上的锁类型，或者返回NL表示没有锁。
         * <br>
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement
            for (Lock lock : locks) {
                if (lock.transactionNum == transaction) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }
    //  some helper method to maintain transactionLocks////////////////////////////////////////////

    /**
     * helper method for maintain transactionLocks
     * @param lock
     */
    private void addTransactionLocks(Lock lock) {
        long trxId = lock.transactionNum;
        if (!transactionLocks.containsKey(trxId)) {
            transactionLocks.put(trxId, new ArrayList<>());
        }
        transactionLocks.get(trxId).add(lock);
    }

    private void updateTransactionLocks(Lock lock) {
        long trxId = lock.transactionNum;
        List<Lock> locks = transactionLocks.get(trxId);
        for (int i = 0; i < locks.size(); ++i) {
            Lock aLock = locks.get(i);
            if (aLock.name.equals(lock.name)) {
                locks.set(i, lock);
                return;
            }
        }
    }

    private void removeTransactionLocks(Lock lock) {
        long trxId = lock.transactionNum;
        List<Lock> locks = transactionLocks.get(trxId);
        locks.remove(lock);
    }

    // You should not modify or use this directly.
    /**只保存根节点？？*/
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * 获取与·name·对应的resourceEntry，如果尚不存在，则将新的（空）resourceEntry插入map。
     * <p></p>
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }


    /**
     * 该方法以原子方式（用户角度）获取一个锁并释放0个或多个锁。此方法优先于任何排队请求，如果未成功应该排队到队首。
     * 可能出现释放的锁中含有请求的锁，这种情况就是锁的更新。
     * <br>
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        long trxId = transaction.getTransNum();
        synchronized (this) {
            ResourceEntry re = getResourceEntry(name);

//            if (!re.getTransactionLockType(trxId).equals(LockType.NL) &&
//                    !releaseNames.contains(name)) {
//                throw new DuplicateLockRequestException("Can't have multiple locks on the same resource.");
//            }
            if (re.getTransactionLockType(trxId).equals(lockType)) {
                throw new DuplicateLockRequestException("Can't have multiple locks on the same resource.");
            }
            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            if (re.checkCompatible(lockType, trxId)) {
                re.grantOrUpdateLock(lock);

                for (ResourceName resourceName : releaseNames) {
                    if (name.equals(resourceName)) continue;
                    if (getResourceEntry(resourceName).getTransactionLockType(trxId).equals(LockType.NL)) {
                        throw new NoLockHeldException("Transaction :" + transaction.toString() +
                                " does not hold a lock on resource :[" + resourceName.toString() + "]");
                    }
                    release(transaction, resourceName);
                }

            } else {//加入队列头等待
                List<Lock> releaseLocks = new ArrayList<>(releaseNames.size());
                //根据releaseNames获取锁
                for (ResourceName resourceName : releaseNames) {
                    if (name.equals(resourceName)) continue;
                    Lock rLock = new Lock(resourceName, getResourceEntry(resourceName).getTransactionLockType(trxId), trxId);
                    releaseLocks.add(rLock);
                }
                LockRequest lockRequest = new LockRequest(transaction, lock, releaseLocks);
                re.addToQueue(lockRequest, true);
                shouldBlock = true;
                transaction.prepareBlock();
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * 在·name·所指示的资源上获取lockType类型的锁，用于事务transaction。
     * 你需要在获取锁之前进行错误检查。如果新的锁与另一个事务对资源的锁不兼容，或者如果该资源的队列中有其他事务，
     * 则该事务被阻塞，请求被放置在队列末尾。
     * <br>
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException 如果·name·上的锁已经被transaction持有，则抛出此异常。
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            ResourceEntry re = getResourceEntry(name);
            if (!re.getTransactionLockType(transaction.getTransNum()).equals(LockType.NL)) {
                throw new DuplicateLockRequestException("Can't have multiple locks on the same resource.");
            }
            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            if (re.waitingQueue.isEmpty() && re.checkCompatible(lockType, transaction.getTransNum())) {
                re.grantOrUpdateLock(lock);
            } else {
                LockRequest lockRequest = new LockRequest(transaction, lock);
                re.addToQueue(lockRequest, false);
                shouldBlock = true;
                transaction.prepareBlock();
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            ResourceEntry re = getResourceEntry(name);
            LockType lockType = re.getTransactionLockType(transaction.getTransNum());
            if (lockType.equals(LockType.NL)) {
                throw new NoLockHeldException("Transaction :" + transaction.toString() +
                        " does not hold a lock on resource :[" + name.toString() + "]");
            }
            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            re.releaseLock(lock);
        }
    }

    /**
     * 将事务对·name·的锁提升为·newLockType·
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        long trxId = transaction.getTransNum();

        synchronized (this) {
            ResourceEntry re = getResourceEntry(name);
            LockType oldLockType = re.getTransactionLockType(trxId);
            if (oldLockType.equals(newLockType)) {
                throw new DuplicateLockRequestException("`transaction` already has a `newLockType` lock on `name`");
            } else if (oldLockType == LockType.NL) {
                throw new NoLockHeldException("`transaction` has no lock on `name`");
            } else if (!LockType.substitutable(newLockType, oldLockType)) {
                throw new InvalidLockException(oldLockType + " cannot be upgraded to " + newLockType);
            }

            Lock newLock = new Lock(name, newLockType, trxId);
            if (!re.checkCompatible(newLockType, trxId)) {
                shouldBlock = true;
                re.addToQueue(new LockRequest(transaction, newLock), true);
                transaction.prepareBlock();
            } else {
                re.grantOrUpdateLock(newLock);
            }

        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * 返回事务在资源name上的锁对应的锁类型
     * <br>
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);
        return resourceEntry.getTransactionLockType(transaction.getTransNum());
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
