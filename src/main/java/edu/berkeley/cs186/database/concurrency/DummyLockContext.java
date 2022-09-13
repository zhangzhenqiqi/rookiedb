package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * 虚拟锁，不执行任何锁定。一个有用的例子：在临时表只能从创建他们的事务中访问，所以无需考虑并发问题，
 * 此时就可以传进去一个虚拟锁。
 * <p></p>
 * A lock context that doesn't do anything at all. Used where a lock context
 * is expected, but no locking should be done.
 *
 * An example of where this is useful: temporary tables (for example the runs
 * created in external sort) are only accessible from the transaction that
 * created them. Since there's no chance of multiple transactions attempting
 * to access these tables at the same time, we can safely use a dummy lock
 * context since no synchronization across transactions is needed.
 */
public class DummyLockContext extends LockContext {
    public DummyLockContext() {
        this((LockContext) null);
    }

    public DummyLockContext(LockContext parent) {
        super(new DummyLockManager(), parent, "Unnamed");
    }

    public DummyLockContext(String name) {
        this(null, name);
    }

    public DummyLockContext(LockContext parent, String name) {
        super(new DummyLockManager(), parent, name);
    }

    @Override
    public void acquire(TransactionContext transaction, LockType lockType) { }

    @Override
    public void release(TransactionContext transaction) { }

    @Override
    public void promote(TransactionContext transaction, LockType newLockType) { }

    @Override
    public void escalate(TransactionContext transaction) { }

    @Override
    public void disableChildLocks() { }

    @Override
    public LockContext childContext(String name) {
        return new DummyLockContext(this, name);
    }

    @Override
    public int getNumChildren(TransactionContext transaction) {
        return 0;
    }

    @Override
    public LockType getExplicitLockType(TransactionContext transaction) {
        return LockType.NL;
    }

    @Override
    public LockType getEffectiveLockType(TransactionContext transaction) {
        return LockType.NL;
    }

    @Override
    public String toString() {
        return "Dummy Lock Context(\"" + name.toString() + "\")";
    }
}

