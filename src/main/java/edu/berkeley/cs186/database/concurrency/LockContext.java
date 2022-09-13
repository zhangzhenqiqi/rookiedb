package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext包裹LockManager以提供多粒度锁定的层次结构。调用请求/释放等主要通过LockContext完成，它提供了对层次结构
 * 中某个点（数据库、表等）的锁定方法的访问。
 * <p>
 * LockContext对象集合，每个对象代表一个可锁定的对象（例如页面和表），位于LockManager之上。LockContext对象根据层次结构
 * （例如，表的LockContext将数据库的上下文作为父级，将其页面的上下文作为其子级）。LockContext对象都共享一个LockManager，
 * 并且每个上下文都对其方法实施多粒度约束（例如，如果事务尝试在没有IX（db）的情况下请求X（table），则会引发异常）。
 * <p>
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    /**父亲LockContext，或者null表示当前已经是顶部节点。*/
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * 从LockManager中获取与·name·相关的LockContext。
     * <p></p>
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }


    /**
     * 对当前节点的后代中，关于trxId的资源集合：resourceNames 每个都变化了delta。
     * @param trxId
     * @param delta
     * @param resourceNames
     */
    private void updateNumChildLocks(long trxId, int delta, List<ResourceName> resourceNames) {
        for (ResourceName resourceName : resourceNames) {
            LockContext ctx = fromResourceName(lockman, resourceName).parentContext();
            if (ctx != null) updateNumChildLocks(trxId, delta);
        }
    }

    /**
     * 当前节点的后代中，关于trxId所代表的事务的锁数量更改了delta。
     * @param trxId
     * @param delta
     */
    private void updateNumChildLocks(long trxId, int delta) {
        numChildLocks.putIfAbsent(trxId, 0);
        numChildLocks.put(trxId, numChildLocks.get(trxId) + delta);
        LockContext parentContext = parentContext();
        if (parentContext != null) {
            parentContext.updateNumChildLocks(trxId, delta);
        }
    }

    /**
     * 此方法在确保满足所有多粒度约束后通过底层LockManager执行获取。
     * 如果事务具有SIX锁，则事务在任何后代资源上具有IS/S都是多余的。因此在我们的实现中，如果祖先有SIX，我们禁止后代获取IS/S锁，
     * 并认为这是一个无效请求。
     * <p></p>
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("This lockContext is read only.");
        }
        if (parent != null && !LockType.canBeParentLock(parent.getEffectiveLockType(transaction), lockType)) {
            throw new InvalidLockException("This request is invalid.");
        }
        lockman.acquire(transaction, name, lockType);
        LockContext pLockContext = parentContext();
        if (pLockContext != null) {
            pLockContext.updateNumChildLocks(transaction.getTransNum(), 1);
        }
        return;
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("This lockContext is read only.");
        }
        if (getNumChildren(transaction) > 0) {
            throw new InvalidLockException("The release request is invalid.");
        }
        lockman.release(transaction, name);
        LockContext pLockContext = parentContext();
        if (pLockContext != null) {
            pLockContext.updateNumChildLocks(transaction.getTransNum(), -1);
        }
        return;
    }

    /**
     * 锁提升为newLockType。
     * 注意，在将祖先提升为SIX而后代仍持有SIX的情况下，这仍然允许在SIX锁下持有SIX。这是多余的，但修复它
     * 即麻烦（必须用IX锁交换所有后代的SIX锁）而且毫无意义（无论如何你仍然持有后代的锁），所以我们让他保持原样。
     *
     * 将锁升级为SIX后，需要释放子代上所有的S和IS锁。
     * <p></p>
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        if (readonly) {
            throw new UnsupportedOperationException("This lockContext is read only.");
        }
        LockType oldLockType = lockman.getLockType(transaction, name);
        if (!LockType.substitutable(newLockType, oldLockType)
                || (parent != null && !LockType.canBeParentLock(parent.getEffectiveLockType(transaction), newLockType))) {
            throw new InvalidLockException("Invalid lock promote.");
        }

        if (newLockType == LockType.SIX) {
//            if (hasSIXAncestor(transaction)) {
//                throw new InvalidLockException("ancestor already has SIX lock, redundant lock request");
//            }
            List<ResourceName> sisDescendants = sisDescendants(transaction);
            updateNumChildLocks(transaction.getTransNum(), -1, sisDescendants);
            sisDescendants.add(name);
            //请求SIX并释放所有子代上的S/IS
            lockman.acquireAndRelease(transaction, name, newLockType, sisDescendants);
        } else {
            lockman.promote(transaction, name, newLockType);
        }
        return;
    }

    /**
     * 一种合并锁的操作，但会改变锁的粒度，例如以A为根的一些页有X锁，就可以取消掉这些页的X锁而直接把A升级为X锁。
     * 使用S或X锁将·transaction·的锁从这个上下文的后代升级到这个level。
     * <p></p>
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        LockType lockType = getExplicitLockType(transaction);
        if (readonly) {
            throw new UnsupportedOperationException("The context is read only.");
        } else if (lockType == LockType.NL) {
            throw new NoLockHeldException("`transaction` has no lock at this level.");
        }
        List<Lock> locks = lockman.getLocks(transaction);
        List<ResourceName> resourceNames = new ArrayList<>();
        boolean S = true;
        if (lockType == LockType.X || lockType == LockType.IX || lockType == LockType.SIX) {
            S = false;
        }
        for (Lock lock : locks) {
            if (lock.name.isDescendantOf(name)) {
                if (lock.lockType == LockType.X || lock.lockType == LockType.IX || lock.lockType == LockType.SIX) {
                    S = false;
                }
                resourceNames.add(lock.name);
            }
        }
        if (resourceNames.isEmpty()) {
            if (lockType == LockType.S || lockType == LockType.X) {
                return;
            }
        }
        updateNumChildLocks(transaction.getTransNum(), -1, resourceNames);
        resourceNames.add(name);
        lockman.acquireAndRelease(transaction, name, S ? LockType.S : LockType.X, resourceNames);
        return;
    }

    /**
     * 获取事务在当前节点上的锁，或者NL表示事务没有持有当前节点的锁。
     * <p></p>
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockType lockType = lockman.getLockType(transaction, getResourceName());
        return lockType;
    }

    /**
     * 获取事务在当前节点持有的锁，可以是隐式的（例如更高级别的显式S锁暗示着此级别的S锁）或者显式的。
     * 如果都没有则返回NL。
     * 意图锁不会授予较低等级别的锁获取权限，因为在获得S/X锁之前，必须在粒度层结构的所有祖先上具有适当的意图锁（这显然指的是显式的意图锁）。
     * 如果意图锁具有暗示机制，那么当顶部节点具有了IS，即使中间的节点没有IS，某个后代节点也可以获得S，这是不合法的。
     * 这个方法的必要性在于，对于：
     *                      table_1(S)
     *                       /
     *                    page_0(NL)
     * 虽然在page_0没锁，但实际上由于在table_1处的S锁的作用，事务是可以读page_0的。
     * <p></p>
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement
        LockType lockType = getExplicitLockType(transaction);
        if (lockType != LockType.NL) return lockType;
        LockContext parentContext = parentContext();
        if (parentContext != null) {
            LockType parentLockType = parentContext.getEffectiveLockType(transaction);
            if (parentLockType == LockType.SIX) {
                lockType = LockType.S;
            } else if (!parentLockType.isIntent()) {
                lockType = parentLockType;
            }
        }
        return lockType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        LockContext ctx = parent;
        while (ctx != null) {
            LockType lockType = lockman.getLockType(transaction, ctx.name);
            if (lockType == LockType.SIX) {
                return true;
            }
            ctx = ctx.parentContext();
        }
        return false;
    }

    /**
     * 获取所有事务transaction持有的锁的资源名称列表，这些锁是S/IS并且是当前LockContext上下文的后代。
     * <p></p>
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        List<ResourceName> resourceNames = new ArrayList<>();
        List<Lock> locks = lockman.getLocks(transaction);
        for (Lock lock : locks) {
            if (lock.name.isDescendantOf(name) && (lock.lockType == LockType.S || lock.lockType == LockType.IS)) {
                resourceNames.add(lock.name);
            }
        }
        return resourceNames;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

