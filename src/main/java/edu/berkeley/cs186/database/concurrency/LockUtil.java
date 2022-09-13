package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil是一个声明层，它为用户提供了多粒度锁获取。一般来说您应该调用LockUtil来获取锁而不是直接
 * 调用LockContext中的方法。
 * 三层次结构：
 *                  LockUtil
 *                      ⬆
 *                 LockContext
 *                      ⬆
 *                 LockManager
 * <p>
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        //隐式锁
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        //显式锁
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement

        if (LockType.substitutable(effectiveLockType, requestType)) {//至少具有隐式锁，直接返回即可
            return;
        } else if (explicitLockType == LockType.IX && requestType == LockType.S) {//升级为SIX
            lockContext.promote(transaction, LockType.SIX);
            return;
        } else if (explicitLockType.isIntent()) {
            lockContext.escalate(transaction);//锁提升
            //提升之后显式锁为X/S
            explicitLockType = lockContext.getExplicitLockType(transaction);
            if (explicitLockType == requestType || explicitLockType == LockType.X) {//锁提升有效
                return;
            }
        }

        //explicitType  = S or NL
        //(explicit,request): (NL,S) (NL,X) (S,X)
        //请求链条上所有父代的意向锁
        if (requestType == LockType.S) {//(NL,S)
            ensureAncestorsIntention(transaction, parentContext, LockType.IS);
        } else {//(*,X)
            ensureAncestorsIntention(transaction, parentContext, LockType.IX);
        }


        if (explicitLockType == LockType.NL) {//显式锁为NL，这是一个请求锁的过程
            lockContext.acquire(transaction, requestType);
        } else {//(S,X) 说明这是一个锁升级（S -> X）的过程
            lockContext.promote(transaction, requestType);
        }


        return;
    }

    // TODO(proj4_part2) add any helper methods you want
    //lockType-only IS/IX
    private static void ensureAncestorsIntention(TransactionContext transaction, LockContext lockContext, LockType lockType) {
        assert (lockType == LockType.IS || lockType == LockType.IX);
        if (lockContext == null) {
            return;
        }
        //先处理当前节点的祖先
        ensureAncestorsIntention(transaction, lockContext.parentContext(), lockType);
        LockType curLockType = lockContext.getExplicitLockType(transaction);
        if (!LockType.substitutable(curLockType, lockType)) {
            if (curLockType == LockType.NL) {//这是一个锁申请的过程
                lockContext.acquire(transaction, lockType);
            } else if (curLockType == LockType.S && lockType == LockType.IX) {//升级为SIX
                lockContext.promote(transaction, LockType.SIX);
            } else {
                lockContext.promote(transaction, lockType);
            }
        }
    }
}
