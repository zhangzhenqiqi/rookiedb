package edu.berkeley.cs186.database.concurrency;

/**
 * 从层次结构根开始，
 * 1.要获得节点上的S/IS锁，必须在父节点上持有IS/IX。
 * 2.要在节点上获取X/IX/SIX，必须在父节点上持有IX/SIX。
 * 3.必须自下而上的顺序释放锁。
 * <p></p>
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared，S(A)可以读取A和A的所有`后代`。
    X,   // exclusive，X(A)可以读写A和A的所有`后代`。
    IS,  // intention shared，IS(A)可以在A的所有`孩子`上请求S和IS。
    IX,  // intention exclusive，IX(A)可以在A的所有`孩子`上请求任意类型的锁。
    SIX, // shared intention exclusive ，可以做任何具有S(A)和IX(A)允许他做的事；除了请求A的孩子上的S、IS、SIX，这是多余的。
    NL;  // no lock held


    //lock compatibility matrix
    static boolean[][] lcm = {
            //      IS    IX    S     SIX    X     NL
            /*IS*/{true, true, true, true, false, true},
            /*IX*/{true, true, false, false, false, true},
            /* S*/{true, false, true, false, false, true},
            /*SIX*/{true, false, false, false, false, true},
            /* X*/{false, false, false, false, false, true},
            /*NL */{true, true, true, true, true, true}
    };

    //    father-son relationship
    //这里没有允许SIX-SIX，这是冗余的
    static boolean[][] fsr = {
            //      IS    IX    S     SIX    X     NL
            /*IS*/{true, false, true, false, false, true},
            /*IX*/{true, true, true, true, true, true},
            /* S*/{false, false, false, false, false, true},
            /*SIX*/{false, true, false, false, true, true},
            /* X*/{false, false, false, false, false, true},
            /*NL */{false, false, false, false, false, true}
    };

    /**
     * 为了方便调用lcm，需要将LockType映射到一个整数表示，和lcm保持一致。
     * @param a
     * @return
     */
    public static int getOrder(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
            case IS:
                return 0;
            case IX:
                return 1;
            case S:
                return 2;
            case SIX:
                return 3;
            case X:
                return 4;
            case NL:
                return 5;
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }


    /**
     * a与b兼容表示的含义为：两个不同的事务分别持有同一个资源的锁a和锁b是合法的、不冲突的。
     * <p>
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        return lcm[getOrder(a)][getOrder(b)];
    }

    /**
     * 返回 ：如果想在A上请求a lock，那么在A的父资源上要获取的锁。
     * <br>
     * SIX(A) can do anything that having S(A) or IX(A) lets it do, except requesting S, IS,
     * or SIX** locks on children of A, which would be redundant.
     * ** This differs from how its presented in lecture,
     * where SIX(A) allows a transaction to request SIX locks on children of A.
     * We disallow this in the project since the S aspect of the SIX child would be redundant.
     * <br>
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
            case S:
                return IS;
            case X:
                return IX;
            case IS:
                return IS;
            case IX:
                return IX;
            case SIX:
                return IX;
            case NL:
                return NL;
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * parentLock类型的锁是否有权限在孩子上授予childLock类型的锁
     * <p>
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        return fsr[getOrder(parentLockType)][getOrder(childLockType)];
    }


    //can sub
    static boolean[][] canReplace = {
            //      IS    IX    S     SIX    X     NL
            /*IS*/{true, true, true, true, true, false},
            /*IX*/{false, true, false, true, true, false},
            /* S*/{false, false, true, true, true, false},
            /*SIX*/{false, false, false, true, true, false},
            /* X*/{false, false, false, false, true, false},
            /*NL */{true, true, true, true, true, true}
    };

    /**
     * 可代替的含义并不同于兼容和父子，如果把锁A换成B，A能做的事B都可以做，那就可以替换，不必考虑兼容。
     * <p>
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement

        return canReplace[getOrder(required)][getOrder(substitute)];
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
            case S:
                return "S";
            case X:
                return "X";
            case IS:
                return "IS";
            case IX:
                return "IX";
            case SIX:
                return "SIX";
            case NL:
                return "NL";
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }
}

