package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 使用Block(Chunk) Nested Loop Join (块嵌套循环连接)算法在 leftColumnName 和 rightColumnName 两个关系上连接。
 * 左表使用块枚举，右表使用页枚举
 *
 * <br>
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Block Nested Loop Join algorithm.
 */
public class BNLJOperator extends JoinOperator {
    /**buffer 数目，1 buffer用作 输出缓存，1 buffer 用作 right relation 的输入缓存，剩余下的 n-2 buffer用作 left table 的chunk缓存*/
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
                leftColumnName, rightColumnName, transaction, JoinType.BNLJ
        );
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
                getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Look over the implementation in SNLJOperator if you want to get a feel
     * for the fetchNextRecord() logic.
     */
    private class BNLJIterator implements Iterator<Record> {
        // Iterator over all the records of the left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();
            if (leftBlockIterator.hasNext()) {
                leftRecord = leftBlockIterator.next();
            }

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * Fetch the next block of records from the left source.
         * leftBlockIterator should be set to a backtracking iterator over up to
         * B-2 pages of records from the left source, and leftRecord should be
         * set to the first record in this block.
         *
         * If there are no more records in the left source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         * Make sure you pass in the correct schema to this method.
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement
            leftBlockIterator = getBlockIterator(leftSourceIterator, getLeftSource().getSchema(), numBuffers - 2);
            leftBlockIterator.markNext();
        }

        /**
         * fetch 右关系的一页记录
         * <br>
         * Fetch the next page of records from the right source.
         * rightPageIterator should be set to a backtracking iterator over up to
         * one page of records from the right source.
         *
         * If there are no more records in the right source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         * Make sure you pass in the correct schema to this method.
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement
            rightPageIterator = getBlockIterator(rightSourceIterator, getRightSource().getSchema(), 1);
            rightPageIterator.markNext();
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         *
         * You may find JoinOperator#compare useful here. (You can call compare
         * function directly from this file, since BNLJOperator is a subclass
         * of JoinOperator).
         */
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement
            //构造方法中已经执行了，fetch，之后leftRecord一直不会为null，直至遍历完所有的左表
            if (leftRecord == null) {
                return null;
            }
            while (true) {
                if (rightPageIterator.hasNext()) {//当前right page 还有记录
                    Record rightRecord = rightPageIterator.next();
                    if (compare(leftRecord, rightRecord) == 0) {
                        return leftRecord.concat(rightRecord);
                    } else {
                        continue;
                    }
                } else if (leftBlockIterator.hasNext()) {
                    //right page没记录了，但是left block还有，此时移动left指针，同时reset right page
                    leftRecord = leftBlockIterator.next();
                    rightPageIterator.reset();
                    continue;
                }


                //此时，当前left block 和当前right page 已双重循环完毕
                leftRecord = null;
                if (rightSourceIterator.hasNext()) {//right rel 还有page
                    fetchNextRightPage();
                    leftBlockIterator.reset();
                    leftRecord = leftBlockIterator.next();
                } else if (leftSourceIterator.hasNext()) {//right rel 没page了，但是left rel还有block
                    fetchNextLeftBlock();
                    leftRecord = leftBlockIterator.next();
                    rightSourceIterator.reset();
                    fetchNextRightPage();
                } else {
                    return null;
                }

            }
            /*while (leftRecord != null) {
                if (rightPageIterator.hasNext()) {//右表页仍有元素
                    Record rightRecord = rightPageIterator.next();
                    if (compare(leftRecord, rightRecord) == 0) {
                        return leftRecord.concat(rightRecord);
                    }
                } else {
                    fetchNextRightPage();  //尝试获取右表下一页
                    if (!rightPageIterator.hasNext()) {//说明右表已完成一轮遍历
                        rightSourceIterator.reset();
                        leftRecord = null;
                        fetchNextRightPage();
                        if (!rightPageIterator.hasNext()) {
                            return null;
                        }
                    }
                }
                //此时 rightPageIt 一定有元素
                if (leftRecord == null) {
                    if (!leftBlockIterator.hasNext()) {
                        fetchNextLeftBlock();
                    }
                    if (leftBlockIterator.hasNext()) {
                        leftRecord = leftBlockIterator.next();
                    }
                }
            }*/
        }

        /**
         * 这里用了一个小技巧，判断hashNext时，如果存在下一个元素则直接缓存到 nextRecord 中，供
         * next()消费。
         * <br>
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
//            System.out.println(nextRecord);
            return nextRecord;
        }
    }
}
