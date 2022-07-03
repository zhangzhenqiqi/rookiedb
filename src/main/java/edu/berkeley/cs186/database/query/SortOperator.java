package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;
import java.util.stream.Stream;

/**
 * 外部排序：
 *  pass 0. 设numBuffers = B，将大文件分为 ceil(N/B)=N0 个part，对于每个part执行内部排序并暂存到磁盘。
 *  pass 1. 此时有N0个已排序好的part，将buffers分为两部分，一部分大小为B-1用作输入，一个用作输出；每次输入B-1个 part，
 *          然后对这B-1个part进行合并，合并为一个排序好的新part；重复此操作直至产生 ceil(N0/(B-1)) 个part。
 *  pass 2~. 如果part个数不为1，那就重复pass1中的操作，直至成为一个part，表示归并结束。
 */
public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / (double) numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses) + getSource().estimateIOCost();
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() {
        return true;
    }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * 使用内部排序，对一个runs排序。
     * <br>
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * @return a single sorted run containing all the records from the input
     * iterator
     */
    public Run sortRun(Iterator<Record> records) {
        // TODO(proj3_part1): implement
        List<Record> recordList = new ArrayList<>();
        while (records.hasNext()) {
            recordList.add(records.next());
        }
        recordList.sort(this.comparator);
        return makeRun(recordList);
    }

    /**
     * 将多个已排序的runs合并为一个已排序的runs。
     * <br>
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run
     * next.
     *
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     *
     * @return a single sorted run obtained by merging the input runs
     */
    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);
        // TODO(proj3_part1): implement
        int rs = runs.size();
        Iterator<Record>[] its = new Iterator[rs];
        for (int i = 0; i < rs; i++) {
            its[i] = runs.get(i).iterator();
        }
        Run ret = makeRun();
        PriorityQueue<Pair<Record, Integer>> q = new PriorityQueue<>((pair1, pair2) -> {
            return pair1.getFirst().getValue(sortColumnIndex)
                    .compareTo(pair2.getFirst().getValue(sortColumnIndex));
        });
        for (int i = 0; i < rs; i++) {
            if (its[i].hasNext()) {
                q.add(new Pair<>(its[i].next(), i));
            }
        }
        while (!q.isEmpty()) {
            ret.add(q.peek().getFirst());
            Integer index = q.poll().getSecond();
            if (its[index].hasNext()) {
                q.add(new Pair<>(its[index].next(), index));
            }
        }
        return ret;
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * pass1,2,...
     * <br>
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     *
     * @return a list of sorted runs obtained by merging the input runs
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(proj3_part1): implement
        List<Run> ret = new ArrayList<>();
        int availBuffers = numBuffers - 1;
        for (int i = 0, j = 0; j < runs.size(); i = j) {//每次选出B-1个sorted runs进行归并
            j = i + availBuffers;//[i,j)
            if (j > runs.size()) j = runs.size();
            ret.add(mergeSortedRuns(runs.subList(i, j)));
        }
        return ret;
    }

    /**
     * Does an external merge sort over the records of the source operator.
     * You may find the getBlockIterator method of the QueryOperator class useful
     * here to create your initial set of sorted runs.
     *
     * @return a single run containing all of the source operator's records in
     * sorted order.
     */
    public Run sort() {
        // Iterator over the records of the relation we want to sort
        Iterator<Record> sourceIterator = getSource().iterator();
        if (!sourceIterator.hasNext()) return makeRun();
        // TODO(proj3_part1): implement
        //pass 0
        int availBuffers = numBuffers;
        List<Run> runs = new ArrayList<>();
        while (sourceIterator.hasNext()) {
            Iterator<Record> partIterator = getBlockIterator(sourceIterator, getSchema(), availBuffers);
            runs.add(sortRun(partIterator));
        }
        // pass 1,2 ...
        while (runs.size() > 1) {
            runs = mergePass(runs);
        }
        return runs.get(0);
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}

