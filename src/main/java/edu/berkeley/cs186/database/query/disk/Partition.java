package edu.berkeley.cs186.database.query.disk;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.SequentialScanOperator;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;

import java.util.List;

/**
 * 和Run类似，也表示一部分临时的磁盘空间，他对应的是分区操作而已。
 * 新建的Partition对应的临时表不会是同一个的，虽然传入的都是同一个schema，因为schema并不是用作命名的，
 * 只是为了告诉底层表的结构，每次新建的临时表名字与一个自增的整数有关，不会重复。
 * <br>
 * A partition represents a section of space on disk that we can append records
 * to or read from. This is useful for external hashing to store records we
 * aren't using and free up memory. Automatically buffers reads and writes to
 * minimize I/Os incurred.
 */
public class Partition implements Iterable<Record> {
    // The transaction this partition will be used within
    private TransactionContext transaction;
    // Under the hood we'll be storing all the records in a temporary table
    private String tempTableName;

    public Partition(TransactionContext transaction, Schema s) {
        this.transaction = transaction;
        this.tempTableName = transaction.createTempTable(s);
    }

    /**
     * Adds a record to this partition.
     *
     * @param record the record to add
     */
    public void add(Record record) {
        this.transaction.addRecord(this.tempTableName, record);
    }

    /**
     * Adds a list of records to this partition.
     *
     * @param records the records to add
     */
    public void addAll(List<Record> records) {
        for (Record record : records) this.add(record);
    }

    /**
     * @return an iterator over the records in this partition
     */
    public BacktrackingIterator<Record> iterator() {
        return this.transaction.getRecordIterator(this.tempTableName);
    }

    /**
     * @return returns a sequential scan operator over the temporary table
     * backing this partition.
     */
    public SequentialScanOperator getScanOperator() {
        return new SequentialScanOperator(this.transaction, this.tempTableName);
    }

    /**
     * Returns the number of pages used to store records in this partition.
     */
    public int getNumPages() {
        return this.transaction.getNumDataPages(this.tempTableName);
    }
}
