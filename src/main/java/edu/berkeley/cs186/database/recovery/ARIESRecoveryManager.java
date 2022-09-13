package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;
import sun.rmi.runtime.Log;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    /** Dirty page table (page number -> recLSN).*/
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * 初始化日志，仅在第一次·设置·数据库时调用。将一条master记录添加到日志中，并采取checkpoint。
     * <p></p>
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * 当一个新事务开启时调用
     * <p></p>
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * 当事务试图进入COMMITTING状态时调用。
     * 在COMMITTING时，首先要做的就是刷新日志到disk，只要日志存在，就有复原的依靠。
     * <p></p>
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry trxEntry = transactionTable.get(transNum);
        Transaction trx = trxEntry.transaction;
        long lastLSN = trxEntry.lastLSN;
        lastLSN = logManager.appendToLog(new CommitTransactionLogRecord(transNum, lastLSN));
        trxEntry.lastLSN = lastLSN;
        logManager.flushToLSN(lastLSN);
        trx.setStatus(Transaction.Status.COMMITTING);
        return lastLSN;
    }

    /**
     * 当事务试图进入ABORTING时调用。
     * 应追加一条<Abort>记录，并更新事务表和事务状态。
     * 注：···调用该函数不应执行任何回滚···
     * <p></p>
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry trxEntry = transactionTable.get(transNum);
        Transaction trx = trxEntry.transaction;
        long abortLSN = logManager.appendToLog(new AbortTransactionLogRecord(transNum, trxEntry.lastLSN));
        trx.setStatus(Transaction.Status.ABORTING);
        trxEntry.lastLSN = abortLSN;
        return abortLSN;
    }

    /**
     * 在事务尝试进入COMPLETE状态时调用。对于ABORTING状态的事务，应该在此处回滚数据。
     * <p></p>
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(proj5): implement
        TransactionTableEntry trxEntry = transactionTable.get(transNum);
        Transaction trx = trxEntry.transaction;
        if (trx.getStatus() == Transaction.Status.ABORTING) {
            rollbackToLSN(transNum, 0l);
        }
        long lastLSN = trxEntry.lastLSN;
        //remove trx from trxTable
        transactionTable.remove(transNum);
        //add end record
        lastLSN = logManager.appendToLog(new EndTransactionLogRecord(transNum, lastLSN));
        trxEntry.lastLSN = lastLSN;

        trx.setStatus(Transaction.Status.COMPLETE);
        return lastLSN;
    }

    /**
     * 执行所有事务的操作的回滚，直到（但不包含）某个LSN为止。
     * -从最近未撤销的记录的LSN开始。
     *  -如果当前LSN的记录是可撤销的：
     *      -通过在记录上调用undo获取CLR
     *      -附加CLR
     *      -在CLR上调用redo以执行撤销
     *  -将当前LSN更新为下一条要撤销的记录。
     * <p></p>
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastLSN);
        // TODO(proj5) implement the rollback logic described above
        for (; currentLSN > LSN; ) {
            LogRecord logRecord = logManager.fetchLogRecord(currentLSN);
            if (logRecord.isUndoable()) {
                LogRecord CLR = logRecord.undo(lastLSN);
                lastLSN = logManager.appendToLog(CLR);
                transactionEntry.lastLSN = lastLSN;
                CLR.redo(this, diskSpaceManager, bufferManager);
            }
            currentLSN = logRecord.getPrevLSN().get();
        }
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * 当页面刷新到磁盘时(flush()->diskManager)后便会回调这个方法，作用是从脏页中移除刷新到磁盘的page，
     * 刷新之后就不是脏页了，内存与磁盘一致。
     * <p></p>
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * 当发生页面写入时调用。
     * 永远不会在日志page上调用此方法，保证before和after的长度相同。
     * 应该追加相应的日志记录，并相应更新事务表和脏页表。
     * <p></p>
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // TODO(proj5): implement
        TransactionTableEntry trxEntry = transactionTable.get(transNum);
        Transaction trx = trxEntry.transaction;
        long lastLSN = trxEntry.lastLSN;

        LogRecord logRecord = new UpdatePageLogRecord(transNum, pageNum, lastLSN, pageOffset, before, after);
        lastLSN = logManager.appendToLog(logRecord);
        trxEntry.lastLSN = lastLSN;

        dirtyPage(pageNum, lastLSN);
        return lastLSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * DiskSpaceManager释放页面时调用,在调用此方法时已经将page刷盘了。
     * 需要进行日志刷新，因为在返回后立即可以在磁盘上看到更改。
     *
     * <p></p>
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * 为事务创建一个保存点。创建一个与事务的现有保存点同名的保存点应该删除旧的保存点。
     * 应记录适当的LSN，以便以后可以进行部分回滚。
     * 对应SQL：SAVEPOINT pomelo
     * <p></p>
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * 释放一个保存点。对应SQL RELEASE SAVEPOINT pomelo
     * <p></p>
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * 将事务回滚到指定的savepoint，对应SQL：ROLLBACK TO SAVEPOINT pomelo。
     * 在保存点之后所做的所有更改都应该撤销，类似于终止事务，除了事务的状态没有改变。
     * <p></p>
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long savepointLSN = transactionEntry.getSavepoint(name);

        // TODO(proj5): implement
        rollbackToLSN(transNum, savepointLSN);
        return;
    }

    /**
     * 创建一个checkpoint。步骤如下：
     * 1）首先，应该写入一条<BEGIN CKPT> 记录。
     * 2）然后，应首先使用DPT中的recLSN检查结束检查点记录，然后使用trx表中的status/lastLSNs，并在
     * 写满时写入（或当没任何内容可写时）。
     * 3）写入<END CKPT>并刷新log。
     * 4）最后更新master log，设置新的beginLSN为1)中的那条<BEGIN CKPT>的LSN。
     *
     * 这是一个模糊的checkpoint，因为同时有其他活跃的事务可能会对辅助Table：trxTable以及DPT造成更改，但我们持久化的
     * 数据可能会发生在更改之前，从而无法体现在持久化后的两个辅助Table中。这部分更改是通过日志来感知的，最后写入EndCKPT之后
     * 会刷新日志到disk。
     * <p></p>
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        // TODO(proj5): generate end checkpoint record(s) for DPT and transaction table
        for (Map.Entry<Long, Long> dptEntry : dirtyPageTable.entrySet()) {
            boolean canFit = EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size() + 1, chkptTxnTable.size());
            if (!canFit) {
                logManager.appendToLog(
                        new EndCheckpointLogRecord(chkptDPT, chkptTxnTable)
                );
                chkptDPT = new HashMap<>();
            }
            chkptDPT.put(dptEntry.getKey(), dptEntry.getValue());
        }

        for (Map.Entry<Long, TransactionTableEntry> trxTableEntry : transactionTable.entrySet()) {
            boolean canFit = EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size(), chkptTxnTable.size() + 1);
            if (!canFit) {
                logManager.appendToLog(
                        new EndCheckpointLogRecord(chkptDPT, chkptTxnTable)
                );
                chkptDPT = Collections.emptyMap();
                chkptTxnTable = new HashMap<>();
            }
            TransactionTableEntry trxEntry = trxTableEntry.getValue();
            chkptTxnTable.put(trxTableEntry.getKey(), new Pair<>(trxEntry.transaction.getStatus(), trxEntry.lastLSN));
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);
        // Ensure checkpoint is fully flushed before updating the master record
        flushToLSN(endRecord.getLSN());

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN, v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * 每当数据库启动时调用，即在DataBase的构造方法内调用，并执行重新启动恢复。
     * 恢复期间不会出现新的事务，一旦此方法返回，新的事务可能会开始。
     * <p></p>
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * 即使我们在脏页表中已有记录，也应该始终使用checkpoint中页面的recLSN，checkpoint总是比我们仅从log中推断出的任何内容更为准确。
     * 是否存在这种情况：DPT中的某个recLSN是在 <BEGIN CHECKPOINT> 之前的，而我们恢复时要从startLSN开始，startLSN是DPT中的最小值，
     * 所以DPT的数据更为准确！那为什么不直接用END记录中的DPT呢？我认为可能是END之后可能还有新的日志记录，因为checkpoint的时候是可以
     * 有其他事务同时进行的(我们使用的是fuzzy checkpoint)。？？
     *
     *
     * 构建辅助table时：
     *
     * 对log的时间顺序划分：
     * 1）在<BEGIN CKPT>之前.这部分数据自然而然体现在了辅助table中。
     * 2）在<BEGIN CKPT> 和 <END CKPT> 之间；这部分的变化checkpoint并不一定能够完整的持久化，因为checkpoint同时允许其他事务活跃，
     *     这就意味着，持久化DPT和trxTable是一个模糊的状态（比如已经读取出了CHM中的一项并放入临时的HashMap中，但这一项有可能在之后又有其他事务对其造成了影响，比如事务的状态改变等，但我们持久化时用的显然是临时的HashMap中的数据）。
     *     这部分变化可以通过读取日志来恢复，因为checkpoint结束之前会把最新的日志刷新到disk中。
     * 3）在 <END CKPT> 之后的，checkpoint之后也可能会有新的日志刷到盘里（例如commit事务）。
     *
     * 对数据来源划分：
     * 1）checkpoint中的保存的辅助table。
     * 2）从startLSN一直读到最新的log。
     *
     *
     * <p></p>
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records in the log are processed, for each ttable entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // Set of transactions that have completed
        Set<Long> endedTransactions = new HashSet<>();
        // TODO(proj5): implement
        Iterator<LogRecord> logRecordItrs = logManager.scanFrom(LSN);
        while (logRecordItrs.hasNext()) {
            LogRecord logRecord = logRecordItrs.next();
            LogType logType = logRecord.getType();

            //1.涉及事务的记录
            if (logRecord.getTransNum().isPresent()) {
                long trxId = logRecord.getTransNum().get();
                if (!transactionTable.containsKey(trxId)) {
                    Transaction trx = newTransaction.apply(trxId);
                    transactionTable.put(trxId, new TransactionTableEntry(trx));
                }
                TransactionTableEntry trxEntry = transactionTable.get(trxId);
                trxEntry.lastLSN = logRecord.LSN;

                //2.改变事务状态
                if (logType == LogType.COMMIT_TRANSACTION) {
                    trxEntry.transaction.setStatus(Transaction.Status.COMMITTING);
                } else if (logType == LogType.ABORT_TRANSACTION) {
                    trxEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                } else if (logType == LogType.END_TRANSACTION) {
                    trxEntry.transaction.cleanup();
                    trxEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                    transactionTable.remove(trxId);
                    endedTransactions.add(trxId);
                }
            }

            //3.与page相关的记录
            if (logRecord.getPageNum().isPresent()) {
                long pageNum = logRecord.getPageNum().get();
                if (logType == LogType.UPDATE_PAGE || logType == LogType.UNDO_UPDATE_PAGE) {//脏页
                    dirtyPage(pageNum, logRecord.LSN);
                } else if (logType == LogType.FREE_PAGE || logType == LogType.UNDO_ALLOC_PAGE) {//能使更改立即在磁盘上可见?why?
                    if (dirtyPageTable.containsKey(pageNum)) dirtyPageTable.remove(pageNum);
                }
            }

            //4.END_CHECKPOINT
            if (logRecord.getType() == LogType.END_CHECKPOINT) {
                //处理脏页表
                Map<Long, Long> chkpDPT = logRecord.getDirtyPageTable();
                dirtyPageTable.putAll(chkpDPT);
                //处理事务表
                Map<Long, Pair<Transaction.Status, Long>> chkpTrxTable = logRecord.getTransactionTable();
                for (Map.Entry<Long, Pair<Transaction.Status, Long>> entry : chkpTrxTable.entrySet()) {
                    long trxId = entry.getKey();
                    long lastLSN = entry.getValue().getSecond();
                    Transaction.Status trxStatus = entry.getValue().getFirst();
                    if (endedTransactions.contains(trxId)) {
                        continue;
                    }
                    TransactionTableEntry trxEntry = null;
                    if (!transactionTable.containsKey(trxId)) {//trxTable中没有此事务，添加进去
                        Transaction trx = newTransaction.apply(trxId);
                        trx.setStatus(trxStatus);
                        if (trxStatus == Transaction.Status.ABORTING) {
                            trx.setStatus(Transaction.Status.RECOVERY_ABORTING);
                        }
                        trxEntry = new TransactionTableEntry(trx);
                        trxEntry.lastLSN = lastLSN;
                        transactionTable.put(trxId, trxEntry);
                    }

                  /*  if (trxEntry == null) {//内存中有此项
                        trxEntry = transactionTable.get(trxId);
                        if (lastLSN >= trxEntry.lastLSN) {
                            trxEntry.lastLSN = lastLSN;
                        }
                        Transaction trx = trxEntry.transaction;
                        if (trx.getStatus().ordinal() < trxStatus.ordinal()) {
                            if (trxStatus == Transaction.Status.ABORTING)
                                trx.setStatus(Transaction.Status.RECOVERY_ABORTING);
                            else trx.setStatus(trxStatus);
                        }
                    }*/
                }
            }
        }


        //此时事务只有三种状态：COMMITTING、RUNNING、RECOVERY_ABORTING
        transactionTable.forEach((trxId, trxEntry) -> {
            Transaction.Status trxStatus = trxEntry.transaction.getStatus();
            if (trxStatus == Transaction.Status.COMMITTING) {
                //recovery phase 不会调用end()
                trxEntry.transaction.cleanup();
                trxEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(trxId);
                trxEntry.lastLSN = logManager.appendToLog(new EndTransactionLogRecord(trxId, trxEntry.lastLSN));
            } else if (trxStatus == Transaction.Status.RUNNING) {
                trxEntry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                trxEntry.lastLSN = logManager.appendToLog(new AbortTransactionLogRecord(trxId, trxEntry.lastLSN));
            }
        });

        return;
    }

    /**
     * -分区相关的记录(AllocPart, UndoAllocPart, FreePart, UndoFreePart);
     * -分配页面的记录(AllocPage/UndoFreePage);
     * -更新页面的记录(UpdatePage, UndoUpdatePage, UndoAllocPage, FreePage)；对于此情况，有三个条件，如果满足至少一个就不必redo：
     *     1.记录对应的page不在DPT中；例子：页面刷新到了磁盘，并在checkpoint之前从DPT中移除了，然后DPT才刷新到checkpoint。
     *     2.记录的LSN小于该页面的DPT中的recLSN；例子：
     *     3.页面本身的pageLSN大于等于记录的LSN；例子：在该条记录之后，页面刷新到了磁盘。
     * <p></p>
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        // TODO(proj5): implement

        if (dirtyPageTable.isEmpty()) return;
        long startLSN = Collections.min(dirtyPageTable.values());

        Iterator<LogRecord> logRecordItr = logManager.scanFrom(startLSN);
        while (logRecordItr.hasNext()) {
            LogRecord logRecord = logRecordItr.next();
            LogType logType = logRecord.getType();
            long LSN = logRecord.getLSN();
            if (!logRecord.isRedoable()) continue;
            if (logRecord.getPartNum().isPresent() || logType == LogType.ALLOC_PAGE || logType == LogType.UNDO_FREE_PAGE) {
                logRecord.redo(this, diskSpaceManager, bufferManager);
            } else {
                long pageNum = logRecord.getPageNum().get();
                Page page = bufferManager.fetchPage(new DummyLockContext(), pageNum);
                if (!dirtyPageTable.containsKey(pageNum) ||
                        dirtyPageTable.get(pageNum) > LSN) continue;
                try {
                    long pageLSN = page.getPageLSN();
                    if (pageLSN >= LSN) continue;
                } finally {
                    page.unpin();
                }
                logRecord.redo(this, diskSpaceManager, bufferManager);
            }
        }
        return;
    }

    /**
     * 如果按照事务枚举，即一次处理一个事务的所有的undo操作，这会带来许多随机IO(日志读取)；优化做法为我们每次都读取
     * 具有最高值的LSN，这样是按照一种日志的记录顺序读取的，对IO友好。
     * <p></p>
     * This method performs the undo pass of restart recovery.
     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   if the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    void restartUndo() {
        // TODO(proj5): implement
        //(lsn,trxId)
        PriorityQueue<Pair<Long, Long>> toUndo = new PriorityQueue<>(Comparator.comparingLong(Pair<Long, Long>::getFirst).reversed());
        transactionTable.forEach((trxId, trxEntry) -> {
            toUndo.add(new Pair<>(trxEntry.lastLSN, trxId));
        });
        while (!toUndo.isEmpty()) {
            long LSN = toUndo.peek().getFirst();
            long trxId = toUndo.poll().getSecond();
            TransactionTableEntry trxEntry = transactionTable.get(trxId);
            LogRecord logRecord = logManager.fetchLogRecord(LSN);
            if (logRecord.isUndoable()) {
                LogRecord CLR = logRecord.undo(trxEntry.lastLSN);
                trxEntry.lastLSN = logManager.appendToLog(CLR);
                //这里真正执行undo动作
                CLR.redo(this, diskSpaceManager, bufferManager);
            }
            long nextLSN = 0;
            if (logRecord.getUndoNextLSN().isPresent()) {
                nextLSN = logRecord.getUndoNextLSN().get();
            } else {
                nextLSN = logRecord.getPrevLSN().get();
            }
            if (nextLSN == 0) {
                trxEntry.transaction.cleanup();
                trxEntry.transaction.setStatus(Transaction.Status.COMPLETE);
                trxEntry.lastLSN = logManager.appendToLog(new EndTransactionLogRecord(trxId, trxEntry.lastLSN));
                transactionTable.remove(trxId);
            } else {
                toUndo.add(new Pair<>(nextLSN, trxId));
            }
        }
        return;
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
            Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
