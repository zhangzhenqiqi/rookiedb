package edu.berkeley.cs186.database.io;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.recovery.RecoveryManager;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 磁盘空间管理器的实现，具有虚拟页转换功能用于将Page映射到os级的文件中，为每个Page分配一个虚拟页号，并将这些Page加载、写入磁盘。<br>
 *                                             [master page]    <br>
 *                    /                              |                               \  <br>
 *             [header page]                                                    [header page]   <br>
 *       /    |     |     |     \                   ...                   /    |     |     |     \  <br>
 *   [data] [data] ... [data] [data]                                   [data] [data] ... [data] [data]      <br>
 * 其中page size 为4KB。<br></br>
 * data表示一个数据页。<br></br>
 * header page中保存着一个bitmap，用于指示某个Page是否被分配了，一个header page管理着32K个数据页（4KB=32K bit）。
 * master page用于管理header page，每个header page索引占据16bit，所以一个master page管理着4K*8/16=2K个header page。
 * 一个master page表示一个分区，一个分区最大可以有64M的数据页，大小为64M*4KB=256GB<br></br>
 *
 * master page以及header page永久缓存在内存中，相当于是索引，对其造成的更改会立即刷新到磁盘。此缓存与BufferManager分开完成。<br></br>
 *
 * 虚拟页号用一个64位整数表示，格式为：<br></br>
 *      partition number * 10^10 + n<br></br>
 * 其中n为分区的第n个数据页（从0开始索引）。<br></br>
 *
 *  每个分区对应一个os级别的文件。在这个文件中，存储方式如下：<br></br>
 *  - master page 是os文件的第0页<br></br>
 *  - 第一个 header page 是os文件的第1页<br></br>
 *  - 接下来的32K页是第一个 header page 管理的页。<br></br>
 *  - 第二个 header page.. <br></br>
 *  - ...   <br></br>
 *
 * &nbsp;注意，这里的这颗树可不是B+树，这只是磁盘管理器所用的管理方式，千万别把二者混淆。
 *
 * An implementation of a disk space manager with virtual page translation, and
 * two levels of header pages, allowing for (with page size of 4K) 256G worth of data per partition:
 *
 *                                           [master page]
 *                  /                              |                               \
 *           [header page]                                                    [header page]
 *     /    |     |     |     \                   ...                   /    |     |     |     \
 * [data] [data] ... [data] [data]                                   [data] [data] ... [data] [data]
 *
 * Each header page stores a bitmap, indicating whether each of the data pages has been allocated,
 * and manages 32K pages. The master page stores 16-bit integers for each of the header pages indicating
 * the number of data pages that have been allocated under the header page (managing 2K header pages).
 * A single partition may therefore have a maximum of 64M data pages.
 *
 * Master and header pages are cached permanently in memory; changes to these are immediately flushed to
 * disk. This imposes a fairly small memory overhead (128M partitions have 2 pages cached). This caching
 * is done separately from the buffer manager's caching.
 *
 * Virtual page numbers are 64-bit integers (Java longs) assigned to data pages in the following format:
 *       partition number * 10^10 + n
 * for the n-th data page of the partition (indexed from 0). This particular format (instead of a simpler
 * scheme such as assigning the upper 32 bits to partition number and lower 32 to page number) was chosen
 * for ease of debugging (it's easier to read 10000000006 as part 1 page 6, than it is to decipher 4294967302).
 *
 * Partitions are backed by OS level files (one OS level file per partition), and are stored in the following
 * manner:
 * - the master page is the 0th page of the OS file
 * - the first header page is the 1st page of the OS file
 * - the next 32K pages are data pages managed by the first header page
 * - the second header page follows
 * - the next 32K pages are data pages managed by the second header page
 * - etc.
 */
public class DiskSpaceManagerImpl implements DiskSpaceManager {
    static final int MAX_HEADER_PAGES = PAGE_SIZE / 2; // 2 bytes per header page
    static final int DATA_PAGES_PER_HEADER = PAGE_SIZE * 8; // 1 bit per data page

    // Name of base directory.
    private String dbDir;

    // Info about each partition.
    private Map<Integer, PartitionHandle> partInfo;

    // Counter to generate new partition numbers.
    private AtomicInteger partNumCounter;

    // Lock on the entire manager.
    private ReentrantLock managerLock;

    // recovery manager
    private RecoveryManager recoveryManager;

    /**
     * Initialize the disk space manager using the given directory. Creates the directory
     * if not present.
     *
     * @param dbDir base directory of the database
     */
    public DiskSpaceManagerImpl(String dbDir, RecoveryManager recoveryManager) {
        this.dbDir = dbDir;
        this.recoveryManager = recoveryManager;
        this.partInfo = new HashMap<>();
        this.partNumCounter = new AtomicInteger(0);
        this.managerLock = new ReentrantLock();

        File dir = new File(dbDir);
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new PageException("could not initialize disk space manager - could not make directory");
            }
        } else {
            int maxFileNum = -1;
            File[] files = dir.listFiles();
            if (files == null) {
                throw new PageException("could not initialize disk space manager - directory is a file");
            }
            for (File f : files) {
                if (f.length() == 0) {
                    if (!f.delete()) {
                        throw new PageException("could not clean up unused file - " + f.getName());
                    }
                    continue;
                }
                int fileNum = Integer.parseInt(f.getName());
                maxFileNum = Math.max(maxFileNum, fileNum);

                PartitionHandle pi = new PartitionHandle(fileNum, recoveryManager);
                pi.open(dbDir + "/" + f.getName());
                this.partInfo.put(fileNum, pi);
            }
            this.partNumCounter.set(maxFileNum + 1);
        }
    }

    @Override
    public void close() {
        for (Map.Entry<Integer, PartitionHandle> part : this.partInfo.entrySet()) {
            try {
                part.getValue().close();
            } catch (IOException e) {
                throw new PageException("could not close partition " + part.getKey() + ": " + e.getMessage());
            }
        }
    }

    @Override
    public int allocPart() {
        return this.allocPartHelper(this.partNumCounter.getAndIncrement());
    }

    @Override
    public int allocPart(int partNum) {
        this.partNumCounter.updateAndGet((int x) -> Math.max(x, partNum) + 1);
        return this.allocPartHelper(partNum);
    }

    private int allocPartHelper(int partNum) {
        PartitionHandle pi;

        this.managerLock.lock();
        try {
            if (this.partInfo.containsKey(partNum)) {
                throw new IllegalStateException("partition number " + partNum + " already exists");
            }

            pi = new PartitionHandle(partNum, recoveryManager);
            this.partInfo.put(partNum, pi);

            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            // We must open partition only after logging, but we need to release the
            // manager lock first, in case the log manager is currently in the process
            // of allocating a new log page (for another txn's records).
            TransactionContext transaction = TransactionContext.getTransaction();
            if (transaction != null) {
                recoveryManager.logAllocPart(transaction.getTransNum(), partNum);
            }

            pi.open(dbDir + "/" + partNum);
            return partNum;
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public void freePart(int partNum) {
        PartitionHandle pi;

        this.managerLock.lock();
        try {
            pi = this.partInfo.remove(partNum);
            if (pi == null) {
                throw new NoSuchElementException("no partition " + partNum);
            }
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            try {
                pi.freeDataPages();
                pi.close();
            } catch (IOException e) {
                throw new PageException("could not close partition " + partNum + ": " + e.getMessage());
            }

            TransactionContext transaction = TransactionContext.getTransaction();
            if (transaction != null) {
                recoveryManager.logFreePart(transaction.getTransNum(), partNum);
            }

            File pf = new File(dbDir + "/" + partNum);
            if (!pf.delete()) {
                throw new PageException("could not delete files for partition " + partNum);
            }
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public long allocPage(int partNum) {
        this.managerLock.lock();
        PartitionHandle pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            int pageNum = pi.allocPage();
            pi.writePage(pageNum, new byte[PAGE_SIZE]);
            return DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        } catch (IOException e) {
            throw new PageException("could not modify partition " + partNum + ": " + e.getMessage());
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public long allocPage(long page) {
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        int headerIndex = pageNum / DATA_PAGES_PER_HEADER;
        int pageIndex = pageNum % DATA_PAGES_PER_HEADER;

        this.managerLock.lock();
        PartitionHandle pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            pi.allocPage(headerIndex, pageIndex);
            pi.writePage(pageNum, new byte[PAGE_SIZE]);
            return DiskSpaceManager.getVirtualPageNum(partNum, pageNum);
        } catch (IOException e) {
            throw new PageException("could not modify partition " + partNum + ": " + e.getMessage());
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public void freePage(long page) {
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        this.managerLock.lock();
        PartitionHandle pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            pi.freePage(pageNum);
        } catch (IOException e) {
            throw new PageException("could not modify partition " + partNum + ": " + e.getMessage());
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public void readPage(long page, byte[] buf) {
        if (buf.length != PAGE_SIZE) {
            throw new IllegalArgumentException("readPage expects a page-sized buffer");
        }
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        this.managerLock.lock();
        PartitionHandle pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            pi.readPage(pageNum, buf);
        } catch (IOException e) {
            throw new PageException("could not read partition " + partNum + ": " + e.getMessage());
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public void writePage(long page, byte[] buf) {
        if (buf.length != PAGE_SIZE) {
            throw new IllegalArgumentException("writePage expects a page-sized buffer");
        }
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        this.managerLock.lock();
        PartitionHandle pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            pi.writePage(pageNum, buf);
        } catch (IOException e) {
            throw new PageException("could not write partition " + partNum + ": " + e.getMessage());
        } finally {
            pi.partitionLock.unlock();
        }
    }

    @Override
    public boolean pageAllocated(long page) {
        int partNum = DiskSpaceManager.getPartNum(page);
        int pageNum = DiskSpaceManager.getPageNum(page);
        this.managerLock.lock();
        PartitionHandle pi;
        try {
            pi = getPartInfo(partNum);
            pi.partitionLock.lock();
        } finally {
            this.managerLock.unlock();
        }
        try {
            return !pi.isNotAllocatedPage(pageNum);
        } finally {
            pi.partitionLock.unlock();
        }
    }

    // Gets PartInfo, throws exception if not found.
    private PartitionHandle getPartInfo(int partNum) {
        PartitionHandle pi = this.partInfo.get(partNum);
        if (pi == null) {
            throw new NoSuchElementException("no partition " + partNum);
        }
        return pi;
    }
}
