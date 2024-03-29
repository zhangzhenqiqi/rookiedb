package edu.berkeley.cs186.database.memory;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.PageException;
import edu.berkeley.cs186.database.recovery.LogManager;
import edu.berkeley.cs186.database.recovery.RecoveryManager;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

/**
 * 缓冲区管理器的实现，具有可调配的页面替换策略。
 * 数据保存在一个page大小的字节数组之中。
 * Implementation of a buffer manager, with configurable page replacement policies.
 * Data is stored in page-sized byte arrays, and returned in a Frame object specific
 * to the page loaded (evicting and loading a new page into the frame will result in
 * a new Frame object, with the same underlying byte array), with old Frame objects
 * backed by the same byte array marked as invalid.
 */
public class BufferManager implements AutoCloseable {
    // We reserve 36 bytes on each page for bookkeeping for recovery
    // (used to store the pageLSN, and to ensure that a redo-only/undo-only log record can
    // fit on one page).
    /**
     * 每页保存36字节数,保留字节用于记录恢复。用于存储pageLSN并确保 redo-only/undo-only 日志可以放在一页上。
     * 所以在每次读写page时，偏移量需要加上这个值才可以得到物理上的真正的偏移量。
     * 这是对于[D]页面来说的，保留的36Byte具有很多用处：
     * 1）对数据页面而言，需要保存着LSN。
     * 2）对日志记录而言，它不需要保存LSN，但需要保存着一些日志记录的元信息，不同的日志类型具有不同的元信息，
     * 36Byte足以应对所有情况。
     */
    public static final short RESERVED_SPACE = 36;

    // Effective page size available to users of buffer manager.
    /**
     * 可以使用的有效页面大小（Byte）
     */
    public static final short EFFECTIVE_PAGE_SIZE = (short) (DiskSpaceManager.PAGE_SIZE - RESERVED_SPACE);

    // Buffer frames
    private Frame[] frames;

    // Reference to the disk space manager underneath this buffer manager instance.
    private DiskSpaceManager diskSpaceManager;

    // Map of page number to frame index
    /**
     * 页号 到 帧下标的映射
     */
    private Map<Long, Integer> pageToFrame;

    // Lock on buffer manager
    private ReentrantLock managerLock;

    // Eviction policy
    /**淘汰策略*/
    private EvictionPolicy evictionPolicy;

    // Index of first free frame
    private int firstFreeIndex;

    // Recovery manager
    private RecoveryManager recoveryManager;

    // Count of number of I/Os
    private long numIOs = 0;

    /**
     * 缓冲区帧，包含已加载页面的信息，包裹在底层字节数组。空闲帧使用一个链表链接。
     * Buffer frame, containing information about the loaded page, wrapped around the
     * underlying byte array. Free frames use the index field to create a (singly) linked
     * list between free frames.
     */
    class Frame extends BufferFrame {
        private static final int INVALID_INDEX = Integer.MIN_VALUE;

        byte[] contents;//内容
        private int index;//帧号
        private long pageNum;//虚拟页号
        /**脏位*/
        private boolean dirty;
        private ReentrantLock frameLock;
        private boolean logPage; // log 类型的page？

        Frame(byte[] contents, int nextFree) {
            this(contents, ~nextFree, DiskSpaceManager.INVALID_PAGE_NUM);
        }

        Frame(Frame frame) {
            this(frame.contents, frame.index, frame.pageNum);
        }

        Frame(byte[] contents, int index, long pageNum) {
            this.contents = contents;
            this.index = index;
            this.pageNum = pageNum;
            this.dirty = false;
            this.frameLock = new ReentrantLock();
            int partNum = DiskSpaceManager.getPartNum(pageNum);
            this.logPage = partNum == LogManager.LOG_PARTITION;
        }

        /**
         * 固定此帧
         * Pin buffer frame; cannot be evicted while pinned. A "hit" happens when the
         * buffer frame gets pinned.
         */
        @Override
        public void pin() {
            this.frameLock.lock();

            if (!this.isValid()) {
                throw new IllegalStateException("pinning invalidated frame");
            }

            super.pin();
        }

        /**
         * 取消固定
         * Unpin buffer frame.
         */
        @Override
        public void unpin() {
            super.unpin();
            this.frameLock.unlock();
        }

        /**
         * 是否有效，index从0分界
         * @return whether this frame is valid
         */
        @Override
        public boolean isValid() {
            return this.index >= 0;
        }

        /**
         * 此帧的页面是否被释放
         * @return whether this frame's page has been freed
         */
        private boolean isFreed() {
            return this.index < 0 && this.index != INVALID_INDEX;
        }

        /**
         * 验证此帧，并在需要时刷盘
         * Invalidates the frame, flushing it if necessary.
         */
        private void invalidate() {
            if (this.isValid()) {
                this.flush();
            }
            this.index = INVALID_INDEX;
            this.contents = null;
        }

        /**
         * Marks the frame as free.
         */
        private void setFree() {
            if (isFreed()) {
                throw new IllegalStateException("cannot free free frame");
            }
            int nextFreeIndex = firstFreeIndex;
            firstFreeIndex = this.index;
            this.index = ~nextFreeIndex;
        }

        private void setUsed() {
            if (!isFreed()) {
                throw new IllegalStateException("cannot unfree used frame");
            }
            int index = firstFreeIndex;
            firstFreeIndex = ~this.index;
            this.index = index;
        }

        /**
         * @return page number of this frame
         */
        @Override
        public long getPageNum() {
            return this.pageNum;
        }

        /**
         * Flushes this buffer frame to disk, but does not unload it.
         */
        @Override
        void flush() {
            this.frameLock.lock();
            super.pin();
            try {
                if (!this.isValid()) {
                    return;
                }
                if (!this.dirty) {
                    return;
                }
                //在将脏页刷新到磁盘之前，需要将页内相关的日志先刷盘。《窃取》
                if (!this.logPage) {
                    recoveryManager.pageFlushHook(this.getPageLSN());
                }
                BufferManager.this.diskSpaceManager.writePage(pageNum, contents);
                BufferManager.this.incrementIOs();
                this.dirty = false;
            } finally {
                super.unpin();
                this.frameLock.unlock();
            }
        }

        /**
         * Read from the buffer frame.
         * @param position position in buffer frame to start reading
         * @param num number of bytes to read
         * @param buf output buffer
         */
        @Override
        void readBytes(short position, short num, byte[] buf) {
            this.pin();
            try {
                if (!this.isValid()) {
                    throw new IllegalStateException("reading from invalid buffer frame");
                }
                System.arraycopy(this.contents, position + dataOffset(), buf, 0, num);
                BufferManager.this.evictionPolicy.hit(this);
            } finally {
                this.unpin();
            }
        }

        /**
         * Write to the buffer frame, and mark frame as dirtied.
         * @param position position in buffer frame to start writing
         * @param num number of bytes to write
         * @param buf input buffer
         */
        @Override
        void writeBytes(short position, short num, byte[] buf) {
            this.pin();
            try {
                if (!this.isValid()) {
                    throw new IllegalStateException("writing to invalid buffer frame");
                }
                int offset = position + dataOffset();
                TransactionContext transaction = TransactionContext.getTransaction();
                if (transaction != null && !logPage) {
                    List<Pair<Integer, Integer>> changedRanges = getChangedBytes(offset, num, buf);
                    for (Pair<Integer, Integer> range : changedRanges) {
                        int start = range.getFirst();
                        int len = range.getSecond();
                        byte[] before = Arrays.copyOfRange(contents, start + offset, start + offset + len);
                        byte[] after = Arrays.copyOfRange(buf, start, start + len);
                        long pageLSN = recoveryManager.logPageWrite(transaction.getTransNum(), pageNum, (short) (start + position), before,
                                after);
                        this.setPageLSN(pageLSN);
                    }
                }
                System.arraycopy(buf, 0, this.contents, offset, num);
                this.dirty = true;
                BufferManager.this.evictionPolicy.hit(this);
            } finally {
                this.unpin();
            }
        }

        /**
         * Requests a valid Frame object for the page (if invalid, a new Frame object is returned).
         * Page is pinned on return.
         */
        @Override
        Frame requestValidFrame() {
            this.frameLock.lock();
            try {
                if (this.isFreed()) {
                    throw new PageException("page already freed");
                }
                if (this.isValid()) {
                    this.pin();
                    return this;
                }
                return BufferManager.this.fetchPageFrame(this.pageNum);
            } finally {
                this.frameLock.unlock();
            }
        }

        @Override
        short getEffectivePageSize() {
            if (logPage) {
                return DiskSpaceManager.PAGE_SIZE;
            } else {
                return BufferManager.EFFECTIVE_PAGE_SIZE;
            }
        }

        @Override
        long getPageLSN() {
            return ByteBuffer.wrap(this.contents).getLong(8);
        }

        @Override
        public String toString() {
            if (index >= 0) {
                return "Buffer Frame " + index + ", Page " + pageNum + (isPinned() ? " (pinned)" : "");
            } else if (index == INVALID_INDEX) {
                return "Buffer Frame (evicted), Page " + pageNum;
            } else {
                return "Buffer Frame (freed), next free = " + (~index);
            }
        }

        /**
         * 将contents发生改变的区域进行截取并封装为 (offset,len) 的格式返回。但每组最多也不能超过maxRange，
         * 这是为了保证一页可以存下一条log record，before+after相当于两倍长度，前面保留的36Byte足以容下日志记录的其他字段了，
         * Generates (offset, length) pairs for where buf differs from contents. Merges nearby
         * pairs (where nearby is defined as pairs that have fewer than BufferManager.RESERVED_SPACE
         * bytes of unmodified data between them).
         */
        private List<Pair<Integer, Integer>> getChangedBytes(int offset, int num, byte[] buf) {
            List<Pair<Integer, Integer>> ranges = new ArrayList<>();
            int maxRange = EFFECTIVE_PAGE_SIZE / 2;
            int startIndex = -1;
            int skip = -1;
            for (int i = 0; i < num; ++i) {
                if (startIndex >= 0 && maxRange == i - startIndex) {//[start,i)
                    ranges.add(new Pair<>(startIndex, maxRange));
                    startIndex = -1;
                    skip = -1;
                    i--;
                } else if (buf[i] == contents[offset + i] && startIndex >= 0) {
                    if (skip > BufferManager.RESERVED_SPACE) {//skip 即当前连续出现相同值(buf & contents)达到了阈值
                        ranges.add(new Pair<>(startIndex, i - startIndex - skip));
                        startIndex = -1;
                        skip = -1;
                    } else {
                        ++skip;
                    }
                } else if (buf[i] != contents[offset + i]) {
                    if (startIndex < 0) {
                        startIndex = i;
                    }
                    skip = 0;
                }
            }
            if (startIndex >= 0) {
                ranges.add(new Pair<>(startIndex, num - startIndex - skip));
            }
            return ranges;
        }

        void setPageLSN(long pageLSN) {
            ByteBuffer.wrap(this.contents).putLong(8, pageLSN);
        }

        private short dataOffset() {
            if (logPage) {
                return 0;
            } else {
                return BufferManager.RESERVED_SPACE;
            }
        }
    }

    /**
     * Creates a new buffer manager.
     *
     * @param diskSpaceManager the underlying disk space manager
     * @param bufferSize size of buffer (in pages)
     * @param evictionPolicy eviction policy to use
     */
    public BufferManager(DiskSpaceManager diskSpaceManager, RecoveryManager recoveryManager,
                         int bufferSize, EvictionPolicy evictionPolicy) {
        this.frames = new Frame[bufferSize];
        for (int i = 0; i < bufferSize; ++i) {
            this.frames[i] = new Frame(new byte[DiskSpaceManager.PAGE_SIZE], i + 1);
        }
        this.firstFreeIndex = 0;
        this.diskSpaceManager = diskSpaceManager;
        this.pageToFrame = new HashMap<>();
        this.managerLock = new ReentrantLock();
        this.evictionPolicy = evictionPolicy;
        this.recoveryManager = recoveryManager;
    }

    @Override
    public void close() {
        this.managerLock.lock();
        try {
            for (Frame frame : this.frames) {
                frame.frameLock.lock();
                try {
                    if (frame.isPinned()) {
                        throw new IllegalStateException("closing buffer manager but frame still pinned");
                    }
                    if (!frame.isValid()) {
                        continue;
                    }
                    evictionPolicy.cleanup(frame);
                    frame.invalidate();
                } finally {
                    frame.frameLock.unlock();
                }
            }
        } finally {
            this.managerLock.unlock();
        }
    }

    /**
     * Fetches a buffer frame with data for the specified page. Reuses existing
     * buffer frame if page already loaded in memory. Pins the buffer frame.
     * Cannot be used outside the package.
     *
     * @param pageNum page number
     * @return buffer frame with specified page loaded
     */
    Frame fetchPageFrame(long pageNum) {
        this.managerLock.lock();
        Frame newFrame;
        Frame evictedFrame;
        // figure out what frame to load data to, and update manager state
        try {
            if (!this.diskSpaceManager.pageAllocated(pageNum)) {
                throw new PageException("page " + pageNum + " not allocated");
            }
            if (this.pageToFrame.containsKey(pageNum)) {
                newFrame = this.frames[this.pageToFrame.get(pageNum)];
                newFrame.pin();
                return newFrame;
            }
            // prioritize free frames over eviction
            if (this.firstFreeIndex < this.frames.length) {
                evictedFrame = this.frames[this.firstFreeIndex];
                evictedFrame.setUsed();
            } else {
                evictedFrame = (Frame) evictionPolicy.evict(frames);
                this.pageToFrame.remove(evictedFrame.pageNum, evictedFrame.index);
                evictionPolicy.cleanup(evictedFrame);
            }
            int frameIndex = evictedFrame.index;
            newFrame = this.frames[frameIndex] = new Frame(evictedFrame.contents, frameIndex, pageNum);
            evictionPolicy.init(newFrame);

            evictedFrame.frameLock.lock();
            newFrame.frameLock.lock();

            this.pageToFrame.put(pageNum, frameIndex);
        } finally {
            this.managerLock.unlock();
        }
        // flush evicted frame
        try {
            evictedFrame.invalidate();
        } finally {
            evictedFrame.frameLock.unlock();
        }
        // read new page into frame
        try {
            newFrame.pageNum = pageNum;
            newFrame.pin();
            BufferManager.this.diskSpaceManager.readPage(pageNum, newFrame.contents);
            this.incrementIOs();
            return newFrame;
        } catch (PageException e) {
            newFrame.unpin();
            throw e;
        } finally {
            newFrame.frameLock.unlock();
        }
    }

    /**
     * Fetches the specified page, with a loaded and pinned buffer frame.
     *
     * @param parentContext lock context of the **parent** of the page being fetched
     * @param pageNum       page number]
     * @return specified page
     */
    public Page fetchPage(LockContext parentContext, long pageNum) {
        return this.frameToPage(parentContext, pageNum, this.fetchPageFrame(pageNum));
    }

    /**
     * Fetches a buffer frame for a new page. Pins the buffer frame. Cannot be used outside the package.
     *
     * @param partNum partition number for new page
     * @return buffer frame for the new page
     */
    Frame fetchNewPageFrame(int partNum) {
        long pageNum = this.diskSpaceManager.allocPage(partNum);
        this.managerLock.lock();
        try {
            return fetchPageFrame(pageNum);
        } finally {
            this.managerLock.unlock();
        }
    }

    /**
     * Fetches a new page, with a loaded and pinned buffer frame.
     *
     * @param parentContext parent lock context of the new page
     * @param partNum       partition number for new page
     * @return the new page
     */
    public Page fetchNewPage(LockContext parentContext, int partNum) {
        Frame newFrame = this.fetchNewPageFrame(partNum);
        return this.frameToPage(parentContext, newFrame.getPageNum(), newFrame);
    }

    /**
     * 释放页面-将页面从缓存中移出，并告诉DiskSpaceManager不再需要该页面。页面必须在此调用之前固定，
     * 并且在此调用之后不能使用（除取消固定外）。如果事务存在的话，会将页面写回disk中。
     * <p></p>
     * Frees a page - evicts the page from cache, and tells the disk space manager
     * that the page is no longer needed. Page must be pinned before this call,
     * and cannot be used after this call (aside from unpinning).
     *
     * @param page page to free
     */
    public void freePage(Page page) {
        this.managerLock.lock();
        try {
            TransactionContext transaction = TransactionContext.getTransaction();
            int frameIndex = this.pageToFrame.get(page.getPageNum());

            Frame frame = this.frames[frameIndex];
            //write to disk
            if (transaction != null) page.flush();
            this.pageToFrame.remove(page.getPageNum(), frameIndex);
            evictionPolicy.cleanup(frame);
            frame.setFree();

            this.frames[frameIndex] = new Frame(frame);
            diskSpaceManager.freePage(page.getPageNum());
        } finally {
            this.managerLock.unlock();
        }
    }

    /**
     * Frees a partition - evicts all relevant pages from cache, and tells the disk space manager
     * that the partition is no longer needed. No pages in the partition may be pinned before this call,
     * and cannot be used after this call.
     *
     * @param partNum partition number to free
     */
    public void freePart(int partNum) {
        this.managerLock.lock();
        try {
            for (int i = 0; i < frames.length; ++i) {
                Frame frame = frames[i];
                if (DiskSpaceManager.getPartNum(frame.pageNum) == partNum) {
                    this.pageToFrame.remove(frame.getPageNum(), i);
                    evictionPolicy.cleanup(frame);
                    frame.flush();
                    frame.setFree();
                    frames[i] = new Frame(frame);
                }
            }

            diskSpaceManager.freePart(partNum);
        } finally {
            this.managerLock.unlock();
        }
    }

    /**
     * Calls flush on the frame of a page and unloads the page from the frame. If the page
     * is not loaded, this does nothing.
     * @param pageNum page number of page to evict
     */
    public void evict(long pageNum) {
        managerLock.lock();
        try {
            if (!pageToFrame.containsKey(pageNum)) {
                return;
            }
            evict(pageToFrame.get(pageNum));
        } finally {
            managerLock.unlock();
        }
    }

    /**
     * 淘汰第i帧，当此帧有效并且未被pin时淘汰。
     * @param i
     */
    private void evict(int i) {
        Frame frame = frames[i];
        frame.frameLock.lock();
        try {
            if (frame.isValid() && !frame.isPinned()) {
                this.pageToFrame.remove(frame.pageNum, frame.index);
                evictionPolicy.cleanup(frame);

                frames[i] = new Frame(frame.contents, this.firstFreeIndex);
                this.firstFreeIndex = i;
                //evict 之前刷盘
                frame.invalidate();
            }
        } finally {
            frame.frameLock.unlock();
        }
    }

    /**
     * 淘汰所有帧
     * Calls evict on every frame in sequence.
     */
    public void evictAll() {
        for (int i = 0; i < frames.length; ++i) {
            evict(i);
        }
    }

    /**
     * Calls the passed in method with the page number of every loaded page.
     * @param process method to consume page numbers. The first parameter is the page number,
     *                and the second parameter is a boolean indicating whether the page is dirty
     *                (has an unflushed change).
     */
    public void iterPageNums(BiConsumer<Long, Boolean> process) {
        for (Frame frame : frames) {
            frame.frameLock.lock();
            try {
                if (frame.isValid()) {
                    process.accept(frame.pageNum, frame.dirty);
                }
            } finally {
                frame.frameLock.unlock();
            }
        }
    }

    /**
     * Get the number of I/Os since the buffer manager was started, excluding anything used in disk
     * space management, and not counting allocation/free. This is not really useful except as a
     * relative measure.
     * @return number of I/Os
     */
    public long getNumIOs() {
        return numIOs;
    }

    public static boolean logIOs;

    private void incrementIOs() {
        if (logIOs) {
            System.out.println("IO incurred");
            StackTraceElement[] trace = Thread.currentThread().getStackTrace();
            for (int i = 0; i < trace.length; i++) {
                String s = trace[i].toString();
                if (s.startsWith("edu")) {
                    System.out.println(s);
                }
            }
        }
        ++numIOs;
    }

    /**
     * Wraps a frame in a page object.
     * @param parentContext parent lock context of the page
     * @param pageNum page number
     * @param frame frame for the page
     * @return page object
     */
    private Page frameToPage(LockContext parentContext, long pageNum, Frame frame) {
        return new Page(parentContext.childContext(pageNum), frame);
    }
}
