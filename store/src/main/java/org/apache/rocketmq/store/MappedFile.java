/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.CommitLog.PutMessageContext;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

// mappedFile可以通过组来管理 mappedFileQueue
public class MappedFile extends ReferenceResource {

    // 内存页大小
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 当前进程下所有的mappedFile占用的总虚拟内存大小
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    // 当前进程下所有的mappedFile个数
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    // 当前mappedFile数据写入点
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    // 不使用MappedByteBuffer的时候才会使用当前字段
    protected final AtomicInteger committedPosition = new AtomicInteger(0);


    //当前mappedFile数据刷盘点 flushedPosition之前的数据都是安全数据 flushedPosition和wrotePosition是脏页等待刷盘
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    // 文件大小
    protected int fileSize;

    // 文件访问通道
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    // 异步刷盘先写入writeBuffer，由commitReadTime线程定时200ms提交到fileChannel中，再由flushRealTime线程每500ms将fileChannel中的内容落盘
    protected ByteBuffer writeBuffer = null;

    //堆外内存，用于异步刷盘，为了减少内存申请和销毁时间，提前向OS申请并锁定一块堆外内存，writeBuffer就从这里获取
    protected TransientStorePool transientStorePool = null;

    //类似于 00000102342  后面这个数字就是这个文件第一条消息偏移量
    //文件名 通常以此文件开始写入的字节数
    private String fileName;

    // 文件名对应的物理偏移量
    private long fileFromOffset;

    // 文件对象
    private File file;

    // 内存映射缓冲区，访问虚拟内存
    // 在当前进程虚拟内存开辟一块空空间，和数据文件大小一样。由mappedByteBuffer进行管理
    private MappedByteBuffer mappedByteBuffer;

    // 该文件下最近一条写入数据的时间戳
    private volatile long storeTimestamp = 0;

    //当前文件如果是目录内有效文件的首文件 这个值为true
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    // 参数一：文件名  绝对路径文件名
    // 参数二：文件大小
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            if (dirName.contains(MessageStoreConfig.MULTI_PATH_SPLITTER)) {
                String[] dirs = dirName.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
                for (String dir : dirs) {
                    createDirIfNotExist(dir);
                }
            } else {
                createDirIfNotExist(dirName);
            }
        }
    }

    private static void  createDirIfNotExist(String dirName) {
        File f = new File(dirName);
        if (!f.exists()) {
            boolean result = f.mkdirs();
            log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
        }
    }

    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            try {
                Method method = method(target, methodName, args);
                method.setAccessible(true);
                return method.invoke(target);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    // 参数一：文件名  绝对路径文件名
    // 参数二：文件大小
    private void init(final String fileName, final int fileSize) throws IOException {
        /**
         * 文件名 长度为20 左边补零，剩余为起始偏移量
         * 如果一个文件大小为1g，比如0000000000000000000000代表了第一个文件，起始偏移量尾0
         * 文件大小为1G=1073741824；当第一个文件写满了，第二个文件为00000000001073741824，起始偏移量为1073741824
         */
        this.fileName = fileName;
        // 文件大小 默认为1g
        this.fileSize = fileSize;

        // 创建文件对象
        this.file = new File(fileName);

        // 构建文件起始索引，就是创建文件名
        this.fileFromOffset = Long.parseLong(this.file.getName());

        // 标志位
        boolean ok = false;

        // 确定文件目录是否存在
        ensureDirOK(this.file.getParent());

        try {

            // 创建文件通道
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();

            // 获取对应虚拟内存映射缓冲区
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);

            // 记录文件大小
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);

            // mappedFile总数+1
            TOTAL_MAPPED_FILES.incrementAndGet();

            // 创建成功
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }


    // 提供上层向内存映射追加消息

    // 参数一：msg消息
    // 参数二：追加消息回调，每条消息具体哪些字段需要追加到文件中需要他进行控制
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb,
            PutMessageContext putMessageContext) {
        return appendMessagesInner(msg, cb, putMessageContext);
    }

    //
    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb,
            PutMessageContext putMessageContext) {
        return appendMessagesInner(messageExtBatch, cb, putMessageContext);
    }

    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb,
            PutMessageContext putMessageContext) {
        assert messageExt != null;
        assert cb != null;
        // 当前内存映射文件的写入位点
        int currentPos = this.wrotePosition.get();
        // 如果写入位点小于文件大小，说明当前文件可写
        if (currentPos < this.fileSize) {
            // 获取当前内存映射对象创建一个切片。mappedByteBuffer是一个固定的大小。通过slice可以获取一个Bytebuffer对象
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            // 设置byteBuffer的position为写入位点
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            // 单条消息
            if (messageExt instanceof MessageExtBrokerInner) {
                // 具体如何追加数据由callback进行控制
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBrokerInner) messageExt, putMessageContext);
                // 批量消息
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBatch) messageExt, putMessageContext);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            // 更新数据写入位点
            this.wrotePosition.addAndGet(result.getWroteBytes());
            // 记录最后一条消息存储时间
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be written to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     * data 需要写入文件的字节数组
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {

        // 获取当前文件写入位点
        int currentPos = this.wrotePosition.get();

        //  条件成立
        if ((currentPos + length) <= this.fileSize) {
            try {
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data, offset, length);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * @return The current flushed position
     * flushLeastPages 刷盘的最小页数，
     *  等于0时属于强制刷盘
     *  大于0时，需要脏页数据达到flushLeastPages才进行物理刷盘
     *
     */
    public int flush(final int flushLeastPages) {
        // 条件成立，说明需要进行刷盘
        if (this.isAbleToFlush(flushLeastPages)) {

            // 增加当前mappedFile的引用计数
            if (this.hold()) {
                //
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0();
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    protected void commit0() {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - lastCommittedPosition > 0) {
            try {
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    // 判断是否需要进行脏页刷盘
    private boolean isAbleToFlush(final int flushLeastPages) {
        // 获取了当前刷盘位点，
        int flush = this.flushedPosition.get();

        // 获取当前可读位点
        int write = getReadPosition();

        // 条件成立，说明文件写满了，必选进行刷盘
        if (this.isFull()) {
            return true;
        }

        // 说明要执行刷盘flushLeastPages个脏页
        if (flushLeastPages > 0) {
            //  计算脏页数量是否达到阈值
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        // 说明flushLeastPages等于0或者小于0。此时只要有未刷盘的脏页
        return write > flush;
    }

    protected boolean isAbleToCommit(final int commitLeastPages) {
        int commit = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (commit / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > commit;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    // 当前写入位点和当前文件大小相等，说明当前文件已经写满
    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    // 从position位点开始到有效数据为止，创建一个mappedByteBuffer的切片供业务访问数据
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {

        // 获取当前有效数据位点
        int readPosition = getReadPosition();

        // 说保证访问数据的位点是合法的
        if ((pos + size) <= readPosition) {
            // 持有资源
            if (this.hold()) {

                // 创建切片
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }
    
    // 创建指定大小的可读内存映射切片
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    // 释放堆外内存mappedByteBuffer
    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    // 删除文件
    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                // 关闭文件通道
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    // 内存映射文件，内存预热
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    // 锁住当前内存
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
