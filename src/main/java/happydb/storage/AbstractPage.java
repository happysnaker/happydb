package happydb.storage;

import happydb.common.Database;
import happydb.transaction.TransactionId;
import lombok.Getter;
import lombok.Setter;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 抽象页，实现一些可复用的方法
 *
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
public abstract class AbstractPage implements Page {
    @Setter
    @Getter
    protected PageId pid;
    protected volatile boolean dirty;


    /**
     * 这是一个特殊的标识，它标识写锁目前正在被读者占有
     */
    private final TransactionId READER_HOLD_WRITER_LOCK = new TransactionId(-2048);
    /**
     * 页面上的读者集合，这是并发安全的容器<br>
     * <P><strong>同时，此 final 对象将作为同步锁和条件变量使用，在该对象上了，可能存在未获取锁而休眠的对象</strong></P>
     */
    @Getter // for debug
    private final Set<TransactionId> readers = ConcurrentHashMap.newKeySet();
    /**
     * 页面上的写者，只允许一个事务锁定
     */
    private volatile TransactionId writer;

    @Override
    public boolean hasReadLock(TransactionId tid) {
        return readers.contains(tid);
    }


    @Override
    public boolean hasWriteLock(TransactionId tid) {
        try {
            return this.writer != null && this.writer.equals(tid);
        } catch (NullPointerException e) {
            // 别的线程可能并发的释放了写锁，导致 writer == null
            return false;
        }
    }


    @Override
    public boolean tryReadLock(TransactionId tid, long timeoutMillis) {
        // 重入
        if (this.readers.contains(tid)) {
            return true;
        }

        long startTime = System.currentTimeMillis();

        synchronized (readers) {
            // 持有读锁，或者事务已经持有写锁了，都是可重入的情况
            if (hasReadLock(tid) || hasWriteLock(tid)) {
                readers.add(tid);
                // 注意。重入的情况不允许修改 writer
                return true;
            }

            // 有写者，必须要获取写锁
            if (writer != null && writer != READER_HOLD_WRITER_LOCK) {
                // 休眠等待写锁唤醒自身
                // 与 READER_HOLE_WRITER_LOCK 比较要用 == 比较对象是否是同一个
                while (writer != null && writer != READER_HOLD_WRITER_LOCK) {
                    if (timeoutMillis > 0 && System.currentTimeMillis() - startTime >= timeoutMillis - 10) {
                        return false;
                    }
                    try {
                        if (timeoutMillis > 0) {
                            readers.wait(startTime - System.currentTimeMillis() + timeoutMillis);
                        } else {
                            readers.wait();
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (IllegalArgumentException e) {
                        return false;
                    }
                }
            }
            this.writer = READER_HOLD_WRITER_LOCK;
            readers.add(tid);
        }
        return true;
    }

    @Override
    public boolean tryWriteLock(TransactionId tid, long timeoutMillis) {
        if (hasWriteLock(tid)) {
            return true;
        }
        long startTime = System.currentTimeMillis();

        synchronized (readers) {
            // 双重判断
            while (this.writer != null) {
                if (timeoutMillis > 0 && System.currentTimeMillis() - startTime >= timeoutMillis - 10) {
                    return false;
                }
                // 如果写锁占有，则休眠等待唤醒
                try {
                    if (timeoutMillis > 0) {
                        readers.wait(startTime - System.currentTimeMillis() + timeoutMillis);
                    } else {
                        readers.wait();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            this.writer = tid;
        }
        return true;
    }

    /**
     * 以读者模式释放页面
     *
     * @throws IllegalMonitorStateException 如果事务没有持有锁
     */
    public void readUnLock(TransactionId tid) {
        if (!hasReadLock(tid)) {
            throw new IllegalMonitorStateException();
        }

        synchronized (readers) {
            readers.remove(tid);
            if (readers.isEmpty()) {
                this.writer = null;
                readers.notifyAll();
            }
        }
    }

    /**
     * 以写者模式释放页面
     *
     * @throws IllegalMonitorStateException 如果事务没有持有锁
     */
    public void writeUnLock(TransactionId tid) {
        if (!hasWriteLock(tid)) {
            throw new IllegalMonitorStateException();
        }
        synchronized (readers) {
            this.writer = null;
            readers.notifyAll();
        }
    }

    @Override
    public void readLock(TransactionId tid) {
        tryReadLock(tid, 0);
    }

    @Override
    public void writeLock(TransactionId tid) {
        tryWriteLock(tid, 0);
    }



    @Override
    public PageId getPageId() {
        return pid;
    }

    @Override
    public void markDirty(boolean dirty) {
        synchronized (Database.getBufferPool()) {
            this.dirty = dirty;
        }
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    /**
     * 一个页面总是以 pid 唯一标识
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbstractPage that)) return false;

        return pid.equals(that.pid);
    }

    @Override
    public int hashCode() {
        return pid.hashCode();
    }

    @Override
    public String toString() {
        return "AbstractPage{" +
                "pid=" + pid +
                '}';
    }

    /**
     * 为子类提供的受保护的帮助方法，标识第 i 个页面是否被使用
     * @param i pageNo
     * @param header 插槽
     * @return 返回是否被使用
     */
    protected boolean isSlotUsed(int i, byte[] header) {
        return ((header[i / 8] >> (i % 8)) & 1) == 1;
    }

    /**
     * 为子类提供的受保护的帮助方法，标记插槽，由于一字节标识八个插槽，可能存在并发竞争关系，此方法是线程安全的
     * @param header 插槽
     * @param i 插槽位
     * @param val val
     */
    protected synchronized void markSlotUsed(int i, boolean val, byte[] header) {
        int mask = 1 << (i % 8);
        if (val) {
            header[i / 8] |= mask;
        } else {
            header[i / 8] &= (~mask);
        }
    }
}
