package happydb.storage;

import happydb.common.Database;
import happydb.common.Debug;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.exception.TimeoutException;
import happydb.transaction.TransactionId;
import lombok.Getter;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 页面池，事务获取页面的唯一入口，提供以特定权限获取页面的接口，以及遵循两相锁定统一释放事务获取的页
 * BufferPool 管理页面从磁盘到内存的读写。访问方法调用它来检索页面，然后它从适当的位置获取页面。
 * <p> BufferPool 还负责锁定；当事务获取页面时，BufferPool 会检查事务是否具有适当的锁来读写页面。
 *
 * @Threadsafe all fields are final
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
public class BufferPool {
    /**
     * 缓冲池用以驱逐页面的超级事务
     */
    public static final TransactionId BUFFER_POOL_EVICT_TRANSACTION = new TransactionId(-4096);

    /**
     * 每页字节数
     */
    public static final int DEFAULT_PAGE_SIZE = 16 * 1024;

    public static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * 传递给构造函数的默认页数。这被其他类使用。 BufferPool 应该改为使用构造函数的 numPages 参数。
     */
    public static int DEFAULT_PAGES = 250;
    /**
     * 最大缓冲数量
     */
    private final int numPages;
    /**
     * 缓冲池中的页面
     */
    @Getter
    public final ConcurrentHashMap<PageId, Page> pagePool;

    /**
     * 此 Map 跟踪事务在哪些页面集合上获取锁，以便释放他们
     */
    private final ConcurrentHashMap<TransactionId, Set<Page>> transactionLockMap;

    /**
     * 创建一个缓存最多 numPages 页的 BufferPool。
     *
     * @param numPages 此缓冲池中的最大页数。
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.transactionLockMap = new ConcurrentHashMap<>();
        this.pagePool = new ConcurrentHashMap<>(numPages);
    }

    public static int getPageSize() {
        return pageSize;
    }


    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }


    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }


    /**
     * 检索具有关联权限的指定页面。将获取一个锁，如果该锁被另一个事务持有，则可能会阻塞。
     * <p>
     * 应该在缓冲池中查找检索到的页面。如果存在，则应将其退回。如果不存在，则应将其添加到缓冲池并返回。
     * 如果缓冲池中没有足够的空间，则应逐出一个页面并在其位置添加新页面。
     *
     * @param tid  请求页面的交易ID
     * @param pid  请求页面的ID
     * @param perm 页面上请求的权限
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws DbException {
        assert tid != null;
        if (tid.getXid() >= 0 && !Database.open)
            throw new DbException("Database is closed");
        // 页面从缓存驱逐时，需要获取页面上的写锁
        // 试想，如果驱逐获取写锁与此方法获取锁并发发生，可能会导致此页面永远等待驱逐页面上的锁，因此驱逐后必须释放锁
        // 而页面获取锁后，需要双重判断自身是否被驱逐
        // 也有可能这里获取了页面但是还没上锁，缓存就驱逐了它，因此最需要双重验证
        Page page = pagePool.get(pid);
        if (page == null) {
            synchronized (pagePool) {
                // 双重验证
                if (pagePool.containsKey(pid)) {
                    page = pagePool.get(pid);
                } else {
                    PageManager pm = Database.getCatalog().getPageManager(pid.getTableName());
                    try {
                        page = pm.readPage(pid);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    while (this.pagePool.size() >= numPages) {
                        Debug.log("缓冲池满，尝试驱逐页面...");
                        try {
                            evictPage(1, false, false, true);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    // 在 pagePool 同步机制内，页面无法驱逐
                    // 并且没人跟他竞争，总能获取锁
                    try {
                        lockPage(tid, page, perm, 0);
                    } catch (TimeoutException e) {
                        throw new RuntimeException(e);
                    }
                    this.pagePool.put(pid, page);
                    return page;
                }
            }
        }

        // 这里的页面可能是已经被驱逐的页，因此要双重验证
        try {
            lockPage(tid, page, perm, 0);
            // 拿到锁之后，页面不可能被驱逐了，因此可以判断一下
            if (pagePool.get(pid) != page) {
                // 页面不相等，已经被驱逐了
                Debug.log("驱逐啦啦啦");
                this.transactionLockMap.get(tid).remove(page);
                return getPage(tid, pid, perm);
            }
            return page;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 事务以某种权限锁定页面，这可能会造成堵塞
     *
     * @param tid           事务，或者说是一个线程
     * @param page          待锁定的页面
     * @param perm          权限，如果为 null，则不会向页面中加任何锁
     * @param timeoutMillis 等待锁的超时时间，可以设置为 0 标识永不超时
     * @throws TimeoutException 如果超时未能获得锁
     */
    public void lockPage(TransactionId tid, Page page, Permissions perm, long timeoutMillis) throws TimeoutException {
        if (perm == null) {
            return;
        }

        if (perm == Permissions.READ_ONLY) {
            if (!page.tryReadLock(tid, timeoutMillis)) {
                throw new TimeoutException();
            }
        } else if (perm == Permissions.READ_WRITE) {
            // 如果线程持有读锁来申请写锁，那么首先应该释放读锁
            if (page.hasReadLock(tid)) {
                page.readUnLock(tid);
            }
            if (!page.tryWriteLock(tid, timeoutMillis)) {
                throw new TimeoutException();
            }
        }
        this.transactionLockMap.putIfAbsent(tid, ConcurrentHashMap.newKeySet());
        this.transactionLockMap.get(tid).add(page);
    }


    /**
     * 释放事务在页面上指定的锁，如果不存在锁将直接返回
     * <P><strong>请注意，如果页面未持有锁，此方法会静默返回而不是抛出一个异常</strong></P>
     *
     * @param tid  请求解锁的交易ID
     * @param page 页面
     * @param perm 需要释放的锁的模式
     */
    public void unsafeReleasePage(TransactionId tid, Page page, Permissions perm) {
        if (page.hasWriteLock(tid) && perm == Permissions.READ_WRITE) {
            page.writeUnLock(tid);
        }
        if (page.hasReadLock(tid) && perm == Permissions.READ_ONLY) {
            page.readUnLock(tid);
        }
        if (this.transactionLockMap.containsKey(tid)) {
            if (!page.hasWriteLock(tid) && !page.hasReadLock(tid)) {
                this.transactionLockMap.get(tid).remove(page);
            }
            if (this.transactionLockMap.get(tid).isEmpty()) {
                this.transactionLockMap.remove(tid);
            }
        }
    }


    /**
     * 释放与给定事务关联的所有锁。
     *
     * @param tid 请求解锁的交易ID
     */
    public void transactionReleaseLock(TransactionId tid) {
        for (Page page : new HashSet<>(this.transactionLockMap.getOrDefault(tid, new HashSet<>()))) {
            unsafeReleasePage(tid, page, Permissions.READ_ONLY);
            unsafeReleasePage(tid, page, Permissions.READ_WRITE);
        }
    }

    /**
     * 获取事务持有锁的所有页面
     *
     * @param tid 事务 ID
     * @return 事务持有锁的所有页面
     */
    public Set<Page> getTransactionPage(TransactionId tid) {
        return new HashSet<>(this.transactionLockMap.getOrDefault(tid, new HashSet<>()));
    }

    /**
     * 返回事务在给定页面上的锁
     * <p>如果事务没有持有锁，它应该返回 null</p>
     */
    public Page getPageLock(TransactionId tid, PageId pid) {
        if (!transactionLockMap.containsKey(tid)) {
            return null;
        }
        for (Page p : transactionLockMap.get(tid)) {
            if (p.getPageId().equals(pid)) {
                return p;
            }
        }
        return null;
    }

    /**
     * 如果指定的事务在指定的页面上有锁，则返回真
     */
    public boolean holdsLock(TransactionId tid, Page page, Permissions perm) {
        if (perm == null) {
            throw new NullPointerException();
        }
        return perm == Permissions.READ_ONLY ? page.hasReadLock(tid) : page.hasWriteLock(tid);
    }

    /**
     * 如果指定的事务在任何页面上上有锁，则返回真
     */
    public boolean holdsAnyLock(TransactionId tid) {
        for (Page page : transactionLockMap.getOrDefault(tid, new HashSet<>())) {
            if (holdsLock(tid, page, Permissions.READ_ONLY) || holdsLock(tid, page, Permissions.READ_WRITE)) {
                return true;
            }
        }
        return false;
    }


    /**
     * 将所有脏页刷新到磁盘。
     */
    public synchronized void flushAllDirtyPages() throws IOException {
        for (Page page : new HashSet<>(this.pagePool.values())) {
            if (page.isDirty()) {
                var pm = Database.getCatalog().getPageManager(page.getPageId().getTableName());
                pm.writePage(page);
                page.markDirty(false);
            }
        }
    }

    /**
     * 不安全的驱逐一个页面
     *
     * @param pid
     * @see #evictPage(long, boolean, boolean, boolean)
     */
    public void unsafeDiscardPage(PageId pid) {
        synchronized (pagePool) {
            this.pagePool.remove(pid);
        }
    }

    /**
     * 将某个页面刷新到磁盘
     *
     * @param pid 指示要刷新的页面的 ID
     */
    public synchronized void unsafeFlushPage(PageId pid) throws IOException {
        Page page = this.pagePool.get(pid);
        if (page == null)
            return;
        if (page.isDirty()) {
            var pm = Database.getCatalog().getPageManager(page.getPageId().getTableName());
            pm.writePage(page);
            page.markDirty(false);
        }
    }


    /**
     * 从缓冲池中丢弃一些页面。将页面刷新到磁盘以确保脏页在磁盘上得到更新。
     * <P><strong>驱逐将获取页面上的写锁以保证没有其他事务引用它</strong></P>
     *
     * @param timeoutMillis      获取锁时等待的时间，为 0 表示无限等待，个位数表示立即返回
     * @param many               指示是否是驱逐一个页面还是驱逐多个页面
     * @param random             当 many 为 true 时，此参数决定是随机驱逐多个页面还是尝试驱逐全部
     * @param evictDirtyAndFlush 指示是否能够驱逐脏页，如果能驱逐脏页，驱逐前需要落盘；否则，将绝不会驱逐脏页
     * @throws DbException 如果无法驱逐至少一个页面
     */
    public void evictPage(long timeoutMillis, boolean many, boolean random, boolean evictDirtyAndFlush) throws DbException, IOException {
        // 页面从缓存驱逐时，需要获取页面上的写锁
        // 试想，如果驱逐获取写锁与此方法获取锁并发发生，可能会导致此页面永远等待驱逐页面上的锁，因此驱逐后必须释放锁
        // 而页面获取锁后，需要双重判断自身是否被驱逐
        // 也有可能这里获取了页面但是还没上锁，缓存就驱逐了它，因此最需要双重验证
        boolean evict = false;
        synchronized (pagePool) {
            for (Map.Entry<PageId, Page> it : new HashSet<>(this.pagePool.entrySet())) {
                AbstractPage page = (AbstractPage) it.getValue();
                if (!page.getReaders().isEmpty() && timeoutMillis == 0) {
                    Debug.log("这个逼不对劲");
                }
                if ((evictDirtyAndFlush || !page.isDirty())
                        && page.tryWriteLock(BUFFER_POOL_EVICT_TRANSACTION, timeoutMillis)) {
                    try {
                        if (!evictDirtyAndFlush && page.isDirty())
                            continue;
                        if (page.isDirty())
                            unsafeFlushPage(page.getPageId());
                        this.pagePool.remove(page.getPageId());
                    } finally {
                        page.writeUnLock(BUFFER_POOL_EVICT_TRANSACTION);
                    }

                    evict = true;
                    if (!many || (random && Math.random() < 0.25))
                        return;
                }
            }
        }
        if (!evict)
            throw new DbException("内存已满，且无法驱逐页面");
    }

    // for test
    /**
     * 将所有页刷新到磁盘。
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page page : new HashSet<>(this.pagePool.values())) {
            var pm = Database.getCatalog().getPageManager(page.getPageId().getTableName());
            pm.writePage(page);
            page.markDirty(false);

        }
    }

}

