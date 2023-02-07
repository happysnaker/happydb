package happydb.index.hash;

import happydb.common.Database;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.index.btree.BTreePage;
import happydb.index.btree.BTreeSuperPage;
import happydb.storage.BufferPool;
import happydb.storage.Page;
import happydb.storage.PageId;
import happydb.storage.PageManager;
import happydb.transaction.TransactionId;
import lombok.Getter;

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 此类在 Hash 索引一次操作中串行传递，操作结束后会自动释放所有的锁。
 * <P><strong>当一个线程开始扩容时，此类会保证没有其他线程并行执行</strong></P>
 *
 * @Author happysnaker
 * @Date 2023/1/4
 * @Email happysnaker@foxmail.com
 */
public class HashPageHolder {

    public final static ReentrantReadWriteLock EXPANSION_LOCK = new ReentrantReadWriteLock();

    /**
     * 操作开始时必须调用的方法，当任何一个线程开始扩容时，此方法将会被堵塞
     */
    public void init() {
        EXPANSION_LOCK.readLock().lock();
    }

    /**
     * 扩容锁，当获取到扩容锁时，只有扩容线程在工作
     */
    public void resizeLock() {
        EXPANSION_LOCK.readLock().unlock();
        EXPANSION_LOCK.writeLock().lock();
    }

    /**
     * 扩容锁，释放扩容锁，其他线程可以继续工作
     */
    public void resizeUnLock() {
        // 锁降级
        EXPANSION_LOCK.readLock().lock();
        EXPANSION_LOCK.writeLock().unlock();
    }


    @Getter
    private final TransactionId tid;
    private final BufferPool pool;
    private final PageManager pm;
    private final String tableName;

    @Getter
    private final HashSet<Page> pages = new HashSet<>();

    /**
     * @param tid 一次操作的事务 ID
     */
    public HashPageHolder(TransactionId tid, String tableName) {
        this.tid = tid;
        this.pool = Database.getBufferPool();
        this.tableName = tableName;
        this.pm = Database.getCatalog().getPageManager(tableName);
    }

    /**
     * 判断线程是否持有某种锁
     *
     * @param pid  页面ID
     * @param perm 锁类型
     * @return 返回真如果它持有锁
     */
    public boolean isHoldLock(PageId pid, Permissions perm) {
        for (Page page : pages) {
            if (page.getPageId().equals(pid)) {
                if (perm == Permissions.READ_ONLY)
                    return page.hasReadLock(tid);
                if (perm == Permissions.READ_WRITE)
                    return page.hasWriteLock(tid);
            }
        }
        return false;
    }


    /**
     * 以某种权限获取 hash 页
     *
     * @param pid
     * @param perm
     * @return
     * @throws DbException
     */
    public HashPage getHashPage(PageId pid, Permissions perm) throws DbException {
        if (!pid.getTableName().equals(tableName)) {
            throw new DbException("模式不匹配");
        }
        if (perm == null) {
            throw new DbException("必须要指定权限");
        }
        Page page = pool.getPage(tid, pid, perm);
        pages.add(page);
        return (HashPage) page;
    }

    /**
     * 释放页面上特定的锁，锁可能会被多次释放，因此如果<strong>事务未持有锁，则会静默返回而不是抛出异常</strong>
     * <P>如果页面是脏的，并且 perm 是写模式，此方法会将页面落盘</P>
     *
     * @param page 页面
     * @param perm 权限
     */
    public void releasePageIfHolder(Page page, Permissions perm) {
        if (!pages.contains(page)) {
            return;
        }
        if (perm == Permissions.READ_WRITE && page.isDirty() && page.hasWriteLock(tid)) {
            try {
                pm.writePage(page);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        pool.unsafeReleasePage(tid, page, perm);
        if (!page.hasReadLock(tid) && !page.hasWriteLock(tid)) {
            pages.remove(page);
        }
    }

    /**
     * 操作结束，释放所有的锁，并将自己弄脏的脏页(持有写锁)刷盘
     */
    public void end() throws IOException {
        HashSet<Page> set = new HashSet<>(pages);
        for (Page page : set) {
            if (page.isDirty() && page.hasWriteLock(tid)) {
                synchronized (Database.getBufferPool()) {
                    pm.writePage(page);
                    page.markDirty(false);
                }
            }

            releasePageIfHolder(page, Permissions.READ_ONLY);
            releasePageIfHolder(page, Permissions.READ_WRITE);
        }
        EXPANSION_LOCK.readLock().unlock();
    }
}
