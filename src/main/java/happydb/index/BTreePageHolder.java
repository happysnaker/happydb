package happydb.index;

import happydb.common.Database;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.storage.BufferPool;
import happydb.storage.Page;
import happydb.storage.PageId;
import happydb.storage.PageManager;
import happydb.transaction.TransactionId;
import lombok.Getter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * <P>负责 B+ 树页面的分配与释放，生命周期为一次 B+ 树操作，一次操作中获取的锁在结束后必须要释放，<strong>脏页必须要刷回磁盘</strong></P>
 * <P>{@link happydb.storage.BufferPool} 记录了事务包含的所有页，它无法区分 B+ 树页面，而此类仅会跟踪 B+ 树页面</P>
 * <P>{@link BTreeIndex} 必须要将 {@link BTreeSuperPage} 注入到此类中，这是因为 {@link BTreeSuperPage} 对象是不可变的，
 * 不能从缓冲池中获取它，因为缓冲池很可能会驱逐它。{@link BTreeSuperPage} 的写入会直接通过 {@link happydb.storage.PageManager} 写入磁盘
 * </P>
 * <P><strong>每个 {@link BTreePageHolder} 实例都是被串行调用的，因此无需额外的并发控制</strong></P>
 *
 * @Author happysnaker
 * @Date 2022/11/21
 * @Email happysnaker@foxmail.com
 */
public class BTreePageHolder {

    @Getter
    private final BTreeSuperPage superPage;
    @Getter
    private final TransactionId tid;
    private final BufferPool pool;
    private final PageManager pm;

    private final HashSet<Page> pages = new HashSet<>();

    /**
     * 构造 BTreePageHolder
     *
     * @param superPage 不可变的超级页对象
     * @param tid       一次操作的事务 ID
     */
    public BTreePageHolder(BTreeSuperPage superPage, TransactionId tid) {
        this.superPage = superPage;
        this.tid = tid;
        this.pool = Database.getBufferPool();
        this.pm = Database.getCatalog().getPageManager(superPage.getPageId().getTableName());
    }

    /**
     * 以某种权限获取超级页
     *
     * @param perm 权限
     * @return 锁定超级页并返回
     */
    public BTreeSuperPage getSuperPage(Permissions perm) {
        if (perm == Permissions.READ_ONLY) {
            superPage.readLock(tid);
        } else {
            if (superPage.hasReadLock(tid))
                superPage.readUnLock(tid);
            superPage.writeLock(tid);
        }
        pages.add(superPage);
        return superPage;
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
     * 以某种权限获取 B+ 树页
     *
     * @param pid
     * @param perm
     * @return
     * @throws DbException
     */
    public BTreePage getBTreePage(PageId pid, Permissions perm) throws DbException {
        if (!pid.getTableName().equals(superPage.getPageId().getTableName())) {
            throw new DbException("模式不匹配");
        }
        if (perm == null) {
            throw new DbException("必须要指定权限");
        }
        if (pid.getPageNumber() == 0) {
            throw new DbException("请调用 getSuperPage 获取超级页");
        }

        BTreePage page = (BTreePage) pool.getPage(tid, pid, perm);
        pages.add(page);
        return page;
    }

    /**
     * 释放页面上特定的锁，此方法用于 B+ 树蟹行协议释放锁，锁可能会被多次释放，因此如果<strong>事务未持有锁，则会静默返回而不是抛出异常</strong>
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
        if (page.getPageId().equals(superPage.getPageId())) {
            if (perm == Permissions.READ_ONLY && page.hasReadLock(tid)) {
                page.readUnLock(tid);
            }
            if (perm == Permissions.READ_WRITE && page.hasWriteLock(tid)) {
                page.writeUnLock(tid);
            }
        } else {
            pool.unsafeReleasePage(tid, page, perm);
        }
        if (!page.hasReadLock(tid) && !page.hasWriteLock(tid)) {
            pages.remove(page);
        }
    }

    /**
     * 操作结束，释放所有的锁，并将自己弄脏的脏页(持有写锁)刷盘
     */
    public void releaseAllPages() throws IOException {
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
    }
}
