package happydb.log;

import happydb.common.ByteArray;
import happydb.common.Database;
import happydb.common.DbFile;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.storage.*;
import happydb.transaction.TransactionId;
import lombok.Data;
import lombok.Getter;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 此类实现与 {@link HeapPageManager} 基本一致
 * @Author happysnaker
 * @Date 2022/11/29
 * @Email happysnaker@foxmail.com
 */
@Data
public class UndoLogPageManager implements PageManager {
    @Getter
    private String tableName;
    @Getter
    private DbFile dbFile;

    private final Set<UndoLogId> pool;

    public UndoLogPageManager(String tableName, DbFile dbFile) throws DbException, IOException {
        this.tableName = tableName;
        this.dbFile = dbFile;
        this.pool = ConcurrentHashMap.newKeySet();
    }

    /**
     * 返回此 HeapFile 中的页数。
     * <p><strong>这个函数必须要实时进行计算，而不能缓存下来</strong></p>
     */
    public int numPages() throws IOException {
        return (int) (dbFile.getLength() / BufferPool.getPageSize());
    }

    @Override
    public synchronized Page readPage(PageId pid) throws IOException {
        long offset = (long) pid.getPageNumber() * BufferPool.getPageSize();
        if (pid.getPageNumber() == 0) {
            UndoLogSuperPage page = null;
            if (dbFile.getLength() < BufferPool.getPageSize()) {
                page = new UndoLogSuperPage(ByteArray.allocate(BufferPool.getPageSize()), pid);
                writePage(page);
            } else {
                page = new UndoLogSuperPage(dbFile.read(offset, BufferPool.getPageSize()), pid);
            }
            return page;
        }
        if (offset >= dbFile.getLength()) {
            throw new NoSuchElementException();
        }
        return new UndoLogPage(dbFile.read(offset, BufferPool.getPageSize()), pid);
    }

    @Override
    public synchronized void writePage(Page page) throws IOException {
        dbFile.write((long) page.getPageId().getPageNumber() * BufferPool.getPageSize(), page.serialized());
    }

    @Override
    public void close() {
        dbFile.close();
    }

    /**
     * 初始化空闲空间，此方法必须要等到 {@link happydb.common.Catalog} 初始化完毕后调用
     *
     * @throws IOException
     */
    public void loadFreePage() throws IOException {
        int numPages = (int) (dbFile.getLength() / BufferPool.getPageSize());
        for (int i = 1; i < numPages; i++) {
            Page page = readPage(new PageId(tableName, i));
            List<Integer> emptySlots = page.getEmptySlots();
            if (emptySlots.size() > 0) {
                pool.addAll(toUndoLogIdSet(emptySlots, page.getPageId()));
            }
        }
    }

    public Set<UndoLogId> toUndoLogIdSet(List<Integer> emptySlots, PageId pageId) {
        Set<UndoLogId> set = new HashSet<>();
        for (Integer emptySlot : emptySlots) {
            set.add(new UndoLogId(pageId, emptySlot));
        }
        return set;
    }


    /**
     * 分配一个空闲的插槽，如果插槽数不够，则新建一个页面
     *
     * @return Pair<PageId, Integer>
     */
    public synchronized UndoLogId malloc() throws IOException {
        if (pool.isEmpty()) {
            if (dbFile.getLength() == 0) {
                // 先创建超级页
                readPage(new PageId(tableName, 0));
            }
            createNewPage();
            return malloc();
        }
        UndoLogId rid = null;
        for (UndoLogId recordId : pool) {
            rid = recordId;
            break;
        }
        pool.remove(rid);
        return rid;
    }

    /**
     * 回收一个插槽
     *
     * @param rid
     */
    public synchronized void free(UndoLogId rid) {
        pool.add(rid);
    }


    private synchronized void createNewPage() throws IOException {
        int numPages = (int) (dbFile.getLength() / BufferPool.getPageSize());
        UndoLogPage page = new UndoLogPage(UndoLogPage.createEmptyPageData(), new PageId(tableName, numPages));
        writePage(page);
        for (UndoLogId recordId : toUndoLogIdSet(page.getEmptySlots(), page.getPageId())) {
            if (pool.contains(recordId)) {
                throw new RuntimeException("异常错误");
            }
            pool.add(recordId);
        }
    }


    /**
     * 返回 UndoLogPage 的迭代器，从第一页开始，他应该调用 {@link BufferPool} 读取页面，并以只读模式锁定页面
     */
    public Iterator<UndoLogPage> iterator(TransactionId tid) throws IOException, DbException {
        int n = (int) (this.dbFile.getLength() / BufferPool.getPageSize());
        List<UndoLogPage> pages = new ArrayList<>();
        for (int i = 1; i < n; i++) {
            pages.add((UndoLogPage) Database.getBufferPool()
                    .getPage(tid, new PageId(tableName, i), Permissions.READ_ONLY));
        }
        return pages.iterator();
    }

    public void malloc(UndoLogId id) {
        pool.remove(id);
    }
}
