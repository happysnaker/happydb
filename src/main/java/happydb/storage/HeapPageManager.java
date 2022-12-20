package happydb.storage;

import happydb.common.ByteArray;
import happydb.common.DbFile;
import happydb.exception.DbException;
import lombok.Data;
import lombok.Getter;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
@Data
public class HeapPageManager implements PageManager {
    @Getter
    private String tableName;
    @Getter
    private DbFile dbFile;

    private final Set<RecordId> pool;

    public HeapPageManager(String tableName, DbFile dbFile) throws DbException, IOException {
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
        if (offset >= dbFile.getLength()) {
            throw new NoSuchElementException();
        }
        return new HeapPage(dbFile.read(offset, BufferPool.getPageSize()), pid);
    }

    @Override
    public synchronized void writePage(Page page) throws IOException {
        ByteArray serialized = page.serialized();
        assert serialized.length() == BufferPool.getPageSize();
        dbFile.write((long) page.getPageId().getPageNumber() * BufferPool.getPageSize(), serialized);
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
        for (int i = 0; i < numPages; i++) {
            Page page = readPage(new PageId(tableName, i));
            List<Integer> emptySlots = page.getEmptySlots();
            if (emptySlots.size() > 0) {
                pool.addAll(toRecordIdSet(emptySlots, page.getPageId()));
            }
        }
    }

    public Set<RecordId> toRecordIdSet(List<Integer> emptySlots, PageId pageId) {
        Set<RecordId> set = new HashSet<>();
        for (Integer emptySlot : emptySlots) {
            set.add(new RecordId(pageId, emptySlot));
        }
        return set;
    }

    /**
     * 分配一个空闲的插槽，如果插槽数不够，则新建一个页面
     *
     * @return Pair<PageId, Integer>
     */
    public synchronized RecordId malloc() throws IOException {
        if (pool.isEmpty()) {
            createNewPage();
            return malloc();
        }
        RecordId rid = null;
        for (RecordId recordId : pool) {
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
    public synchronized void free(RecordId rid) {
        pool.add(rid);
    }


    private synchronized void createNewPage() throws IOException {
        int numPages = (int) (dbFile.getLength() / BufferPool.getPageSize());
        HeapPage page = new HeapPage(HeapPage.createEmptyPageData(), new PageId(tableName, numPages));
        writePage(page);
        for (RecordId recordId : toRecordIdSet(page.getEmptySlots(), page.getPageId())) {
            if (pool.contains(recordId)) {
                throw new RuntimeException("异常错误");
            }
            pool.add(recordId);
        }
    }


    public synchronized void malloc(RecordId recordId) {
        pool.remove(recordId);
    }
}
