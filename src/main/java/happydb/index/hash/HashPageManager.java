package happydb.index.hash;

import happydb.common.ByteArray;
import happydb.common.ByteList;
import happydb.common.Catalog;
import happydb.common.DbFile;
import happydb.exception.DbException;
import happydb.storage.BufferPool;
import happydb.storage.Page;
import happydb.storage.PageId;
import happydb.storage.PageManager;
import lombok.Data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @Author happysnaker
 * @Date 2023/1/4
 * @Email happysnaker@foxmail.com
 */
@Data
public class HashPageManager implements PageManager {

    private String tableName;

    private DbFile dbFile;


    public HashPageManager(String tableName, DbFile dbFile) throws DbException, IOException {
        this.tableName = tableName;
        this.dbFile = dbFile;
    }

    @Override
    public synchronized Page readPage(PageId pid) throws IOException {
        long offset = (long) pid.getPageNumber() * BufferPool.getPageSize();
        if (dbFile.getLength() < offset + BufferPool.getPageSize()) {
            throw new NoSuchElementException();
        }
        return new HashPage(dbFile.read(offset, BufferPool.getPageSize()), pid);
    }

    @Override
    public synchronized void writePage(Page page) throws IOException {
        dbFile.write((long) page.getPageId().getPageNumber() * BufferPool.getPageSize(), page.serialized());
    }

    /**
     * 创建 n 个新页，当 n <= 0 时，此方法返回总页数
     * @return 返回总的页面数量
     */
    public synchronized int malloc(int n) throws IOException {
        if (n == 0) {
            return (int) ((dbFile.getLength()) / BufferPool.getPageSize());
        }
        ByteArray data = ByteArray.allocate(n * BufferPool.getPageSize());
        dbFile.setLength(dbFile.getLength() + (long) n * BufferPool.getPageSize());
        int size = (int) ((dbFile.getLength()) / BufferPool.getPageSize());
        return size;
    }

    public synchronized Iterator<HashPage> iterator() throws IOException {
        List<HashPage> pageList = new ArrayList<>();
        for (int i = 0; i < malloc(0); i++) {
            pageList.add((HashPage) readPage(new PageId(tableName, i)));
        }
        return pageList.iterator();
    }


    @Override
    public void close() {
    }
}
