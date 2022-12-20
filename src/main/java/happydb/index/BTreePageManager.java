package happydb.index;

import happydb.common.ByteArray;
import happydb.common.Catalog;
import happydb.common.DbFile;
import happydb.exception.DbException;
import happydb.storage.*;
import lombok.Data;

import java.io.IOException;

/**
 * @Author happysnaker
 * @Date 2022/11/21
 * @Email happysnaker@foxmail.com
 */
@Data
public class BTreePageManager implements PageManager {
    private String tableName;

    private DbFile dbFile;


    public BTreePageManager(String tableName, DbFile dbFile) throws DbException, IOException {
        this.tableName = tableName;
        this.dbFile = dbFile;
    }


    @Override
    public synchronized Page readPage(PageId pid) throws IOException {
        if (pid.getPageNumber() == 0) {
            return readSuperPage();
        }
        ByteArray read = dbFile.read((long) pid.getPageNumber() * BufferPool.getPageSize(), BufferPool.getPageSize());
        return switch (read.readByte(0)) {
            case BTreePage.INTERNAL -> new BTreeInternalPage(read, pid);
            case BTreePage.LEAF -> new BTreeLeafPage(read, pid);
            default -> {
                throw new RuntimeException("类型不匹配");
            }
        };
    }


    private Page readSuperPage() throws IOException {
        // 初始化超级页
        if (dbFile.getLength() < BufferPool.getPageSize()) {
            Page page = new BTreeSuperPage(BTreeSuperPage.createEmptyPageData(
                    Catalog.getFieldTypeFromIndexTableName(tableName)),
                    new PageId(tableName, 0)
            );
            writePage(page);
            return page;
        }
        return new BTreeSuperPage(dbFile.read(0, BufferPool.getPageSize()), new PageId(tableName, 0));
    }


    @Override
    public synchronized void writePage(Page page) throws IOException {
        dbFile.write((long) page.getPageId().getPageNumber() * BufferPool.getPageSize(), page.serialized());
    }


    @Override
    public void close() {
        dbFile.close();
    }
}
