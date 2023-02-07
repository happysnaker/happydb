package happydb.index.hash;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.exception.DbException;
import happydb.execution.Predicate;
import happydb.index.Index;
import happydb.index.IndexType;
import happydb.storage.*;
import happydb.transaction.TransactionId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @Author happysnaker
 * @Date 2023/1/5
 * @Email happysnaker@foxmail.com
 */
public class HashIndexTest extends TestBase {
    @Before
    public void setUp() throws Exception {
        BufferPool.DEFAULT_PAGES = 1024;
        Database.reset();

        TableDesc td = TestUtil.createTableDesc(1, 0, 0,
                "tb",
                integer -> integer == 0 ? IndexType.indexSetToInt(Set.of(IndexType.HASH)) : 0);

        Database.getCatalog().createTable(td);
    }

    @Test
    public void testFindIndex() {
        Index index = Database.getCatalog().getIndex("tb", 0, IndexType.HASH);
        Assert.assertNotNull(index);
    }

    @Test
    public void testInsertAndSearch() throws IOException, DbException {
        Index index = Database.getCatalog().getIndex("tb", 0, IndexType.HASH);
        int n = 10;
        PageId pid = new PageId("tb", 2);
        for (int i = 0; i < n; i++) {
            index.insert(new TransactionId(0), new IntField(i % 2), new RecordId(pid, i));
            // 确保每次都能释放锁
            Database.getBufferPool().evictPage(100, true, false, true);
            Assert.assertTrue(Database.getBufferPool().pagePool.isEmpty());
        }
        List<RecordId> search = index.search(new TransactionId(0), Predicate.Op.EQUALS, new IntField(0));
        Assert.assertEquals(n / 2, search.size());
        for (int i = 0; i < n / 2; i++) {
            Assert.assertEquals(new RecordId(pid, i * 2), search.get(i));
        }
    }
}
