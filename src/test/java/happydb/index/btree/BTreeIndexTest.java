package happydb.index.btree;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author happysnaker
 * @Date 2022/11/22
 * @Email happysnaker@foxmail.com
 */
public class BTreeIndexTest extends TestBase {

    @Before
    public void setUp() throws Exception {
        // 腾出页面供 B+ 树插入
        BufferPool.DEFAULT_PAGES = 100;
        Database.reset();

        TableDesc td = TestUtil.createTableDesc(1, 0, 0,
                "tb",
                integer -> integer == 0 ? IndexType.indexSetToInt(Set.of(IndexType.BTREE)) : 0);

        Database.getCatalog().createTable(td);
    }

    @Test
    public void testFindIndex() {
        Index index = Database.getCatalog().getIndex("tb", 0, IndexType.BTREE);
        Assert.assertNotNull(index);
    }

    @Test
    public void testInsertAndSearch() throws IOException, DbException {
        Index index = Database.getCatalog().getIndex("tb", 0, IndexType.BTREE);
        int n = 100;
        PageId pid = new PageId("tb", 2);
        for (int i = 0; i < n; i++) {
            index.insert(new TransactionId(0), new IntField(i), new RecordId(pid, i));
        }
        List<RecordId> search = index.search(new TransactionId(0), null, null);
        Assert.assertEquals(n, search.size());
        for (int i = 0; i < n; i++) {
            Assert.assertEquals(new RecordId(pid, i), search.get(i));
        }
    }


    @Test
    public void testSearch() throws IOException, DbException {
        Index index = Database.getCatalog().getIndex("tb", 0, IndexType.BTREE);
        int n = 50; // 偶数
        PageId pid = new PageId("tb", 2);
        for (int i = 0; i < n; i++) {
            index.insert(new TransactionId(0), new IntField(i), new RecordId(pid, i));
            // 确保每次都能释放锁
            Database.getBufferPool().evictPage(100, true, false, true);
            Assert.assertTrue(Database.getBufferPool().pagePool.isEmpty());
        }
        // 全表扫描
        List<RecordId> search = index.search(new TransactionId(0), null, null);
        Assert.assertEquals(n, search.size());
        for (int i = 0; i < n; i++) {
            Assert.assertEquals(new RecordId(pid, i), search.get(i));
        }

        // 小于
        search = index.search(new TransactionId(0), Predicate.Op.LESS_THAN, new IntField(n / 2));
        Assert.assertEquals(n / 2, search.size());
        for (int i = 0; i < n / 2; i++) {
            Assert.assertEquals(new RecordId(pid, i), search.get(i));
        }

        // 小于等于
        search = index.search(new TransactionId(0), Predicate.Op.LESS_THAN_OR_EQ, new IntField(n / 2));
        Assert.assertEquals(n / 2 + 1, search.size());
        for (int i = 0; i <= n / 2; i++) {
            Assert.assertEquals(new RecordId(pid, i), search.get(i));
        }

        // 大于
        search = index.search(new TransactionId(0), Predicate.Op.GREATER_THAN, new IntField(n / 2 - 1));
        Assert.assertEquals(n / 2, search.size());
        for (int i = n / 2; i < n; i++) {
            Assert.assertEquals(new RecordId(pid, i), search.get(i - n / 2));
        }

        // 大于等于
        search = index.search(new TransactionId(0), Predicate.Op.GREATER_THAN_OR_EQ, new IntField(n / 2 - 1));
        Assert.assertEquals(n / 2 + 1, search.size());
        for (int i = n / 2 - 1; i < n; i++) {
            Assert.assertEquals(new RecordId(pid, i), search.get(i - (n / 2 - 1)));
        }

        // 等于
        search = index.search(new TransactionId(0), Predicate.Op.EQUALS, new IntField(0));
        Assert.assertEquals(1, search.size());
        Assert.assertEquals(new RecordId(pid, 0), search.get(0));

        // 不等于
        search = index.search(new TransactionId(0), Predicate.Op.NOT_EQUALS, new IntField(n - 1));
        Assert.assertEquals(n - 1, search.size());
        for (int i = 0; i < n - 1; i++) {
            Assert.assertEquals(new RecordId(pid, i), search.get(i));
        }
    }


    @Test
    public void testInsertByManyThread() throws IOException, DbException {
        Index index = Database.getCatalog().getIndex("tb", 0, IndexType.BTREE);
        int n = 10, task = 10;
        AtomicInteger ai = new AtomicInteger(-1);
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        PageId pid = new PageId("tb", 2);
        for (int i = 0; i < n; i++) {
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    for (int j = 0; j < task; j++) {
                        int get = ai.incrementAndGet();
                        index.insert(new TransactionId(get), new IntField(get), new RecordId(pid, get));
                    }
                    setDone(true);
                }
            });
        }
        TestUtil.runManyThread(tasks, 1000 * 60L * 5);
        List<RecordId> search = index.search(new TransactionId(0), null, null);
        Assert.assertEquals(n * task, search.size());
        for (int i = 0; i < n * task; i++) {
            Assert.assertEquals(new RecordId(pid, i), search.get(i));
        }
    }


    @Test
    public void testInsertAndReadByManyThread() throws IOException, DbException {
        Index index = Database.getCatalog().getIndex("tb", 0, IndexType.BTREE);
        int n = 100;
        AtomicInteger ai = new AtomicInteger(-1);
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        PageId pid = new PageId("tb", 2);
        for (int i = 0; i < n; i++) {
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    int get = ai.incrementAndGet();
                    if (get < n / 2) {
                        index.insert(new TransactionId(get), new IntField(get), new RecordId(pid, get));
                    } else {
                        index.search(new TransactionId(get), null, null);
                    }
                    setDone(true);
                }
            });
        }
        TestUtil.runManyThread(tasks, 1000 * 60L * 5);
        List<RecordId> search = index.search(new TransactionId(0), null, null);
        Assert.assertEquals((n / 2), search.size());
        for (int i = 0; i < (n / 2); i++) {
            Assert.assertEquals(new RecordId(pid, i), search.get(i));
        }
    }
}
