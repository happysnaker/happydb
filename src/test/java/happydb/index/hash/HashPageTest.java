package happydb.index.hash;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.exception.DuplicateValueException;
import happydb.execution.Predicate;
import happydb.index.IndexType;
import happydb.index.btree.BTreePage;
import happydb.index.btree.BTreeSuperPage;
import happydb.storage.*;
import happydb.transaction.TransactionId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author happysnaker
 * @Date 2023/1/4
 * @Email happysnaker@foxmail.com
 */
public class HashPageTest extends TestBase {
    BufferPool pool;

    String tb = "tb-0-" + IndexType.HASH;

    HashPageManager pm;

    @Before
    public void setUp() throws DuplicateValueException, IOException, DbException {
        pool = Database.getBufferPool();
        TableDesc td = TestUtil.createTableDesc(10, 0, 0,
                "tb",
                    integer -> integer == 0 ? IndexType.indexSetToInt(Set.of(IndexType.HASH)) : 0);

        Database.getCatalog().createTable(td);

        pm = (HashPageManager) Database.getCatalog().getPageManager(tb);
    }

    @Test
    public void testRwSuperPage() throws Exception {
        int n = pm.malloc(5);
        Assert.assertEquals(5, n);

        HashPage page = (HashPage) pool.getPage(new TransactionId(0),
                new PageId(tb, 0), Permissions.READ_WRITE);
        Assert.assertNotNull(page);
        Assert.assertEquals(page.getEmptySlots().size(), page.getMaxNumEntries());

        HashEntry entry1 = new HashEntry(new IntField(1), new RecordId(new PageId("tb", 1), 1));
        HashEntry entry2 = new HashEntry(new IntField(12), new RecordId(new PageId("tb", 1), 1));
        page.putIfAbsent(2, entry1);
        page.putIfAbsent(4, entry2);

        pm.writePage(page);
        page = (HashPage) pm.readPage(page.getPageId());
        Assert.assertEquals(page.readEntry(2), entry1);
        Assert.assertEquals(page.readEntry(4), entry2);
        Assert.assertEquals(page.getMaxNumEntries() - 2, page.getEmptySlots().size());
    }


    /**
     * 确保只有一个线程能进入扩容，并且不会死锁
     * @throws Exception
     */
    @Test
    public void testResizeLock() throws Exception {
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger s = new AtomicInteger(0);
        for (int i = 0; i < 500; i++) {
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    HashPageHolder holder = new HashPageHolder(new TransactionId(counter.incrementAndGet()), tb);
                    holder.init();
                    s.incrementAndGet();
                    holder.end();
                }
            });
        }

        final int[] g = new int[2];

        for (int i = 0; i < 500; i++) {
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    HashPageHolder holder = new HashPageHolder(new TransactionId(counter.incrementAndGet()), tb);
                    holder.init();
                    holder.resizeLock();

                    g[0] = s.get();
                    g[1]++;
                }
            });
        }

        try {
            TestUtil.runManyThread(tasks, 10 * 1000);
        } catch (IllegalStateException ignore) {}
        Assert.assertEquals(g[0], s.get());
        Assert.assertEquals(g[1], 1);
    }

    @Test
    public void testIt() throws Exception {
        int n = pm.malloc(5);
        Assert.assertEquals(5, n);

        HashPage page = (HashPage) pool.getPage(new TransactionId(0),
                new PageId(tb, 0), Permissions.READ_WRITE);
        Assert.assertNotNull(page);
        Assert.assertEquals(page.getEmptySlots().size(), page.getMaxNumEntries());

        HashEntry entry1 = new HashEntry(new IntField(1), new RecordId(new PageId("tb", 1), 1));
        HashEntry entry2 = new HashEntry(new IntField(12), new RecordId(new PageId("tb", 1), 1));
        HashEntry entry3 = new HashEntry(new IntField(122), new RecordId(new PageId("tb", 12), 1));
        HashEntry entry4 = new HashEntry(new IntField(1222), new RecordId(new PageId("tb", 11), 1));
        page.putIfAbsent(2, entry1);
        page.putIfAbsent(4, entry2);

        pm.writePage(page);
        page = (HashPage) pool.getPage(new TransactionId(0),
                new PageId(tb, 4), Permissions.READ_WRITE);
        page.putIfAbsent(3, entry3);
        page.putIfAbsent(6, entry4);
        pm.writePage(page);

        List<HashEntry> list = new ArrayList<>();
        Iterator<HashPage> iterator = pm.iterator();
        while (iterator.hasNext()) {
            Iterator<HashEntry> iterator1 = iterator.next().iterator();
            while (iterator1.hasNext()) {
                HashEntry next = iterator1.next();
                list.add(next);
            }
        }
        list.sort((a, b) -> a.getKey().compare(Predicate.Op.LESS_THAN, b.getKey()) ? -1 : 1);

        Assert.assertEquals(4, list.size());
        Assert.assertEquals(entry1, list.get(0));
        Assert.assertEquals(entry2, list.get(1));
        Assert.assertEquals(entry3, list.get(2));
        Assert.assertEquals(entry4, list.get(3));
    }
}
