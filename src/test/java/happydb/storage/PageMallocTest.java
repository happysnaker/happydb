package happydb.storage;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.common.Debug;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.transaction.TransactionId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author happysnaker
 * @Date 2022/11/18
 * @Email happysnaker@foxmail.com
 */
public class PageMallocTest extends TestBase {

    HeapPageManager pm;

    @Before
    public void setUp() throws Exception {
        try {
            Database.getCatalog().getPageManager("tb");
        } catch (NoSuchElementException e) {
            Database.getCatalog().createTable(TestUtil.createTableDesc(1, 1, 1, "tb", null));
        }

        pm = (HeapPageManager) Database.getCatalog().getPageManager("tb");
    }

    @Test
    public void testMalloc() {
        int n = 20480;
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        Set<RecordId> set = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < n; i++) {
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() {
                    try {
                        set.add(pm.malloc());
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                    setDone(true);
                }
            });
        }
        TestUtil.runManyThread(tasks, 10000L);
        Assert.assertEquals(n, set.size());
    }


    @Test
    public void testFree() {
        int n = 20480;
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        Set<RecordId> set = ConcurrentHashMap.newKeySet();
        for (int i = 0; i < n; i++) {
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() {
                    try {
                        RecordId malloc = pm.malloc();
                        set.add(malloc);
                        pm.free(malloc);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                    setDone(true);
                }
            });
        }
        TestUtil.runManyThread(tasks, 10000L);
        Assert.assertTrue(set.size() < n);
    }


    @Test(timeout = 1000L * 60 * 3)
    public void testInsertRecord() throws IOException, DbException {
        int n = 20480;
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        Record[] records = new Record[n];
        for (int i = 0; i < n; i++) {
            records[i] = TestUtil.createRecord(1, 1, 1, "tb");
        }
        Set<RecordId> set = ConcurrentHashMap.newKeySet();
        BufferPool bufferPool = Database.getBufferPool();
        for (int i = 0; i < n; i++) {
            final var fi = i;
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() {
                    try {
                        TransactionId tid = new TransactionId(fi);
                        RecordId malloc = pm.malloc();
                        set.add(malloc);
                        HeapPage page = (HeapPage) bufferPool.getPage(tid, malloc.getPid(), Permissions.READ_ONLY);
                        page.insertRecord(malloc.getRecordNumber(), records[fi]);
                        page.markDirty(true);
                        bufferPool.transactionReleaseLock(tid);

                        Assert.assertFalse(bufferPool.holdsAnyLock(tid));
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                    setDone(true);
                }
            });
        }
        TestUtil.runManyThread(tasks, 60000L * 3);
        Assert.assertEquals(n, set.size());

        HashMap<PageId, Page> map = new HashMap<>(bufferPool.pagePool);
        Assert.assertEquals(map.size(), new HashSet<>(map.values()).size());

        Debug.log("插入成功，开始写入磁盘并重新读出比对");
        bufferPool.evictPage(0L, true, false, true);
        Assert.assertTrue(bufferPool.pagePool.isEmpty());
        for (Record record : records) {
            HeapPage page = (HeapPage) bufferPool.getPage(new TransactionId(0), record.getRecordId().getPid(), Permissions.READ_ONLY);
            TestUtil.assertRecordEquals(record, page.readRecord(record.getRecordId()), true);
            bufferPool.transactionReleaseLock(new TransactionId(0));
        }
    }
}
