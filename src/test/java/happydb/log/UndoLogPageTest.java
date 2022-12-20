package happydb.log;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.common.Debug;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.TransactionId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author happysnaker
 * @Date 2022/12/1
 * @Email happysnaker@foxmail.com
 */
public class UndoLogPageTest extends TestBase {

    String undoTableName = "tb" + UndoLogId.UNDO_LOG_TABLE_NAME_SUFFIX;
    TableDesc td;

    @Before
    public void setUp() throws Exception {
        td = TestUtil.createSimpleAndInsert(1, "tb", null);
    }

    @Test
    public void testManyThreadInsertAndDelete() throws Exception {
        UndoLogPageManager pm = (UndoLogPageManager) Database.getCatalog().getPageManager(undoTableName);

        BufferPool.DEFAULT_PAGES = 100;
        Database.reset();

        Record record = new Record(td);
        record.setField(0, new IntField(0));
        record.setField(1, new DoubleField(0));
        record.setField(2, new StringField(""));
        record.setRecordId(new RecordId(new PageId("tb", 1), 1));

        UndoLogSuperPage superPage = (UndoLogSuperPage) Database.getBufferPool()
                .getPage(new TransactionId(-1), new PageId(undoTableName, 0), Permissions.READ_ONLY);
        Database.getBufferPool().transactionReleaseLock(new TransactionId(-1));


        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        for (int i = 0; i < 5096; i++) {
            int finalI = i;
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    TransactionId tid = new TransactionId(finalI);
                    UndoLogId malloc = pm.malloc();
                    UndoLogPage page = (UndoLogPage) Database.getBufferPool()
                            .getPage(tid, malloc.pid(), Permissions.READ_ONLY);

                    page.insertUndoLog(malloc, new UndoLog(0, record, record.getRecordId(), tid));
                    superPage.markTransactionOn(tid, true);
                    page.deleteUndoLog(malloc);
                    superPage.markTransactionOn(tid, false);
                    Database.getBufferPool().transactionReleaseLock(tid);
                    setDone(true);
                }
            });
        }

        TestUtil.runManyThread(tasks, 1000 * 60);
        Database.getBufferPool().evictPage(0, true, false, true);
        Assert.assertTrue(Database.getBufferPool().pagePool.isEmpty());
        for (int i = 0; i < 5096; i++) {
            TransactionId tid = new TransactionId(i);
            Assert.assertTrue(superPage.getTransactionUndoLog(tid).isEmpty());
            superPage.markTransactionOn(tid, true);
            Assert.assertTrue(superPage.getTransactionUndoLog(tid).isEmpty());
        }

    }

    @Test
    public void testPageInsertByManyThread() throws IOException, DbException {
        UndoLogPageManager pm = (UndoLogPageManager) Database.getCatalog().getPageManager(undoTableName);
        TransactionId tid = new TransactionId(0);

        Record record = new Record(td);
        record.setField(0, new IntField(0));
        record.setField(1, new DoubleField(0));
        record.setField(2, new StringField(""));
        record.setRecordId(new RecordId(new PageId("tb", 1), 1));

        UndoLogId malloc = pm.malloc();
        UndoLogPage page = (UndoLogPage) Database.getBufferPool().getPage(tid, malloc.pid(), Permissions.READ_ONLY);

        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        for (int i = 0; i < page.getMaxNumEntries(); i++) {
            int finalI = i;
            UndoLogPage finalPage = page;
            Record finalRecord = record.clone();
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    TransactionId tid1 = new TransactionId(finalI);
                    finalRecord.setLastModify(tid1);
                    finalPage.insertUndoLog(finalI, new UndoLog(finalI, finalRecord, record.getRecordId(), tid1));
                    setDone(true);
                }
            });
        }
        TestUtil.runManyThread(tasks, 1000 * 10);

        pm.writePage(page);
        page = (UndoLogPage) pm.readPage(page.getPid());
        Assert.assertTrue(page.getEmptySlots().isEmpty());

        for (int i = 0; i < page.getMaxNumEntries(); i++) {
            UndoLog undoLog = page.readUndoLog(new UndoLogId(page.getPid(), i));
            Assert.assertEquals(new TransactionId(i), undoLog.getRecord().getLastModify());
            Assert.assertEquals(record.getRecordId(), undoLog.getRecordId());
            TestUtil.assertRecordEquals(record, undoLog.getRecord(), false);
        }
    }


    @Test
    public void testSuperPage() throws DbException, IOException {
        UndoLogPageManager pm = (UndoLogPageManager) Database.getCatalog().getPageManager(undoTableName);
        TransactionId tid = new TransactionId(0);
        UndoLogSuperPage superPage = (UndoLogSuperPage) Database.getBufferPool()
                .getPage(tid, new PageId(undoTableName, 0), Permissions.READ_ONLY);

        Record record = new Record(td);
        record.setField(0, new IntField(0));
        record.setField(1, new DoubleField(0));
        record.setField(2, new StringField(""));

        UndoLogId malloc = pm.malloc();
        UndoLogPage page = (UndoLogPage) Database.getBufferPool()
                .getPage(tid, malloc.pid(), Permissions.READ_ONLY);
        page.insertUndoLog(malloc, new UndoLog(0, record, record.getRecordId(), tid));
        Debug.log(malloc);

        record.setField(0, new IntField(1));

        malloc = pm.malloc();
        page = (UndoLogPage) Database.getBufferPool()
                .getPage(tid, malloc.pid(), Permissions.READ_ONLY);
        page.insertUndoLog(malloc, new UndoLog(1, record, record.getRecordId(), tid));
        Debug.log(malloc);

        Assert.assertTrue(superPage.getTransactionUndoLog(tid).isEmpty());
        superPage.markTransactionOn(tid, true);
        Assert.assertFalse(superPage.getTransactionUndoLog(tid).isEmpty());
        Assert.assertEquals(new IntField(0),
                superPage.getTransactionUndoLog(tid).get(0).getRecord().getField(0));
        Assert.assertEquals(new IntField(1),
                superPage.getTransactionUndoLog(tid).get(1).getRecord().getField(0));
        superPage.markTransactionOn(tid, false);
        Assert.assertTrue(superPage.getTransactionUndoLog(tid).isEmpty());


        superPage = (UndoLogSuperPage) pm.readPage(superPage.getPid());

        Assert.assertTrue(superPage.getTransactionUndoLog(tid).isEmpty());
        superPage.markTransactionOn(tid, true);
        Assert.assertFalse(superPage.getTransactionUndoLog(tid).isEmpty());
        Assert.assertEquals(new IntField(0),
                superPage.getTransactionUndoLog(tid).get(0).getRecord().getField(0));
        Assert.assertEquals(new IntField(1),
                superPage.getTransactionUndoLog(tid).get(1).getRecord().getField(0));
    }

    @Test
    public void testMallocFree() {
        UndoLogPageManager pm = (UndoLogPageManager) Database.getCatalog().getPageManager(undoTableName);

        Set<UndoLogId> set = ConcurrentHashMap.newKeySet();
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        for (int i = 0; i < 10240; i++) {
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    set.add(pm.malloc());
                    setDone(true);
                }
            });
        }
        TestUtil.runManyThread(tasks, 1000 * 10);

        Assert.assertEquals(10240, set.size());

        set.clear();
        for (int i = 0; i < 10240; i++) {
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    UndoLogId malloc = pm.malloc();
                    set.add(malloc);
                    pm.free(malloc);
                    setDone(true);
                }
            });
        }
        Assert.assertTrue(set.size() < 10240);
    }
}
