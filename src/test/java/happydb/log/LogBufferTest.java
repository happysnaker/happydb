package happydb.log;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Catalog;
import happydb.common.Database;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.execution.BTreeSeqScan;
import happydb.execution.Predicate;
import happydb.index.IndexType;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.TransactionId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * @Author happysnaker
 * @Date 2022/12/1
 * @Email happysnaker@foxmail.com
 */
public class LogBufferTest extends TestBase {
    TableDesc td;

    private Page insert(int pkField, TransactionId tid) throws IOException, DbException {
        Catalog catalog = Database.getCatalog();
        HeapPageManager pm = (HeapPageManager) catalog.getPageManager("tb");

        RecordId malloc = pm.malloc();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, malloc.getPid(), Permissions.READ_ONLY);

        Record record = new Record(td);
        record.setField(0, new IntField(pkField));
        record.setField(1, new DoubleField(pkField));
        record.setField(2, new StringField(""));
        record.setValid(true);
        record.setLastModify(tid);
        record.setRecordId(malloc);


        LogBuffer logBuffer = Database.getLogBuffer();

        UndoLog undoLog = logBuffer.createInsertUndoLog(tid, record);

        record.setLogPointer(undoLog.getId());
        page.insertRecord(malloc, record);
        catalog.getIndex("tb", 0, IndexType.BTREE).insert(tid, record.getField(0), malloc);

        InsertRedoLog redoLog = logBuffer.createInsertRedoLog(tid, record);


        Assert.assertTrue(redoLog.getLsn() <= page.getLsn());
        return page;
    }


    @Before
    public void setUp() throws Exception {
        td = TestUtil.createSimpleAndInsert(0, "tb", null);
        Database.getBufferPool().transactionReleaseLock(new TransactionId(0));
    }

    @Test
    public void testCommit() throws Exception {
        TransactionId tid = new TransactionId(0);
        TransactionId tid1 = new TransactionId(1);
        LogBuffer logBuffer = Database.getLogBuffer();

        insert(0, tid);
        insert(11, tid1);
        insert(1, tid);
        logBuffer.transactionCommit(tid);

        Iterator<RedoLog> iterator = logBuffer.iterator();
        iterator.next(); // 先 undo 在 redi
        Assert.assertEquals(new IntField(0), ((Record) ((InsertRedoLog) iterator.next()).getData()).getField(0));
        iterator.next(); // 先 undo 在 redi
        Assert.assertEquals(new IntField(1), ((Record) ((InsertRedoLog) iterator.next()).getData()).getField(0));
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testAbort() throws Exception {
        TransactionId tid = new TransactionId(0);
        TransactionId tid1 = new TransactionId(1);
        LogBuffer logBuffer = Database.getLogBuffer();

        DataPage p1 = (DataPage) insert(0, tid);
        insert(11, tid1);
        DataPage p2 = (DataPage) insert(1, tid);
        logBuffer.transactionAbort(tid);

        BTreeSeqScan scan = new BTreeSeqScan(tid, "tb", null, null);
        scan.open();
        Assert.assertEquals(1, scan.getRecordAr().length);

        Iterator<RedoLog> iterator = logBuffer.iterator();
        iterator.next(); // 先 undo 在 redi
        Assert.assertEquals(new IntField(0), ((Record) ((InsertRedoLog) iterator.next()).getData()).getField(0));
        iterator.next(); // 先 undo 在 redi
        Assert.assertEquals(new IntField(1), ((Record) ((InsertRedoLog) iterator.next()).getData()).getField(0));
        if (iterator.next() instanceof AbortRedoLog arl) {
            Assert.assertEquals(0, arl.xid().getXid());
            Assert.assertEquals(p1.getLsn(), arl.getLsn());
            Assert.assertEquals(p2.getLsn(), arl.getLsn());
        } else {
            fail();
        }
        Assert.assertFalse(iterator.hasNext());
    }

    @Test
    public void testPushCkp() throws Exception {
        LogBuffer.TRUNCATE_HOLDER = 0;
        LogBuffer logBuffer = Database.getLogBuffer();
        TransactionId tid = new TransactionId(0);

        DataPage p1 = (DataPage) insert(0, tid);
        DataPage p2 = (DataPage) insert(1, tid);
        logBuffer.transactionAbort(tid);
        logBuffer.pushCheckPoint(p2.getLsn());

        Iterator<RedoLog> iterator = logBuffer.iterator();
        RedoLog next = iterator.next();
        if (next instanceof AbortRedoLog arl) {
            Assert.assertEquals(0, arl.xid().getXid());
            Assert.assertEquals(p1.getLsn(), arl.getLsn());
            Assert.assertEquals(p2.getLsn(), arl.getLsn());
        } else {
            fail();
        }
    }


    @Test
    public void testManyThreadAbort() throws Exception {
        BufferPool.DEFAULT_PAGES = 500;
        Database.reset();
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();

        int n = 1024;
        for (int i = 0; i < n; i++) {
            int finalI = i;
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    TransactionId tid = new TransactionId(finalI);
                    insert(finalI, tid);

                    BTreeSeqScan scan = new BTreeSeqScan(tid, "tb", null,
                            new Predicate(0, Predicate.Op.EQUALS, new IntField(finalI)));
                    scan.open();
                    Record next = scan.next();

                    next.setValid(false);
                    next.setLastModify(tid);

                    UndoLog undoLog = Database.getLogBuffer().createDeleteUndoLog(tid, next);
                    next.setLogPointer(undoLog.getId());
                    Database.getLogBuffer().createDeleteRedoLog(tid, next.getRecordId());
                    Database.getLogBuffer().transactionAbort(tid);
                    setDone(true);
                }
            });
        }
        TestUtil.runManyThread(tasks, 1000 * 60);
        BTreeSeqScan scan = new BTreeSeqScan(new TransactionId(0), "tb", null,
                null);
        scan.open();
        Assert.assertFalse(scan.hasNext());

        Iterator<RedoLog> iterator = Database.getLogBuffer().iterator();
        int sum = 0;
        while (iterator.hasNext()) {
            RedoLog next = iterator.next();
            if (next instanceof AbortRedoLog) {
                sum++;
            }
        }
        Assert.assertEquals(n, sum);
    }


    @Test
    public void testCheckPoint() throws Exception {
        LogBuffer.TRUNCATE_HOLDER = 100;

        TransactionId tid = new TransactionId(0);
        TransactionId tid1 = new TransactionId(1);
        LogBuffer logBuffer = Database.getLogBuffer();

        DataPage p1 = (DataPage) insert(0, tid);
        Page insert = insert(11, tid1);
        DataPage p2 = (DataPage) insert(1, tid);
        logBuffer.transactionAbort(tid);
        insert(21, tid1);


        Database.getCheckPoint().sharkCheckPoint();

        Database.reset();
        BTreeSeqScan scan = new BTreeSeqScan(tid, "tb", null, null);
        scan.open();
        Assert.assertEquals(2, scan.getRecordAr().length);

        Assert.assertFalse(logBuffer.iterator().hasNext());
    }
}
