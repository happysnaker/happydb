package happydb.log;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.common.Debug;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.execution.BTreeSeqScan;
import happydb.index.IndexType;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.TransactionId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author happysnaker
 * @Date 2022/12/1
 * @Email happysnaker@foxmail.com
 */
public class UndoLogTest extends TestBase {
    String undoTableName = "tb" + UndoLogId.UNDO_LOG_TABLE_NAME_SUFFIX;
    TableDesc td;

    Record record1;
    Record record2;

    @Before
    public void setUp() throws Exception {
        td = TestUtil.createSimpleAndInsert(2, "tb", null);

        BTreeSeqScan scan = new BTreeSeqScan(new TransactionId(0), "tb", null, null);
        scan.open();

        record1 = scan.next();
        record2 = scan.next();

        Database.getBufferPool().transactionReleaseLock(new TransactionId(0));
    }

    @Test
    public void testInsertUndo() throws DbException, IOException {
        TransactionId tid = new TransactionId(0);
        UndoLog undoLog = Database.getLogBuffer().createInsertUndoLog(tid, record1);

        BTreeSeqScan scan = new BTreeSeqScan(new TransactionId(0), "tb", null, null);
        scan.open();
        Record[] recordAr = scan.getRecordAr();
        Assert.assertEquals(2, recordAr.length);

        undoLog.undo();

        scan = new BTreeSeqScan(new TransactionId(0), "tb", null, null);
        scan.open();
        recordAr = scan.getRecordAr();
        Assert.assertEquals(1, recordAr.length);
        TestUtil.assertRecordEquals(record2, recordAr[0], true);

        // 模拟事务在建立索引后，但是未插入数据时崩溃，然后回滚事务
        Record insert = record1.clone();
        HeapPageManager pm = (HeapPageManager) Database.getCatalog().getPageManager("tb");
        RecordId malloc = pm.malloc();
        insert.setField(0, new IntField(3));
        insert.setRecordId(malloc);
        Database.getCatalog().getIndex("tb", 0, IndexType.BTREE)
                .insert(tid, new IntField(3), malloc);
        undoLog = Database.getLogBuffer().createInsertUndoLog(tid, insert);

        undoLog.undo();

        scan = new BTreeSeqScan(new TransactionId(0), "tb", null, null);
        scan.open();
        recordAr = scan.getRecordAr();
        Assert.assertEquals(1, recordAr.length);
        TestUtil.assertRecordEquals(record2, recordAr[0], true);
    }

    @Test
    public void testUpdateUndo() throws Exception {
        TransactionId tid = new TransactionId(0);
        UndoLog undoLog = Database.getLogBuffer().createUpdateUndoLog(tid, record1);

        HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, record1.getRecordId().getPid(), Permissions.READ_ONLY);
        hp.updateRecord(record1.getRecordId(), record2.clone());

        BTreeSeqScan scan = new BTreeSeqScan(new TransactionId(0), "tb", null, null);
        scan.open();
        Record[] recordAr = scan.getRecordAr();
        Assert.assertEquals(new IntField(1), recordAr[0].getField(0));

        undoLog.undo();
        scan = new BTreeSeqScan(new TransactionId(0), "tb", null, null);
        scan.open();
        recordAr = scan.getRecordAr();
        Assert.assertEquals(new IntField(0), recordAr[0].getField(0));
    }


    @Test
    public void testDeleteUndo() throws DbException {
        TransactionId tid = new TransactionId(0);
        record1.setValid(false);
        UndoLog undoLog = Database.getLogBuffer().createDeleteUndoLog(tid, record1);

        BTreeSeqScan scan = new BTreeSeqScan(new TransactionId(0), "tb", null, null);
        scan.open();
        Record[] recordAr = scan.getRecordAr();
        Assert.assertEquals(1, recordAr.length);

        undoLog.undo();

        scan = new BTreeSeqScan(new TransactionId(0), "tb", null, null);
        scan.open();
        recordAr = scan.getRecordAr();
        Assert.assertEquals(2, recordAr.length);
        TestUtil.assertRecordEquals(record1, recordAr[0], false);
    }

    @Test
    public void testLastVersion() throws Exception {
        TransactionId tid = new TransactionId(0);
        var pm = (HeapPageManager) Database.getCatalog().getPageManager("tb");
        Record record = TestUtil.insertAndRunLog(0, pm.malloc(), tid);

        UndoLogId lp = record.getLogPointer();
        UndoLogPage page = (UndoLogPage) Database.getBufferPool().getPage(tid, lp.pid(), Permissions.READ_ONLY);
        UndoLog undoLog = page.readUndoLog(lp);

        Assert.assertNull(undoLog.getLastVersion(tid));

        Record update = record.clone();
        update.setField(1, new DoubleField(2.50));
        TestUtil.updateAndRunLog(record, update, tid);

        lp = update.getLogPointer();
        page = (UndoLogPage) Database.getBufferPool().getPage(tid, lp.pid(), Permissions.READ_ONLY);
        undoLog = page.readUndoLog(lp);
        TestUtil.assertRecordEquals(record, undoLog.getRecord(), true);

        Record delete = record.clone();
        delete.setValid(false);
        TestUtil.deleteAndRunLog(delete, tid);
        lp = delete.getLogPointer();
        page = (UndoLogPage) Database.getBufferPool().getPage(tid, lp.pid(), Permissions.READ_ONLY);
        undoLog = page.readUndoLog(lp);
        TestUtil.assertRecordEquals(record, undoLog.getRecord(), true);
    }
}
