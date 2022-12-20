package happydb.log;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.common.Permissions;
import happydb.execution.BTreeSeqScan;
import happydb.index.IndexType;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.TransactionId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @Author happysnaker
 * @Date 2022/12/1
 * @Email happysnaker@foxmail.com
 */
public class RedoLogTest extends TestBase {
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
    public void testUpdateRedo() throws Exception {
        TransactionId tid = new TransactionId(0);

        Record update = record1.clone();
        update.setField(1, new DoubleField(250));

        // 不写入页面，模拟页面丢失

        UpdateRedoLog redoLog = Database.getLogBuffer().createUpdateRedoLog(tid, update);
        redoLog.setLsn(Long.MAX_VALUE);
        redoLog.redoIfNecessary();
        BTreeSeqScan scan = new BTreeSeqScan(new TransactionId(0), "tb", null, null);
        scan.open();
        Record[] recordAr = scan.getRecordAr();
        Assert.assertEquals(2, recordAr.length);
        Assert.assertEquals(new DoubleField(250), recordAr[0].getField(1));
    }


    @Test
    public void testDeleteRedo() throws Exception {
        TransactionId tid = new TransactionId(0);

        // 不写入页面，模拟页面丢失

        DeleteRedoLog redoLog = Database.getLogBuffer().createDeleteRedoLog(tid, record1.getRecordId());
        redoLog.setLsn(Long.MAX_VALUE);
        redoLog.redoIfNecessary();
        BTreeSeqScan scan = new BTreeSeqScan(new TransactionId(0), "tb", null, null);
        scan.open();
        Record[] recordAr = scan.getRecordAr();
        Assert.assertEquals(1, recordAr.length);
        TestUtil.assertRecordEquals(record2, recordAr[0], true);
    }


    @Test
    public void testAbortRedo() throws Exception {
        TransactionId tid = new TransactionId(0);
        LogBuffer logBuffer = Database.getLogBuffer();

        logBuffer.createInsertUndoLog(tid, record1);
        logBuffer.createInsertUndoLog(tid, record2);

        AbortRedoLog redoLog = logBuffer.createAbortRedoLog(tid);
        redoLog.setLsn(Long.MAX_VALUE);
        redoLog.redoIfNecessary();

        BTreeSeqScan scan = new BTreeSeqScan(new TransactionId(0), "tb", null, null);
        scan.open();
        Record[] recordAr = scan.getRecordAr();
        Assert.assertEquals(0, recordAr.length);
    }

    @Test
    public void testInsertRedo() throws Exception {
        TransactionId tid = new TransactionId(0);

        Record insert = record1.clone();
        HeapPageManager pm = (HeapPageManager) Database.getCatalog().getPageManager("tb");
        RecordId malloc = pm.malloc();
        insert.setField(0, new IntField(3));
        insert.setRecordId(malloc);

        // 不写入页面，模拟页面丢失
        // 索引文件在操作完成后就被刷盘了，索引文件不会丢失，如果丢失的话，事务肯定未提交
        Database.getCatalog().getIndex("tb", 0, IndexType.BTREE)
                .insert(tid, new IntField(3), malloc);

        InsertRedoLog redoLog = Database.getLogBuffer().createInsertRedoLog(tid, insert);
        redoLog.setLsn(Long.MAX_VALUE);
        redoLog.redoIfNecessary();
        BTreeSeqScan scan = new BTreeSeqScan(new TransactionId(0), "tb", null, null);
        scan.open();
        Record[] recordAr = scan.getRecordAr();
        Assert.assertEquals(3, recordAr.length);
        Assert.assertEquals(new IntField(3), recordAr[2].getField(0));
    }
}
