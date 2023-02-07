package happydb.log;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.index.Index;
import happydb.index.IndexType;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.TransactionId;
import happydb.transaction.TransactionManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * @Author happysnaker
 * @Date 2022/12/1
 * @Email happysnaker@foxmail.com
 */
public class RecoveryTest extends TestBase {
    TableDesc td;

    HeapPageManager pm;

    Index index;

    LogBuffer logBuffer;



    @Before
    public void setUp() throws Exception {
        td = TestUtil.createSimpleAndInsert(0, "tb", null);
        Database.getBufferPool().transactionReleaseLock(new TransactionId(0));

        pm = (HeapPageManager) Database.getCatalog().getPageManager("tb");
        index = Database.getCatalog().getIndex("tb", 0, IndexType.BTREE);
        logBuffer = Database.getLogBuffer();
//        Database.getTransactionManager() = Database.getTransactionManager();
        
        Database.recoveryTest = true;
    }


    /**
     * 一个事务插入、更新然后提交，页面没有刷回，突然宕机
     */
    @Test
    public void condition1() throws Exception {
        TransactionId tid = Database.getTransactionManager().begin();

        Record record = TestUtil.insertAndRunLog(250, pm.malloc(), tid);

        Record clone = record.clone();
        record.setField(1, new DoubleField(250));
        TestUtil.updateAndRunLog(clone, record, tid);

        Database.getTransactionManager().commit(tid, false);
        TestUtil.assertRecordEquals(record, TestUtil.getRecordAr("tb", new TransactionId(0))[0], true);
        // crash
        Database.reset();
        try {
            System.out.println("TestUtil.getRecordAr(\"tb\", new TransactionId(0)).length = " + TestUtil.getRecordAr("tb", new TransactionId(0)).length);
            fail();
        } catch (Exception ignore) {
        }
        Recovery.recovery();
        TestUtil.assertRecordEquals(record, TestUtil.getRecordAr("tb", new TransactionId(0))[0], true);
    }


    /**
     * 一个事务插入、更新页面由检查点刷回，事务回滚，然后突然宕机，磁盘页需要回滚
     */
    @Test
    public void condition2() throws Exception {
        TransactionId tid = Database.getTransactionManager().begin();

        Record record = TestUtil.insertAndRunLog(250, pm.malloc(), tid);

        Record clone = record.clone();
        record.setField(1, new DoubleField(250));
        TestUtil.updateAndRunLog(clone, record, tid);

        Database.getCheckPoint().sharkCheckPoint();
        Database.getTransactionManager().rollback(tid);
        // crash
        Database.reset();

        TestUtil.assertRecordEquals(record, TestUtil.getRecordAr("tb", new TransactionId(0))[0], true);

        Recovery.recovery();
        Assert.assertEquals(0, TestUtil.getRecordAr("tb", new TransactionId(0)).length);
    }


    /**
     * 事务 A 插入然后提交，事务 B 删除，检查点发生强制刷盘，然后宕机，事务 B 未完成，磁盘页需要回滚
     */
    @Test
    public void condition3() throws Exception {
        TransactionId tid1 = Database.getTransactionManager().begin();
        TransactionId tid2 = Database.getTransactionManager().begin();

        Record record = TestUtil.insertAndRunLog(250, pm.malloc(), tid1);
        Record clone = record.clone();
        Database.getTransactionManager().commit(tid1, false);
        record.setValid(false);
        TestUtil.deleteAndRunLog(record, tid2);
        Database.getCheckPoint().sharkCheckPoint();

        // crash
        Database.reset();
        Assert.assertEquals(0, TestUtil.getRecordAr("tb", new TransactionId(0)).length);

        Recovery.recovery();
        TestUtil.assertRecordEquals(clone, TestUtil.getRecordAr("tb", new TransactionId(0))[0], true);
    }

    /**
     * 事务 A 插入 x，事务 B 插入 y 并提交 ，检查点发生，事务 A 回滚，事务 C 修改元组 y 提交，
     * 然后发生宕机，此时需要回滚 A 并重做 C
     */
    @Test
    public void condition4() throws Exception {
        TransactionId tid1 = Database.getTransactionManager().begin();
        TransactionId tid2 = Database.getTransactionManager().begin();
        TransactionId tid3 = Database.getTransactionManager().begin();

        Record x = TestUtil.insertAndRunLog(49, pm.malloc(), tid1);
        Record y = TestUtil.insertAndRunLog(81, pm.malloc(), tid2);
        Database.getTransactionManager().commit(tid2, false);

        Database.getCheckPoint().sharkCheckPoint();

        Database.getTransactionManager().rollback(tid1);

        Record update = y.clone();
        update.setField(1, new DoubleField(250));
        TestUtil.updateAndRunLog(y, update, tid3);
        Database.getTransactionManager().commit(tid3, false);

        TestUtil.assertRecordEquals(update, TestUtil.getRecordAr("tb", new TransactionId(0))[0], true);
        Database.reset();

        Assert.assertEquals(2, TestUtil.getRecordAr("tb", new TransactionId(0)).length);
        TestUtil.assertRecordEquals(y, TestUtil.getRecordAr("tb", new TransactionId(0))[1], true);

        Recovery.recovery();
        Assert.assertEquals(1, TestUtil.getRecordAr("tb", new TransactionId(0)).length);
        TestUtil.assertRecordEquals(update, TestUtil.getRecordAr("tb", new TransactionId(0))[0], true);
    }

    /**
     * 多线程并发插入，然后宕机，需要重做所有事务
     * @throws Exception
     */
    @Test
    public void insertRedoByManyThread() throws Exception {
        BufferPool.DEFAULT_PAGES = 250;
        Database.reset();
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        int n = 20;
        for (int i = 0; i < n; i++) {
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    TransactionId tid = Database.getTransactionManager().begin();
                    TestUtil.insertAndRunLog((int) tid.getXid(), pm.malloc(), tid);
                    Database.getTransactionManager().commit(tid, false);
                    setDone(true);
                }
            });
        }
        TestUtil.runManyThread(tasks, 1000 * 60);
        Assert.assertEquals(n, TestUtil.getRecordAr("tb", new TransactionId(2048)).length);
        Database.reset();
        try {
            TestUtil.getRecordAr("tb", new TransactionId(0));
            fail();
        } catch (Exception ignore) {
            // ignore
        }

        Recovery.recovery();
        Assert.assertEquals(n, TestUtil.getRecordAr("tb", new TransactionId(0)).length);
    }

    @Test
    public void testFuzzleCkp() throws Exception {
        BufferPool.DEFAULT_PAGES = 250;
        Database.reset();
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        int n = 20;
        for (int i = 0; i < n; i++) {
            int finalI = i;
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    TransactionId tid = Database.getTransactionManager().begin();
                    TestUtil.insertAndRunLog((int) tid.getXid(), pm.malloc(), tid);
                    Database.getTransactionManager().commit(tid, false);
                    if (finalI % 100 == 1) {
                        Database.getCheckPoint().fuzzleCheckPoint();
                    }
                    setDone(true);
                }
            });
        }
        TestUtil.runManyThread(tasks, 1000 * 60);
        Database.getCheckPoint().fuzzleCheckPoint();
        Assert.assertTrue(Database.getLogBuffer().getCurrentLsn() != 0);
        Database.reset();

        Recovery.recovery();
        Assert.assertTrue(n / 2 <= TestUtil.getRecordAr("tb", new TransactionId(0)).length);
    }
}
