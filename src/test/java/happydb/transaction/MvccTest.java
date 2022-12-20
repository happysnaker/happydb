package happydb.transaction;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.index.Index;
import happydb.index.IndexType;
import happydb.log.LogBuffer;
import happydb.storage.DoubleField;
import happydb.storage.HeapPageManager;
import happydb.storage.Record;
import happydb.storage.TableDesc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 一些简单的 Mvcc 情景测试
 * @Author happysnaker
 * @Date 2022/12/3
 * @Email happysnaker@foxmail.com
 */
public class MvccTest extends TestBase {

    TableDesc td;

    HeapPageManager pm;

    Index index;

    LogBuffer logBuffer;

    TransactionManager tm;

    @Before
    public void setUp() throws Exception {
        td = TestUtil.createSimpleAndInsert(0, "tb", null);
        Database.getBufferPool().transactionReleaseLock(new TransactionId(0));

        pm = (HeapPageManager) Database.getCatalog().getPageManager("tb");
        index = Database.getCatalog().getIndex("tb", 0, IndexType.BTREE);
        logBuffer = Database.getLogBuffer();
        tm = Database.getTransactionManager();
    }


    /**
     * 可重复读级别下，事务 A 插入元组，未提交状态，事务 A 可见此元组，但事务 B 应该不可见
     */
    @Test
    public void condition1() throws Exception {
        Database.ISOLATION_LEVEL = ReadView.READ_REPEAT;
        TransactionId A = tm.begin();
        TransactionId B = tm.begin();

        TestUtil.insertAndRunLog(0, pm.malloc(), A);

        Assert.assertEquals(1, TestUtil.getRecordAr("tb", A).length);
        Assert.assertEquals(0, TestUtil.getRecordAr("tb", B).length);
    }

    /**
     * 事务 A 先于事务 B 开启，事务 B 第一次查询没有元组，随后事务 A 插入元组，
     * 事务 A 提交。事务 B 继续查询，可重复读级别下，事务 B 应该不可见，但读已提交级别下，可见，产生不可重复读问题
     */
    @Test
    public void condition2() throws Exception {
        Database.ISOLATION_LEVEL = ReadView.READ_REPEAT;
        TransactionId A = tm.begin();
        TransactionId B = tm.begin();


        Assert.assertEquals(0, TestUtil.getRecordAr("tb", B).length);

        TestUtil.insertAndRunLog(0, pm.malloc(), A);
        tm.commit(A, false);

        Assert.assertEquals(0, TestUtil.getRecordAr("tb", B).length);

        Database.ISOLATION_LEVEL = ReadView.READ_COMMIT; // should create new read view
        Assert.assertEquals(1, TestUtil.getRecordAr("tb", B).length);
    }


    /**
     * 事务 A 插入元组 x，然后提交。<br> 事务 C 先于 事务 B 开启，
     * 事务 B 修改元组 x 为 y，此时事务 C 应该看见 x 而不是 y <br>
     * 事务 B 删除元组，事务 C 应该仍然看见 x<br>
     * 事务 B 提交，读已提交应该看不到元组，而可重复读应该继续看到 x
     */
    @Test
    public void condition3() throws Exception {
        Database.ISOLATION_LEVEL = ReadView.READ_REPEAT;
        TransactionId A = tm.begin();
        Record record = TestUtil.insertAndRunLog(0, pm.malloc(), A);
        tm.commit(A, false);

        TransactionId C = tm.begin();
        TransactionId B = tm.begin();

        Record update = record.clone();
        update.setField(1, new DoubleField(2.5));
        TestUtil.updateAndRunLog(record, update, B);

        TestUtil.assertRecordEquals(update, TestUtil.getRecordAr("tb", B)[0], true);
        TestUtil.assertRecordEquals(record, TestUtil.getRecordAr("tb", C)[0], true);

        Record delete = update.clone();
        delete.setValid(false);
        TestUtil.deleteAndRunLog(delete, B);

        Assert.assertEquals(0, TestUtil.getRecordAr("tb", B).length);
        TestUtil.assertRecordEquals(record, TestUtil.getRecordAr("tb", C)[0], true);

        tm.commit(B, false);

        TestUtil.assertRecordEquals(record, TestUtil.getRecordAr("tb", C)[0], true);

        Database.ISOLATION_LEVEL = ReadView.READ_COMMIT; // should create new read view
        Assert.assertEquals(0, TestUtil.getRecordAr("tb", C).length);
    }


}
