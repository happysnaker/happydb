package happydb.execution;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Debug;
import happydb.exception.DbException;
import happydb.storage.IntField;
import happydb.storage.Record;
import happydb.storage.TableDesc;
import happydb.transaction.TransactionId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @Author happysnaker
 * @Date 2022/11/24
 * @Email happysnaker@foxmail.com
 */
public class BTreeSeqScanTest extends TestBase {
    int rows = 1000;
    TableDesc td;

    @Before
    public void setUp() throws Exception {
        this.td = TestUtil.createAndInsert(2, rows, 0, 1, "tb", r -> {
            if ((int) r.getField(0).getObject() % 10 == 0) {
                r.setValid(false);
            }
            return r;
        });
    }

    @Test
    public void testGetTableDesc() {
        BTreeSeqScan scan = new BTreeSeqScan(new TransactionId(0), "tb", "t1", null);

        TableDesc tableDesc = scan.getTableDesc();
        TestUtil.assertTableDescEquals(td, tableDesc, false, true, true);
        for (int i = 0; i < tableDesc.numFields(); i++) {
            Assert.assertEquals(String.format("t1.%d", i), tableDesc.getFieldName(i));
        }
    }

    @Test
    public void testSeqScan() throws DbException {
        BTreeSeqScan scan = new BTreeSeqScan(new TransactionId(0), "tb", "t1", null);

        scan.open();
        Record[] recordAr = scan.getRecordAr();
        int p = 0, sum = 0;
        for (int i = 0; i < rows; i++) {
            if (i % 10 == 0)
                continue;

            TestUtil.assertRecordEquals(TestUtil.createRecord(2, i, "tb"), recordAr[p], true);
            Debug.log(recordAr[p].toString());
            p++; sum++;
        }
        Assert.assertEquals(sum, recordAr.length);
    }


    @Test
    public void testPredict() throws DbException {
        BTreeSeqScan scan = new BTreeSeqScan(new TransactionId(0), "tb", "t1",
                new Predicate(0, Predicate.Op.LESS_THAN, new IntField(rows / 2)));

        scan.open();
        Record[] recordAr = scan.getRecordAr();
        int p = 0, sum = 0;
        for (int i = 0; i < rows / 2; i++) {
            if (i % 10 == 0)
                continue;

            TestUtil.assertRecordEquals(TestUtil.createRecord(2, i, "tb"), recordAr[p], true);
            p++; sum++;
        }
        Assert.assertEquals(sum, recordAr.length);
    }
}
