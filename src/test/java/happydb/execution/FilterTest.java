package happydb.execution;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.storage.IntField;
import happydb.storage.Record;
import happydb.storage.TableDesc;
import happydb.transaction.TransactionId;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

/**
 * @Author happysnaker
 * @Date 2022/11/24
 * @Email happysnaker@foxmail.com
 */
public class FilterTest extends TestBase {
    OpIterator scan;

    int cols = 2;
    int rows = 10;

    @Before
    public void setUp() throws Exception {
        TestUtil.createAndInsert(cols, rows, -5, 5, "tb", null);
        this.scan = new BTreeSeqScan(new TransactionId(0), "tb", null, null);
    }


    @Test
    public void getTableDesc() {
        Predicate pred = new Predicate(0, Predicate.Op.EQUALS, new IntField(0));
        Filter op = new Filter(pred, scan);
        TableDesc actual = op.getTableDesc();
        TestUtil.assertTableDescEquals(scan.getTableDesc(), actual, true, true, true);
    }


    @Test
    public void rewind() throws Exception {
        Predicate pred = new Predicate(0, Predicate.Op.EQUALS, new IntField(0));
        Filter op = new Filter(pred, scan);
        op.open();
        assertTrue(op.hasNext());
        assertNotNull(op.next());
        assertTrue(TestUtil.checkExhausted(op));

        op.rewind();
        Record expected = TestUtil.createRecord(cols, 0, "tb");
        Record actual = op.next();
        TestUtil.assertRecordEquals(expected, actual, true);
        op.close();
    }


    @Test
    public void filterSomeLessThan() throws Exception {
        Predicate pred;
        pred = new Predicate(0, Predicate.Op.LESS_THAN, new IntField(2));
        Filter op = new Filter(pred, scan);
        op.open();
        Record[] recordAr = op.getRecordAr();
        assertEquals(2, recordAr.length);
        TestUtil.assertRecordEquals(TestUtil.createRecord(cols, -5, "tb"), recordAr[0], true);
        TestUtil.assertRecordEquals(TestUtil.createRecord(cols, 0, "tb"), recordAr[1], true);
    }


    @Test
    public void filterAllLessThan() throws Exception {
        Predicate pred;
        pred = new Predicate(0, Predicate.Op.LESS_THAN, new IntField(-5));
        Filter op = new Filter(pred, scan);
        op.open();
        assertTrue(TestUtil.checkExhausted(op));
        op.close();
    }


    @Test
    public void filterEqualNoRecords() throws Exception {
        Predicate pred;
        pred = new Predicate(0, Predicate.Op.EQUALS, new IntField(1));
        Filter op = new Filter(pred, scan);
        op.open();
        TestUtil.checkExhausted(op);
        op.close();
    }
}
