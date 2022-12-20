package happydb.execution;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Debug;
import happydb.exception.DbException;
import happydb.storage.Record;
import happydb.storage.TableDesc;
import happydb.transaction.TransactionId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * @Author happysnaker
 * @Date 2022/11/24
 * @Email happysnaker@foxmail.com
 */
public class JoinTest extends TestBase {
    OpIterator scan1;
    OpIterator scan2;

    int cols = 2;

    @Before
    public void setUp() throws Exception {
        // -5, 0, 5, 10, 15
        TestUtil.createAndInsert(cols, 5, -5, 5, "tb1", null);
        this.scan1 = new BTreeSeqScan(new TransactionId(0), "tb1", null, null);

        // 0, 3, 6, 9, 12, 15
        TestUtil.createAndInsert(cols, 6, 0, 3, "tb2", null);
        this.scan2 = new BTreeSeqScan(new TransactionId(0), "tb2", null, null);
    }

    @Test
    public void testGetTableDesc() {
        JoinPredicate jp = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
        NormalJoin join = new NormalJoin(jp, scan1, scan2);

        TableDesc tableDesc = join.getTableDesc();
        Debug.log(tableDesc.toString());
        TestUtil.assertTableDescEquals(TableDesc.merge(scan1.getTableDesc(),
                scan2.getTableDesc()), tableDesc, true, true, true);


        HashEqualJoin join1 = new HashEqualJoin(jp, scan1, scan2);

        TableDesc tableDesc1 = join1.getTableDesc();
        Debug.log(tableDesc1.toString());
        TestUtil.assertTableDescEquals(TableDesc.merge(scan1.getTableDesc(),
                scan2.getTableDesc()), tableDesc1, true, true, true);
    }



    @Test
    public void testHashOnEqualJoin() throws DbException {
        JoinPredicate jp = new JoinPredicate(0, Predicate.Op.EQUALS, 1);
        HashEqualJoin join = new HashEqualJoin(jp, scan1, scan2);

        join.open();
        Record[] recordAr = join.getRecordAr();
        Assert.assertEquals(2, recordAr.length);
    }


    @Test
    public void testLessThan() throws DbException {
        JoinPredicate jp = new JoinPredicate(0, Predicate.Op.LESS_THAN, 0);
        NormalJoin join = new NormalJoin(jp, scan1, scan2);

        join.open();
        Record[] recordAr = join.getRecordAr();
        Assert.assertEquals(17, recordAr.length);
    }

    @Test
    public void testGreateThan() throws DbException {
        JoinPredicate jp = new JoinPredicate(0, Predicate.Op.GREATER_THAN, 0);
        NormalJoin join = new NormalJoin(jp, scan1, scan2);

        join.open();
        Record[] recordAr = join.getRecordAr();
        Assert.assertEquals(11, recordAr.length);
    }
}
