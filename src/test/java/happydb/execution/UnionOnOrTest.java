package happydb.execution;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.exception.DbException;
import happydb.storage.IntField;
import happydb.transaction.TransactionId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @Author happysnaker
 * @Date 2022/11/24
 * @Email happysnaker@foxmail.com
 */
public class UnionOnOrTest extends TestBase {

    OpIterator scan1;
    OpIterator scan2;

    int rows = 1000;

    @Before
    public void setUp() throws Exception {
        // -5, 0, 5, 10, 15
        TestUtil.createAndInsert(2, rows, 0, 1, "tb", null);
        this.scan1 = new BTreeSeqScan(new TransactionId(0), "tb", null,
                new Predicate(0, Predicate.Op.GREATER_THAN_OR_EQ, new IntField(rows / 2)));
        this.scan2 = new BTreeSeqScan(new TransactionId(0), "tb", null,
                new Predicate(0, Predicate.Op.LESS_THAN, new IntField(rows / 2 + 10)));
        // union 应该是 rows 而不是 rows + 10
    }

    @Test
    public void testGetTableDesc() {
        OpIterator or = new UnionOnOr(scan1, scan2);
        TestUtil.assertTableDescEquals(scan1.getTableDesc(), scan2.getTableDesc(), true, true, true);
        TestUtil.assertTableDescEquals(scan1.getTableDesc(), or.getTableDesc(), true, true, true);
    }

    @Test
    public void tesUnionOr() throws DbException {
        OpIterator or = new UnionOnOr(scan1, scan2);
        or.open();
        Assert.assertEquals(rows, or.getRecordAr().length);
    }
}
