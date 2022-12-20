package happydb.execution;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.exception.DbException;
import happydb.storage.Record;
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
public class OrderByTest extends TestBase {

    OpIterator scan;

    int cols = 2;
    int rows = 1000;

    int[] numsAsc;
    int[] numsDesc;

    @Before
    public void setUp() throws Exception {
        TestUtil.createAndInsert(cols, rows, 0, 1, "tb", null);
        this.scan = new BTreeSeqScan(new TransactionId(0), "tb", null, null);
        this.numsAsc = new int[rows];
        this.numsDesc = new int[rows];
        for (int i = 0; i < rows; i++) {
            numsAsc[i] = i;
            numsDesc[i] = rows - i - 1;
        }
    }

    @Test
    public void testDesc() throws DbException {
        OrderBy order = new OrderBy(0, false, scan);
        order.open();
        int[] arr = Arrays.stream(order.getRecordAr()).mapToInt(r -> (int) r.getField(0).getObject()).toArray();
        Assert.assertArrayEquals(numsDesc, arr);
    }

    @Test
    public void testAsc() throws DbException {
        OrderBy orderDesc = new OrderBy(0, false, scan);
        OrderBy orderAsc = new OrderBy(0, true, orderDesc);
        orderAsc.open();
        Record[] recordAr = orderAsc.getRecordAr();
        int[] arr = Arrays.stream(recordAr).mapToInt(r -> (int) r.getField(0).getObject()).toArray();
        Assert.assertArrayEquals(numsAsc, arr);
    }
}
