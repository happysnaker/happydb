package happydb.optimizer;

import happydb.TestBase;
import happydb.common.Debug;
import happydb.common.Pair;
import happydb.execution.Predicate;
import happydb.execution.Predicate.Op;
import happydb.storage.Field;
import happydb.storage.IntField;
import org.junit.Assert;
import org.junit.Test;

/**
 * @Author happysnaker
 * @Date 2022/11/26
 * @Email happysnaker@foxmail.com
 */
public class IntHistogramTest extends TestBase {


    @Test
    public void orderOfGrowthTest() {
        // 不要为这个测试超时而烦恼。与某些低效算法相比，打印调试语句需要 >> 时间。
        IntHistogram h = new IntHistogram(10000, 0, 100);

        // 为直方图提供比我们的 128mb 分配堆（4 字节整数）更多的整数如果失败，有人存储每个值......
        for (int c = 0; c < 33554432; c++) {
            h.addValue(new IntField((c * 23) % 101));
        }

        // 尝试打印出所有值；确保“estimateSelectivity()”导致任何问题
        double selectivity = 0.0;
        for (int c = 0; c < 101; c++) {
            selectivity += h.estimateSelectivity(Predicate.Op.EQUALS, new IntField(c));
        }
        Debug.log(selectivity);
        Assert.assertTrue(selectivity > 0.99);
    }


    @Test
    public void negativeRangeTest() {
        IntHistogram h = new IntHistogram(10, -60, -10);

        for (int c = -60; c <= -10; c++) {
            h.addValue(new IntField(c));
            h.estimateSelectivity(Predicate.Op.EQUALS, new IntField(c));
        }

        Assert.assertTrue(h.estimateSelectivity(Op.EQUALS, new IntField(-33)) < 0.3);
        Assert.assertTrue(h.estimateSelectivity(Op.EQUALS, new IntField(-33)) > 0.001);
    }


    @Test
    public void opEqualsTest() {
        IntHistogram h = new IntHistogram(10, 1, 10);

        // Set some values
        h.addValue(new IntField(3));
        h.addValue(new IntField(3));
        h.addValue(new IntField(3));

        // This really should return "1.0"; but,
        // be conservative in case of alternate implementations
        Assert.assertTrue(h.estimateSelectivity(Op.EQUALS, new IntField(3)) > 0.8);
        Assert.assertTrue(h.estimateSelectivity(Op.EQUALS, new IntField(8)) < 0.001);
    }

    @Test
    public void opGreaterThanTest() {
        IntHistogram h = new IntHistogram(10, 1, 10);

        // Set some values
        h.addValue(new IntField(3));
        h.addValue(new IntField(3));
        h.addValue(new IntField(3));
        h.addValue(new IntField(1));
        h.addValue(new IntField(10));

        // Be conservative in case of alternate implementations
        Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, new IntField(-1)) > 0.999);
        Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, new IntField(2)) > 0.6);
        Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, new IntField(4)) < 0.4);
        Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, new IntField(12)) < 0.001);
    }

    /**
     * Make sure that LESS_THAN binning does something reasonable.
     */
    @Test
    public void opLessThanTest() {
        IntHistogram h = new IntHistogram(10, 1, 10);

        h.addValue(new IntField(3));
        h.addValue(new IntField(3));
        h.addValue(new IntField(3));
        h.addValue(new IntField(1));
        h.addValue(new IntField(10));

        // Be conservative in case of alternate implementations
        Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN, new IntField(-1)) < 0.001);
        Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN, new IntField(2)) < 0.4);
        Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN, new IntField(4)) > 0.6);
        Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN, new IntField(12)) > 0.999);
    }

    /**
     * Make sure that GREATER_THAN_OR_EQ binning does something reasonable.
     */
    @Test
    public void opGreaterThanOrEqualsTest() {
        IntHistogram h = new IntHistogram(10, 1, 10);

        h.addValue(new IntField(3));
        h.addValue(new IntField(3));
        h.addValue(new IntField(3));
        h.addValue(new IntField(1));
        h.addValue(new IntField(10));

        // Be conservative in case of alternate implementations
        Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, new IntField(-1)) > 0.999);
        Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, new IntField(2)) > 0.6);
        Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, new IntField(3)) > 0.6);
        Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, new IntField(4)) < 0.5);
        Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, new IntField(12)) < 0.001);
    }

    @Test
    public void opLessThanOrEqualsTest() {
        IntHistogram h = new IntHistogram(10, 1, 10);

        // Set some values
        h.addValue(new IntField(3));
        h.addValue(new IntField(3));
        h.addValue(new IntField(3));
        h.addValue(new IntField(1));
        h.addValue(new IntField(10));

        Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, new IntField(-1)) < 0.001);
        Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, new IntField(2)) < 0.4);
        Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, new IntField(3)) > 0.45);
        Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, new IntField(4)) > 0.6);
        Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, new IntField(12)) > 0.999);
    }

    /**
     * 这个测试将打印直方图对
     */
    @Test
    public void testGetHistogramPair() {
        IntHistogram h = new IntHistogram(10, -10, 10);

        // Set some values
        h.addValue(new IntField(1));
        h.addValue(new IntField(-2));
        h.addValue(new IntField(-7));
        h.addValue(new IntField(-8));
        h.addValue(new IntField(3));
        h.addValue(new IntField(3));
        h.addValue(new IntField(3));
        h.addValue(new IntField(10));

        int sum = 0;
        for (Pair<Field, Integer> p : h.getHistogramPair()) {
            Debug.log(p);
            sum += p.val;
        }
        Assert.assertEquals(8, sum);
    }


    @Test
    public void opNotEqualsTest() {
        IntHistogram h = new IntHistogram(10, 1, 10);

        // Set some values
        h.addValue(new IntField(3));
        h.addValue(new IntField(3));
        h.addValue(new IntField(3));

        // Be conservative in case of alternate implementations
        Assert.assertTrue(h.estimateSelectivity(Op.NOT_EQUALS, new IntField(3)) < 0.001);
        Assert.assertTrue(h.estimateSelectivity(Op.NOT_EQUALS, new IntField(8)) > 0.01);
    }
}
