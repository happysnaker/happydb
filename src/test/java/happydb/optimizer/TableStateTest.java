package happydb.optimizer;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Debug;
import happydb.exception.DbException;
import happydb.execution.Predicate;
import happydb.storage.DoubleField;
import happydb.storage.IntField;
import happydb.storage.Record;
import happydb.storage.StringField;
import happydb.storage.TableDesc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author happysnaker
 * @Date 2022/11/26
 * @Email happysnaker@foxmail.com
 */
public class TableStateTest extends TestBase {

    int rows = 1000;

    TableDesc td;

    @Before
    public void setUp() throws Exception {
        td = TestUtil.createSimpleAndInsert(rows, "tb", r -> {
            int i = (int) r.getField(0).getObject();
            // 0 ~ 499 成对
            r.setField(0, new IntField(i / 2));
            // 0、2.5、5.0....22.5 每组 100 个
            r.setField(1, new DoubleField((i % 10) * 2.5));
            // 0~999 各一次
            r.setField(2, new StringField(String.valueOf(i)));
            return r;
        });
        Assert.assertNotNull(td);
    }




    @Test
    public void testEstimateSelectivity() throws DbException {
        TableState ts = TableStateView.getInstance().getTableState("tb");
        Assert.assertNotNull(ts);

        double v = ts.estimateSelectivity(0, Predicate.Op.LESS_THAN_OR_EQ, new IntField(499));
        Debug.log("Selectivity 最优值为 1.0，实际值：%f", v);
        Assert.assertTrue(v > 0.9);

        v = ts.estimateSelectivity(0, Predicate.Op.NOT_EQUALS, new IntField(0));
        Debug.log("Selectivity 最优值为 0.998，实际值：%f", v);
        Assert.assertTrue(v > 0.9);

        v = ts.estimateSelectivity(0, Predicate.Op.LESS_THAN, new IntField(400));
        Debug.log("Selectivity 最优值为 0.8，实际值：%f", v);
        Assert.assertTrue(v > 0.7 && v < 0.9);

        v = ts.estimateSelectivity(1, Predicate.Op.LESS_THAN, new DoubleField(5.99));
        Debug.log("Selectivity 最优值为 0.3，实际值：%f", v);
        Assert.assertTrue(v > 0.18 && v < 0.43);

        v = ts.estimateSelectivity(2, Predicate.Op.LESS_THAN, new StringField("900"));
        Debug.log("Selectivity 最优值为 0.9，实际值：%f", v);
        Assert.assertTrue(v > 0.3); // 我们预估 String 可能非常不准确
    }

    @Test
    public void testUpdateRecord() throws DbException {
        TableStateView.MODIFY_THRESHOLD = 100;
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            int finalI = i;
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    Record record = new Record(td);
                    record.setField(0, new IntField(finalI / 2));
                    record.setField(1, new DoubleField((finalI % 10) * 2.5));
                    record.setField(2, new StringField(String.valueOf(finalI)));

                    TableStateView.getInstance().deleteRecord("tb", record);
                    record.setField(0, new IntField(finalI % 500));
                    TableStateView.getInstance().insertRecord("tb", record);

                    setDone(true);
                }
            });
        }

        TestUtil.runManyThread(tasks, 10000);
        TableState ts = TableStateView.getInstance().getTableState("tb");
        Assert.assertNotNull(ts);

        double v = ts.estimateSelectivity(0, Predicate.Op.LESS_THAN_OR_EQ, new IntField(100));
        Debug.log("Selectivity 最优值接近 0.2，实际值：%f", v);
        Assert.assertTrue(v < 0.35);
    }


    @Test
    public void testEstimateCard() throws DbException {
        TableState ts = TableStateView.getInstance().getTableState("tb");
        Assert.assertNotNull(ts);

        var v = ts.estimateSelectivity(0, Predicate.Op.LESS_THAN, new IntField(400));
        Debug.log("Selectivity 最优值为 0.8，实际值：%f", v);
        Assert.assertTrue(v > 0.7 && v < 0.9);

        int card = ts.estimateTableCardinality(v);
        Debug.log(card);
        Assert.assertTrue(Math.abs(card - 700) <= 100);
    }
}
