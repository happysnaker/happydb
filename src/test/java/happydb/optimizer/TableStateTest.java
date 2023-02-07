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

    int rows = 100;

    TableDesc td;

    @Before
    public void setUp() throws Exception {
        td = TestUtil.createSimpleAndInsert(rows, "tb", r -> {
            int i = (int) r.getField(0).getObject();
            // 0 ~ rows/2 成对
            r.setField(0, new IntField(i / 2));
            // 0、2.5、5.0....22.5 每组 rows / 10 个
            r.setField(1, new DoubleField((i % 10) * 2.5));
            // rows 各一次
            r.setField(2, new StringField(String.valueOf(i)));
            return r;
        });
        Assert.assertNotNull(td);
    }




    @Test
    public void testEstimateSelectivity() throws DbException {
        TableState ts = TableStateView.getInstance().getTableState("tb");
        Assert.assertNotNull(ts);

        double v = ts.estimateSelectivity(0, Predicate.Op.LESS_THAN_OR_EQ, new IntField(rows));
        Debug.log("Selectivity 最优值为 1.0，实际值：%f", v);
        Assert.assertTrue(v > 0.9);

        v = ts.estimateSelectivity(0, Predicate.Op.NOT_EQUALS, new IntField(0));
        Debug.log("Selectivity 最优值为 %f，实际值：%f",(rows - 1.0) / rows * 1.0f ,v);
        Assert.assertTrue(v >= 0.8);

        v = ts.estimateSelectivity(0, Predicate.Op.LESS_THAN, new IntField((int) (rows * 0.4)));
        Debug.log("Selectivity 最优值为 0.8，实际值：%f", v);
        Assert.assertTrue(v > 0.7 && v < 0.9);

        v = ts.estimateSelectivity(1, Predicate.Op.LESS_THAN, new DoubleField(5.99));
        Debug.log("Selectivity 最优值为 0.3，实际值：%f", v);
        Assert.assertTrue(v > 0.18 && v < 0.43);
    }



    @Test
    public void testEstimateCard() throws DbException {
        TableState ts = TableStateView.getInstance().getTableState("tb");
        Assert.assertNotNull(ts);

        var v = ts.estimateSelectivity(0, Predicate.Op.LESS_THAN, new IntField((int) (rows * 0.4)));
        Debug.log("Selectivity 最优值为 0.8，实际值：%f", v);
        Assert.assertTrue(v > 0.7 && v < 0.9);

        int card = ts.estimateTableCardinality(v);
        Debug.log(card);
        Assert.assertTrue(Math.abs(card - rows * 0.8) <= rows * 0.2);
    }
}
