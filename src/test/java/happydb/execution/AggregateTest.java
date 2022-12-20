package happydb.execution;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Debug;
import happydb.exception.DbException;
import happydb.storage.DoubleField;
import happydb.storage.IntField;
import happydb.storage.Record;
import happydb.storage.TableDesc;
import happydb.transaction.TransactionId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static happydb.storage.Type.*;

/**
 * @Author happysnaker
 * @Date 2022/11/24
 * @Email happysnaker@foxmail.com
 */
public class AggregateTest extends TestBase {
    int rows = 1000;
    int group = 10;
    TableDesc td;

    OpIterator scan;

    @Before
    public void setUp() throws Exception {
        // 按照主键分成 10 组 [0, 10)
        this.td = TestUtil.createSimpleAndInsert(rows, "tb", r -> {
            int pk = (int) r.getField(0).getObject();
            int key = pk % group;
            r.setField(0, new IntField(key));
            r.setField(1, new DoubleField(pk));
            return r;
        });
        this.scan = new BTreeSeqScan(new TransactionId(0), "tb", null, null);
    }

    @Test
    public void testGetTableDesc() {
        AggregatorNode node1 = new AggregatorNode(0, AggregatorNode.Op.SUM);
        AggregatorNode node2 = new AggregatorNode(0, AggregatorNode.Op.AVG);
        AggregatorNode node3 = new AggregatorNode(1, AggregatorNode.Op.MAX);
        AggregatorNode node4 = new AggregatorNode(1, AggregatorNode.Op.MIN);
        AggregatorNode node5 = new AggregatorNode(1, AggregatorNode.Op.COUNT);

        Aggregate it = new Aggregate(scan, Aggregator.NO_GROUPING, List.of(node1, node2, node3, node4, node5));

        TableDesc td = it.getTableDesc();
        Assert.assertEquals(INT_TYPE, td.getFieldType(0));
        Assert.assertEquals(DOUBLE_TYPE, td.getFieldType(1));
        Assert.assertEquals(DOUBLE_TYPE, td.getFieldType(2));
        Assert.assertEquals(DOUBLE_TYPE, td.getFieldType(3));
        Assert.assertEquals(INT_TYPE, td.getFieldType(4));

        it = new Aggregate(scan, 2, List.of(node1, node2, node3, node4, node5));
        Assert.assertEquals(STRING_TYPE, it.getTableDesc().getFieldType(0));
    }

    @Test
    public void testAggregate() throws DbException {
        AggregatorNode node1 = new AggregatorNode(0, AggregatorNode.Op.SUM);
        AggregatorNode node2 = new AggregatorNode(0, AggregatorNode.Op.AVG);
        AggregatorNode node3 = new AggregatorNode(1, AggregatorNode.Op.MAX);
        AggregatorNode node4 = new AggregatorNode(1, AggregatorNode.Op.MIN);
        AggregatorNode node5 = new AggregatorNode(1, AggregatorNode.Op.COUNT);

        Aggregate it = new Aggregate(scan, Aggregator.NO_GROUPING, List.of(node1, node2, node3, node4, node5));

        it.open();

        Record record = it.fetchNext();
        TestUtil.checkExhausted(it);
        Assert.assertEquals(4500, record.getField(0).getObject());
        Assert.assertEquals(4500 / 1000.0, record.getField(1).getObject());
        Assert.assertEquals(999.0, (Double) record.getField(2).getObject(), 0f);
        Assert.assertEquals(0, (Double) record.getField(3).getObject(), 0f);
        Assert.assertEquals(1000,  record.getField(4).getObject());
    }


    @Test
    public void testAggregateByGroup() throws DbException {
        AggregatorNode node1 = new AggregatorNode(0, AggregatorNode.Op.SUM);
        AggregatorNode node2 = new AggregatorNode(0, AggregatorNode.Op.AVG);
        AggregatorNode node3 = new AggregatorNode(1, AggregatorNode.Op.MAX);
        AggregatorNode node4 = new AggregatorNode(1, AggregatorNode.Op.MIN);
        AggregatorNode node5 = new AggregatorNode(1, AggregatorNode.Op.COUNT);

        Aggregate it = new Aggregate(scan, 0, List.of(node1, node2, node3, node4, node5));

        it.open();
        Record[] recordAr = it.getRecordAr();
        for (int i = 0; i < recordAr.length; i++) {
            var record = recordAr[i];
            Debug.log(record.toString());

            Assert.assertEquals(i, record.getField(0).getObject());
            Assert.assertEquals(i * 100, record.getField(1).getObject());
            Assert.assertEquals((double) i, (Double) record.getField(2).getObject(), 0f);
            Assert.assertEquals(990.0 + i, (Double) record.getField(3).getObject(), 0f);
            Assert.assertEquals((double) i, (Double) record.getField(4).getObject(), 0f);
            Assert.assertEquals(100,  record.getField(5).getObject());
        }
    }
}
