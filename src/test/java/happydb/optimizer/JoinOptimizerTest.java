package happydb.optimizer;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.common.Debug;
import happydb.execution.Predicate;
import happydb.log.CheckPoint;
import happydb.storage.BufferPool;
import happydb.storage.IntField;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Author happysnaker
 * @Date 2022/11/27
 * @Email happysnaker@foxmail.com
 */
public class JoinOptimizerTest extends TestBase {

    @Test
    public void testEstimateCardinality() throws Exception {
        BufferPool.DEFAULT_PAGES = 1000;
        BufferPool.resetPageSize();
        Database.reset();

        // a 表八千条数据，从 0 到 7999
        var a = TestUtil.createSimpleAndInsert(8000, "a", null);

        // b 表 100 条数据，从 0 到 99
        var b = TestUtil.createSimpleAndInsert(100, "b", null);

        // c 表 2000 条数据，从 0 到 99 各 20 行
        var c = TestUtil.createSimpleAndInsert(2000, "c", r -> {
            r.setField(0, new IntField(((int) r.getField(0).getObject()) % 100));
            return r;
        });

        // d 表 5000 条数据，从 0 到 1 各 2500 行
        var d = TestUtil.createSimpleAndInsert(5000, "d", r -> {
            r.setField(0, new IntField(((int) r.getField(0).getObject()) % 2));
            return r;
        });

        LogicalJoinNode node = new LogicalJoinNode(
                "a", "b", "a.x", "b.x", Predicate.Op.GREATER_THAN_OR_EQ);
        LogicalPlan lp = new LogicalPlan();
        lp.setTableMap(Map.of("a", "a", "b", "b", "c", "c", "d", "d"));
        TableStateView stateView = TableStateView.getInstance();
        int card = new JoinOptimizer(lp, List.of(node)).estimateJoinCardinality(
                node, 8000, 100, true, true, stateView.getTableStateMap(Set.of("a", "b"))
        );
        Debug.log("实际基数为 795050，预估基数为 %d", card);
        Assert.assertTrue(Math.abs(card - 795050) <= 100000);

        node = new LogicalJoinNode(
                "c", "d", "x", "x", Predicate.Op.EQUALS);
        card = new JoinOptimizer(lp, List.of(node)).estimateJoinCardinality(
                node, 2000, 5000, false, false, stateView.getTableStateMap(Set.of("c", "d"))
        );
        Debug.log("实际基数为 100000，预估基数为 %d", card);
        // 他可能会选择两者之间的最大值
        Assert.assertTrue(Math.abs(card - 100000) <= 100000 - 5000);
    }

    @Test
    public void testOrderJoin() throws Exception {
        BufferPool.DEFAULT_PAGES = 1000;
        BufferPool.resetPageSize();
        CheckPoint.RATE = Integer.MAX_VALUE;
        Database.reset();

        // a 表八千条数据，从 1000 开始
        var a = TestUtil.createSimpleAndInsert(8000, "a", r -> {
            r.setField(0, new IntField(((int) r.getField(0).getObject()) + 1000));
            return r;
        });
        // b 表 100 条数据
        var b = TestUtil.createSimpleAndInsert(100, "b", null);
        // c 表 2000 条数据
        var c = TestUtil.createSimpleAndInsert(2000, "c", null);
        // d 表 5000 条数据，从 8000 算起，不重复
        var d = TestUtil.createSimpleAndInsert(5000, "d", r -> {
            r.setField(0, new IntField(((int) r.getField(0).getObject()) + 8000));
            return r;
        });


        // a.0 >= b.0 and a.0 = c.0 and b.0 < d.0 and c.0 = d.0
        LogicalJoinNode node1 = new LogicalJoinNode("a", "b", "x", "x", Predicate.Op.GREATER_THAN_OR_EQ);
        LogicalJoinNode node2 = new LogicalJoinNode("a", "c", "x", "x", Predicate.Op.EQUALS);
        LogicalJoinNode node3 = new LogicalJoinNode("b", "d", "x", "x", Predicate.Op.LESS_THAN);
        LogicalJoinNode node4 = new LogicalJoinNode("c", "d", "x", "x", Predicate.Op.EQUALS);

        LogicalPlan lp = new LogicalPlan();
        lp.setTableMap(Map.of("a", "a", "b", "b", "c", "c", "d", "d"));

        List<LogicalJoinNode> joins = List.of(node1, node2, node3, node4);

        List<LogicalJoinNode> orders = new JoinOptimizer(lp, joins).orderJoins(
                TableStateView.getInstance().getTableStateMap(Set.of("a", "b", "c", "d")),
                Map.of("a", 1.0, "b", 1.0, "c", 1.0, "d", 1.0)
        );
        Debug.log("最优值并不确定，他取决于具体使用的算法");
        Debug.log("从基数看，最优值为 c = d、d > b、b <= a、c = a，预估值为：");
        for (LogicalJoinNode order : orders) {
            System.out.println("order = " + order);
        }
        // 无论无何，下列这种情况都是最糟的选择
        Assert.assertNotEquals(node2, orders.get(0));
        Assert.assertNotEquals(node4, orders.get(3));
    }
}
