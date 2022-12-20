package happydb.optimizer;

import happydb.common.Catalog;
import happydb.common.Database;
import happydb.common.Pair;
import happydb.exception.ParseException;
import happydb.execution.*;
import happydb.index.IndexType;
import happydb.storage.*;

import java.util.*;

/**
 * 查询优化器，决定出最优的连接顺序
 *
 * @Author happysnaker
 * @Date 2022/11/26
 * @Email happysnaker@foxmail.com
 */
public class JoinOptimizer {
    private final LogicalPlan p;
    private final List<LogicalJoinNode> joins;

    /**
     * @param p     正在优化的逻辑计划
     * @param joins 正在执行的连接列表
     */
    public JoinOptimizer(LogicalPlan p, List<LogicalJoinNode> joins) {
        this.p = p;
        this.joins = joins;
    }

    /**
     * 返回计算给定逻辑连接的最佳迭代器，给定指定的统计信息，以及提供的左右子计划。
     *
     * @param lj    正在考虑的连接
     * @param plan1 左连接节点的孩子
     * @param plan2 右连接节点的孩子
     */
    public static OpIterator instantiateJoin(LogicalJoinNode lj, OpIterator plan1, OpIterator plan2) throws ParseException {


        int field1, field2;
        OpIterator j;

        try {
            field1 = plan1.getTableDesc().fieldNameToIndex(lj.getF1QuantifiedName());
        } catch (NoSuchElementException e) {
            throw new ParseException("Unknown field " + lj.getF1QuantifiedName());
        }

        try {
            field2 = plan2.getTableDesc().fieldNameToIndex(lj.getF2QuantifiedName());
        } catch (NoSuchElementException e) {
            throw new ParseException("Unknown field " + lj.getF2QuantifiedName());
        }


        JoinPredicate p = new JoinPredicate(field1, lj.getOp(), field2);

        if (lj.getOp() == Predicate.Op.EQUALS) {
            j = new HashEqualJoin(p, plan1, plan2);
        } else {
            j = new NormalJoin(p, plan1, plan2);
        }
        return j;

    }

    /**
     * 估计连接的成本。
     *
     * @param j     表示正在执行的连接操作的 LogicalJoinNode。
     * @param card1 查询左侧的估计基数
     * @param card2 查询右侧的估计基数
     * @param cost1 查询左侧表的一次完整扫描的估计成本
     * @param cost2 查询右侧表的一次完整扫描的估计成本
     * @return 根据 cost1 和 cost2 估计此查询的成本
     */
    public double estimateJoinCost(LogicalJoinNode j, int card1, int card2,
                                   double cost1, double cost2) {
        // 如果是等值的话，使用哈希连接法: 两表独立加载一次，每次都能在 o1 时间匹配，但是我们预先加载了表1到哈希表中，这需要额外的常数，假设为 1.5
        // 循环嵌套法外表需要扫描一次，内表需要扫描 card1 次，时间复杂度为 card1 * card2
        return (j != null && j.getOp() == Predicate.Op.EQUALS) ?
                cost1 + cost2 + card1 * 1.5 + card2 : cost1 + card1 * cost2 + card1 * card2;

    }

    /**
     * 估计连接的基数。联接的基数是联接生成的元组数。
     *
     * @param j        表示正在执行的连接操作的 LogicalJoinNode。
     * @param card1    联接中左侧表的基数
     * @param card2    连接中右侧表的基数
     * @param t1Unique 左边的表连接列是唯一吗？
     * @param t2Unique 右边的表连接列是唯一吗？
     * @param stateMap 由真实表名映射
     * @return 连接的基数
     */
    public int estimateJoinCardinality(LogicalJoinNode j, int card1, int card2,
                                       boolean t1Unique, boolean t2Unique, Map<String, TableState> stateMap) {
        Map<String, String> tableMap = p.getTableMap();
        String t1 = tableMap.get(j.getT1Alias());
        String t2 = tableMap.get(j.getT2Alias());
        Catalog catalog = Database.getCatalog();
        return estimateTableJoinCardinality(j.getOp(),
                card1, card2,
                t1, t2,
                catalog.getTableDesc(t1).fieldNameToIndex(j.getF1PureName()),
                catalog.getTableDesc(t2).fieldNameToIndex(j.getF2PureName()),
                t1Unique, t2Unique, stateMap);
    }

    /**
     * 根据直方对预估范围查询下的连接基数
     *
     * @param op         范围查询谓词，left op right
     * @param left       左表字段直方对
     * @param right      右表状态
     * @param rightField 右表字段下标
     * @return 预估的基数
     */
    public static int estimateTableJoinCardinality(Predicate.Op op,
                                                   List<Pair<Field, Integer>> left,
                                                   TableState right, int rightField) {
        int ans = 0;
        for (Pair<Field, Integer> pair : left) {
            ans += pair.getVal() * right.estimateTableCardinality(
                    right.estimateSelectivity(rightField, op.getReverseOp(), pair.getKey())
            );
        }
        return ans;
    }

    /**
     * 估计两个表的连接基数。
     */
    public static int estimateTableJoinCardinality(Predicate.Op joinOp,
                                                   int card1, int card2,
                                                   String tableName1, String tableName2,
                                                   int field1, int field2,
                                                   boolean t1Unique,
                                                   boolean t2Unique,
                                                   Map<String, TableState> stateMap) {
        return switch (joinOp) {
            // 等值情况估计连接表的大小
            case EQUALS, LIKE -> {
                if (t1Unique && t2Unique) {
                    yield Math.min(card1, card2);
                } else if (!t1Unique && !t2Unique) {
                    yield Math.min(
                            estimateTableJoinCardinality(joinOp,
                                    stateMap.get(tableName1).getHistogramPair(field1), stateMap.get(tableName2), field2),
                            estimateTableJoinCardinality(joinOp.getReverseOp(),
                                    stateMap.get(tableName2).getHistogramPair(field2), stateMap.get(tableName1), field1)
                    );
                } else {
                    yield t1Unique ? card2 : card1;
                }
            }
            case NOT_EQUALS -> {
                yield card1 * card2 - estimateTableJoinCardinality(Predicate.Op.EQUALS, card1, card2,
                        tableName1, tableName2, field1, field2, t1Unique, t2Unique, stateMap);
            }
            // 范围查询
            default -> {
                // 基于直方图对的匹配，前提是记录均匀分布，否则可能会有误差
                yield Math.max(
                        estimateTableJoinCardinality(joinOp,
                                stateMap.get(tableName1).getHistogramPair(field1), stateMap.get(tableName2), field2),
                        estimateTableJoinCardinality(joinOp.getReverseOp(),
                                stateMap.get(tableName2).getHistogramPair(field2), stateMap.get(tableName1), field1)
                );
            }
        };
    }

    /**
     * 枚举指定向量的给定大小的所有子集的辅助方法。
     *
     * @param v    需要其子集的向量
     * @param size 感兴趣子集的大小
     * @return 指定大小的所有子集的集合
     */
    public <T> Set<Set<T>> enumerateSubsets(List<T> v, int size) {
        Set<Set<T>> ans = new HashSet<>();
        dfs(v, size, 0, ans, new HashSet<>());
        return ans;
    }

    private <T> void dfs(List<T> v, int size, int cur, Set<Set<T>> ans, Set<T> set) {
        if (set.size() == size) {
            ans.add(new HashSet<>(set));
            return;
        }
        // 当前大小 + 剩余大小 < 所需大小，直接返回
        if (set.size() + (v.size() - cur) < size) {
            return;
        }
        // 可以选 cur，也可以不选 cur
        // 不选
        dfs(v, size, cur + 1, ans, set);
        // 选
        set.add(v.get(cur));
        dfs(v, size, cur + 1, ans, set);
        set.remove(v.get(cur));
    }

    /**
     * 在指定的表上计算逻辑上合理有效的连接。有关应如何实施的提示，请参阅 PS4。
     *
     * @param stats             连接中涉及的每个表的统计信息，由基表名引用，而不是别名
     * @param filterSelectivity 连接中每个表的过滤谓词的选择性，由表别名引用（如果没有别名，则为基表名称）
     * @return 一个 List<LogicalJoinNode>，它以执行它们的左深顺序存储连接，它不应该为 null(可以为空)
     * @throws ParseException 当统计或过滤器选择性在连接中缺少表时，或者发生另一个内部错误时
     */
    public List<LogicalJoinNode> orderJoins(
            Map<String, TableState> stats,
            Map<String, Double> filterSelectivity)
            throws ParseException {
        if (this.joins == null || this.joins.isEmpty())
            return new ArrayList<>();

//        if (stats != null)
//            return joins;


        CostCard ret = null;
        Map<Set<LogicalJoinNode>, CostCard> dp = new HashMap<>();
        for (int i = 1; i <= this.joins.size(); i++) {
            Set<Set<LogicalJoinNode>> subsets = enumerateSubsets(joins, i);

            // 枚举每一个大小为 i 的集合
            for (Set<LogicalJoinNode> set : subsets) {

                // 总是从 i - 1 转移过来的，尝试删除任意一个，从剩余的 i - 1 转喻过来
                for (LogicalJoinNode logicalJoinNode : set) {
                    CostCard betterCostCard = computeCostAndCardOfSubPlan(stats, filterSelectivity,
                            logicalJoinNode, set, dp);

                    if (betterCostCard != null) {
                        dp.put(set, betterCostCard);
                    }
                }
            }

            if (i == this.joins.size()) {
                ret = dp.get(subsets.iterator().next());
            }
        }
        assert ret != null;
        return ret.getPlan();
    }


    /**
     * 这是一个辅助方法，它计算将 joinToRemove 连接到 joinSet
     * （joinSet 应该包含 joinToRemove）的成本和基数，假设所有大小为 joinSet.size() - 1 的子集都已经计算并存储在 PlanCache pc 中。
     *
     * @param stats             所有表的表统计信息，由表名而不是别名引用（参见{@link #orderJoins}）
     * @param filterSelectivity 过滤器对每个表的选择性（如果没有给出别名，表由别名或名称标识）
     * @param joinToRemove      要从 joinSet 中移除的连接
     * @param joinSet           正在考虑的连接集
     * @param dp                记忆化搜索的备忘录
     * @return A {@link CostCard} 描述成本、基数、最优子计划的对象
     * @throws ParseException 当 stats、filterSelectivity 或 pc 对象缺少连接中涉及的表时
     */
    private CostCard computeCostAndCardOfSubPlan(
            Map<String, TableState> stats,
            Map<String, Double> filterSelectivity,
            LogicalJoinNode joinToRemove, Set<LogicalJoinNode> joinSet,
            Map<Set<LogicalJoinNode>, CostCard> dp) throws ParseException {

        double bestCostSoFar = dp.getOrDefault(joinSet,
                new CostCard(Double.MAX_VALUE, 0, null)).getCost();
        LogicalJoinNode j = joinToRemove;

        List<LogicalJoinNode> prevBest;

        if (this.p.getTableName(j.getT1Alias()) == null)
            throw new ParseException("Unknown table " + j.getT1Alias());
        if (this.p.getTableName(j.getT2Alias()) == null)
            throw new ParseException("Unknown table " + j.getT2Alias());

        String table1Name = this.p.getTableName(j.getT1Alias());
        String table2Name = this.p.getTableName(j.getT2Alias());

        String table1Alias = j.getT1Alias();
        String table2Alias = j.getT2Alias();

        // 从 joinSet 中移除 joinToRemove
        Set<LogicalJoinNode> before = new HashSet<>(joinSet);
        before.remove(j);

        double t1cost, t2cost;
        int t1card, t2card;
        boolean leftPkey, rightPkey;

        if (before.isEmpty()) {
            prevBest = new ArrayList<>();
            t1cost = stats.get(table1Name).estimateScanCost();
            t1card = stats.get(table1Name).estimateTableCardinality(filterSelectivity.get(j.getT1Alias()));
            leftPkey = isUniqueKey(j.getT1Alias(), j.getF1PureName());

            t2cost = table2Alias == null ? 0 : stats.get(table2Name).estimateScanCost();
            t2card = table2Alias == null ? 0 : stats.get(table2Name)
                    .estimateTableCardinality(filterSelectivity.get(j.getT2Alias()));
            rightPkey = table2Alias != null && isUniqueKey(table2Alias, j.getF2PureName());
        } else {
            // news 不为空——图 join j 到 news 的最佳方式
            prevBest = dp.get(before) == null ? null : dp.get(before).getPlan();

            // 如果子集包含叉积，我们可能没有缓存答案
            if (prevBest == null) {
                return null;
            }

            double prevBestCost = dp.get(before).getCost();
            int bestCard = dp.get(before).getCard();

            // 估计右子树的成本，之前的连接中包含 t1，则这次与 t2 连接
            if (doesJoin(prevBest, table1Alias)) {
                t1cost = prevBestCost;
                t1card = bestCard;
                leftPkey = hasUniqueKey(prevBest);

                t2cost = j.getT2Alias() == null ? 0 : stats.get(table2Name)
                        .estimateScanCost();
                t2card = j.getT2Alias() == null ? 0 : stats.get(table2Name)
                        .estimateTableCardinality(filterSelectivity.get(j.getT2Alias()));
                rightPkey = j.getT2Alias() != null && isUniqueKey(j.getT2Alias(), j.getF2PureName());
            } else if (doesJoin(prevBest, j.getT2Alias())) {
                t2cost = prevBestCost;
                t2card = bestCard;
                rightPkey = hasUniqueKey(prevBest);

                t1cost = stats.get(table1Name).estimateScanCost();
                t1card = stats.get(table1Name)
                        .estimateTableCardinality(filterSelectivity.get(j.getT1Alias()));
                leftPkey = isUniqueKey(j.getT1Alias(), j.getF1PureName());

            } else {
                return null;
            }
        }

        // 当 left 是 before 的情况
        double cost1 = estimateJoinCost(j, t1card, t2card, t1cost, t2cost);

        // 当 right 是 before 的情况
        LogicalJoinNode j2 = j.swapInnerOuter();
        double cost2 = estimateJoinCost(j2, t2card, t1card, t2cost, t1cost);
        if (cost2 < cost1) {
            boolean tmp;
            j = j2;
            cost1 = cost2;
            tmp = rightPkey;
            rightPkey = leftPkey;
            leftPkey = tmp;
        }
        if (cost1 >= bestCostSoFar)
            return null;

        CostCard cc = new CostCard();

        cc.card = estimateJoinCardinality(j, t1card, t2card, leftPkey,
                rightPkey, stats);
        cc.cost = cost1;
        cc.plan = new ArrayList<>(prevBest);
        cc.plan.add(j);
        return cc;
    }

    /**
     * joinList 中是否包含 table
     */
    private boolean doesJoin(List<LogicalJoinNode> joinList, String table) {
        for (LogicalJoinNode j : joinList) {
            if (j.getT1Alias().equals(table)
                    || (j.getT2Alias() != null && j.getT2Alias().equals(table)))
                return true;
        }
        return false;
    }

    /**
     * 如果字段是指定表的唯一键，则返回 true，否则返回 false
     *
     * @param tableAlias 查询中表的别名
     * @param field      纯字段名
     */
    private boolean isUniqueKey(String tableAlias, String field) {
        String table = p.getTableName(tableAlias);
        TableDesc tableDesc = Database.getCatalog().getTableDesc(table);
        int indexType = tableDesc.getIndexType(tableDesc.fieldNameToIndex(field));
        Set<IndexType> typeSet = IndexType.intToIndexSet(indexType);
        return typeSet.contains(IndexType.BTREE_UNIQUE) || typeSet.contains(IndexType.HASH_UNIQUE);
    }

    /**
     * 如果给定连接中存在由唯一键驱动的连接，则返回真
     */
    private boolean hasUniqueKey(List<LogicalJoinNode> joinList) {
        for (LogicalJoinNode j : joinList) {
            if (isUniqueKey(j.getT1Alias(), j.getF1PureName())
                    || (j.getT2Alias() != null && isUniqueKey(j.getT2Alias(), j.getF2PureName())))
                return true;
        }
        return false;
    }
}