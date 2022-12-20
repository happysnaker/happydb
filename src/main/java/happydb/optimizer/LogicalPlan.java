package happydb.optimizer;

import happydb.common.Database;
import happydb.exception.DbException;
import happydb.exception.ParseException;
import happydb.execution.*;
import happydb.index.IndexType;
import happydb.storage.Record;
import happydb.storage.TableDesc;
import happydb.transaction.TransactionId;
import lombok.Getter;
import lombok.Setter;

import java.util.*;

import static happydb.execution.Aggregator.NO_GROUPING;

/**
 * @Author happysnaker
 * @Date 2022/11/26
 * @Email happysnaker@foxmail.com
 */
public class LogicalPlan {
    /**
     * 表别名映射到真实名，@setter 用于测试
     */
    @Getter
    @Setter
    private Map<String, String> tableMap;

    /**
     * 查询计划连接列表，不支持表达式
     */
    @Getter
    private List<LogicalJoinNode> joins;

    /**
     * 查询计划中单表的过滤列表，只允许字段对常数过滤，不支持表达式
     */
    @Getter
    private List<LogicalFilterNode> filters;

    /**
     * 选择列表，保存了 a.x 形式字段名，以及用户预估输出的字段名
     */
    @Getter
    private List<LogicalSelectListNode> selectList;

    /**
     * 待聚合的列
     */
    @Getter
    private List<AggregatorNode> aggregators;

    /**
     * 分组字段，没有则 null
     */
    @Getter
    private String groupByFieldName;

    /**
     * 排序字段，没有则 null
     */
    @Getter
    private String orderByFieldName;

    /**
     * 如果有排序，则此字段表示升序还是降序，默认升序
     */
    @Getter
    private boolean isAsc = true;


    /**
     * 由表的别名返回表的真实名
     *
     * @param alias 别名
     * @return 真实名，没有则 null
     */
    public String getTableName(String alias) {
        return tableMap.get(alias);
    }


    /**
     * 将查询逻辑计划转换为物理计划
     *
     * @param tid 事务 ID
     * @return 返回查询结果
     * @throws DbException
     * @throws ParseException
     */
    public Project toPhysicalPlan(TransactionId tid) throws DbException, ParseException {
        Map<String, OpIterator> subPlanMap = new HashMap<>();
        Map<String, Double> filterSelectivity = new HashMap<>();

        // 解析每个单表的计划
        if (this.filters == null) {
            this.filters = new ArrayList<>();
        }
        for (String tableAlias : tableMap.keySet()) {
            decideBestSubPlan(subPlanMap, filterSelectivity, tableAlias,
                    new ArrayList<>(filters.stream().filter(f -> f.tableAlias.equals(tableAlias)).toList()), tid);
        }

        // 解析连接查询
        if (this.joins == null) {
            this.joins = new ArrayList<>();
        }
        Map<String, String> equivMap = new HashMap<>(); // 如果 a&b 连接，那么 b 应该看成 a 即 b->a
        JoinOptimizer jo = new JoinOptimizer(this, joins);
        joins = jo.orderJoins(TableStateView.getInstance().getTableStateMap(new HashSet<>(tableMap.values())), filterSelectivity);
        String finalPutTable = null;
        for (LogicalJoinNode lj : joins) {
            OpIterator plan1;
            OpIterator plan2;
            String t1name, t2name;

            if (equivMap.get(lj.getT1Alias()) != null)
                t1name = equivMap.get(lj.getT1Alias());
            else
                t1name = lj.getT1Alias();

            if (equivMap.get(lj.getT2Alias()) != null)
                t2name = equivMap.get(lj.getT2Alias());
            else
                t2name = lj.getT2Alias();

            plan1 = subPlanMap.get(t1name);
            plan2 = subPlanMap.get(t2name);

            if (plan1 == plan2)
                throw new ParseException("最多只允许一次多表连接");

            if (plan1 == null)
                throw new ParseException("Unknown table in WHERE clause " + lj.getT1Alias());
            if (plan2 == null)
                throw new ParseException("Unknown table in WHERE clause " + lj.getT2Alias());

            OpIterator j;
            j = JoinOptimizer.instantiateJoin(lj, plan1, plan2);
            subPlanMap.put(t1name, j);
            finalPutTable = t1name;

            // t2 用 t1 代替
            if (!t2name.equals(t1name)) {
                subPlanMap.remove(t2name);
                equivMap.put(t2name, t1name);
            }
            // 所有用 t2 代替的也要改成用 t1 代替
            for (Map.Entry<String, String> s : equivMap.entrySet()) {
                String val = s.getValue();
                if (val.equals(t2name)) {
                    s.setValue(t1name);
                }

            }
        }


        if (subPlanMap.size() > 1 && finalPutTable == null) {
            throw new ParseException("异常错误，在连接查询中没有两表之间的连接限定词");
        }
        OpIterator node = subPlanMap.entrySet().iterator().next().getValue();
        if (finalPutTable != null) {
            node = subPlanMap.get(finalPutTable);
        }

        // 解析分组聚合
        if (this.aggregators == null) {
            this.aggregators = new ArrayList<>();
        }
        if (groupByFieldName != null || !aggregators.isEmpty()) {
            TableDesc td = node.getTableDesc();
            int groupField = groupByFieldName == null ? NO_GROUPING : td.fieldNameToIndex(groupByFieldName);
            for (AggregatorNode aggregator : aggregators) {
                aggregator.setAggregateField(td.fieldNameToIndex(aggregator.getAggregateFieldName()));
            }
            node = new Aggregate(node, groupField, getAggregators());
        }

        if (orderByFieldName != null) {
            TableDesc td = node.getTableDesc();
            node = new OrderBy(td.fieldNameToIndex(orderByFieldName), isAsc(), node);
        }
        return new Project(node, selectList, groupByFieldName);
    }

    /**
     * 根据给定的过滤条件，决定出最佳的顺序将其组合，并添加至 planMap 中以及 filterSelectivity 中。
     * <p>
     * 所谓的最佳顺序指：
     *     <ul>
     *         <li>
     *             <strong>对于 AND，有索引的优先考虑，如果都具备索引则按照基数从小到大执行</strong>
     *         </li>
     *         <li>
     *             对于 OR，将按照顺序进行匹配，有索引则走索引
     *         </li>
     *     </ul>
     *
     * </P>
     *
     * @param planMap           将放入的计划 map
     * @param filterSelectivity 待填入的选择性 map
     * @param tableAlias        表别名
     * @param filters           关于此表的过滤节点
     */
    public void decideBestSubPlan(Map<String, OpIterator> planMap, Map<String, Double> filterSelectivity,
                                   String tableAlias, List<LogicalFilterNode> filters, TransactionId tid) throws DbException {
        if (filters.isEmpty()) {
            planMap.put(tableAlias, new BTreeSeqScan(tid, getTableName(tableAlias), tableAlias, null));
            filterSelectivity.put(tableAlias, 1.0);
            return;
        }
        TableDesc td = Database.getCatalog().getTableDesc(getTableName(tableAlias));
        TableState state = TableStateView.getInstance().getTableState(getTableName(tableAlias));
        sortFilters(filters, state, td);
        for (LogicalFilterNode filter : filters) {
            int field = td.fieldNameToIndex(filter.getFieldPureName());
            Predicate predicate = new Predicate(field, filter.getOp(), filter.parseConstant(td));
            Set<IndexType> indexTypes = IndexType.intToIndexSet(td.getIndexType(field));

            if (planMap.get(tableAlias) == null) {
                planMap.put(tableAlias, getBestFilterOpIterator(indexTypes, predicate, tid, tableAlias));
            } else {
                if (filter.isAnd()) {
                    planMap.put(tableAlias, new Filter(predicate, planMap.get(tableAlias)));
                } else {
                    planMap.put(tableAlias, new UnionOnOr(
                            planMap.get(tableAlias), getBestFilterOpIterator(indexTypes, predicate, tid, tableAlias)));
                }
            }

            double selectivity = state.estimateSelectivity(field, predicate.getOp(), predicate.getOperand());
            if (filter.isAnd()) {
                filterSelectivity.put(tableAlias, filterSelectivity.getOrDefault(tableAlias, 1.0) * selectivity);
            } else {
                filterSelectivity.put(tableAlias, Math.max(filterSelectivity.getOrDefault(tableAlias, 0.0), selectivity));
            }
        }
    }

    /**
     * 对 Filters 进行排序，请参考 {@link #decideBestSubPlan(Map, Map, String, List, TransactionId)}
     * @param filters 过滤节点
     * @param state 表状态
     * @param td 真实表模式
     */
    public static void sortFilters(List<LogicalFilterNode> filters, TableState state, TableDesc td) {
        filters.sort((f1, f2) -> {
            if (f1.isAnd() && f2.isAnd()) {
                int field1 = td.fieldNameToIndex(f1.fieldPureName);
                int field2 = td.fieldNameToIndex(f2.fieldPureName);
                boolean hasIndex1 = !IndexType.intToIndexSet(td.getIndexType(field1)).isEmpty();
                boolean hasIndex2 = !IndexType.intToIndexSet(td.getIndexType(field2)).isEmpty();
                if ((hasIndex1 && hasIndex2) || (!hasIndex1 && !hasIndex2)) {
                    return Double.compare(
                            state.estimateSelectivity(field1, f1.getOp(), f1.parseConstant(td)),
                            state.estimateSelectivity(field2, f2.getOp(), f2.parseConstant(td))
                    );
                } else {
                    return hasIndex1 ? -1 : 1;
                }
            } else if (!f1.isAnd() && !f1.isAnd()) {
                return 0;
            } else {
                return f1.isAnd() ? -1 : 1;
            }
        });
    }

    /**
     * 根据 Predicate 给出最优的执行计划
     *
     * @param indexTypes
     * @param predicate
     * @param tid
     * @param tableAlias
     * @return
     */
    public OpIterator getBestFilterOpIterator(Set<IndexType> indexTypes, Predicate predicate, TransactionId tid, String tableAlias) {
        if (predicate.getOp() == Predicate.Op.EQUALS && indexTypes.contains(IndexType.HASH)) {
            // using hash
            throw new RuntimeException("Hash index haven`t implement");
        } else if (indexTypes.contains(IndexType.BTREE)) {
            return new BTreeSeqScan(tid, getTableName(tableAlias), tableAlias, predicate);
        } else {
            return new Filter(predicate, new BTreeSeqScan(tid, getTableName(tableAlias), tableAlias, null));
        }
    }


    // ================= 一些动态构造方法 =================

    public void addFilter(String fieldName, String opName, String constant, boolean isAnd) throws ParseException {
        if (this.filters == null) {
            this.filters = new ArrayList<>();
        }
        fieldName = disambiguateName(fieldName);
        opName = opName.toUpperCase(Locale.ROOT);
        if (Predicate.OP_MAP.get(opName) == null) {
            throw new ParseException("未知的操作符 " + opName);
        }
        this.filters.add(new LogicalFilterNode(
                getTableAlias(fieldName), getFieldName(fieldName), Predicate.OP_MAP.get(opName), constant, isAnd));
    }


    public void addJoin(String fieldName1, String opName, String fieldName2) throws ParseException {
        if (this.joins == null) {
            this.joins = new ArrayList<>();
        }
        opName = opName.toUpperCase(Locale.ROOT);
        if (Predicate.OP_MAP.get(opName) == null) {
            throw new ParseException("未知的操作符 " + opName);
        }

        fieldName1 = disambiguateName(fieldName1);
        fieldName2 = disambiguateName(fieldName2);
        this.joins.add(new LogicalJoinNode(
                getTableAlias(fieldName1), getTableAlias(fieldName2),
                getFieldName(fieldName1), getFieldName(fieldName2), Predicate.OP_MAP.get(opName)
        ));
    }


    public void addTable(String tableAlias, String tableName) throws ParseException {
        if (this.tableMap == null) {
            this.tableMap = new HashMap<>();
        }
        try {
            Database.getCatalog().getTableDesc(tableName);
        } catch (NoSuchElementException e) {
            throw new ParseException("不存在的表 " + tableName);
        }
        this.tableMap.put(tableAlias, tableName);
    }


    public void addSelect(String fieldName, String aggOp, String as) throws ParseException {
        if (this.selectList == null) {
            this.selectList = new ArrayList<>();
        }
        fieldName = disambiguateName(fieldName);
        if (fieldName.equals("*"))
            fieldName = "null.*";
        selectList.add(new LogicalSelectListNode(fieldName, aggOp, as));
    }


    public void addAggregate(String aggOp, String aggFieldName) throws ParseException {
        if (this.aggregators == null) {
            this.aggregators = new ArrayList<>();
        }

        aggOp = aggOp.toUpperCase(Locale.ROOT);
        if (AggregatorNode.OP_MAP.get(aggOp) == null) {
            throw new ParseException("未知的操作符 " + aggOp);
        }
        aggFieldName = disambiguateName(aggFieldName);
        this.aggregators.add(new AggregatorNode(aggFieldName, AggregatorNode.OP_MAP.get(aggOp)));
    }


    public void addGroupBy(String groupByFieldName) throws ParseException {
        this.groupByFieldName = disambiguateName(groupByFieldName);
    }


    public void addOrder(String orderByFieldName, boolean asc) throws ParseException {
        this.orderByFieldName = disambiguateName(orderByFieldName);
        this.isAsc = asc;
    }


    private String getTableAlias(String quantifiedName) {
        return quantifiedName.split("[.]")[0];
    }

    private String getFieldName(String quantifiedName) {
        return quantifiedName.split("[.]")[1];
    }


    /**
     * 给定一个字段的名称，尝试通过查看通过添加的所有表来找出它属于哪个表
     *
     * @return tableAlias.name 形式的完全限定名称。
     * 如果 name 参数已经用表名限定，则会直接返回。
     * 如果 name 是 *（只出现在 select 中），则直接返回。
     * @throws ParseException 如果找不到对应的表，或者找到了多个（语义不明确）
     */
    private String disambiguateName(String name) throws ParseException {
        String[] fields = name.split("[.]");

        if (fields.length > 2) {
            throw new ParseException("不正确的字段名称 " + name);
        }

        if (fields.length == 2 && (!fields[0].equals("null"))) {
            return name;
        }

        if (fields.length == 2) {
            name = fields[1];
        }

        if (name.equals("*")) {
            return name;
        }

        String tableName = null;

        for (String value : tableMap.values()) {
            try {
                Database.getCatalog().getTableDesc(value).fieldNameToIndex(name);
                // 如果没有抛出异常，说明找到了

                if (tableName != null) {
                    throw new ParseException("字段 " + name + " 不明确");
                }
                tableName = value;
            } catch (NoSuchElementException ignore) {
            }
        }

        if (tableName != null)
            return tableName + "." + name;
        else
            throw new ParseException("字段 " + name + " 没有出现在任何表格中。");

    }
}
