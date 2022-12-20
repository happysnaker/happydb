package happydb.execution;

import happydb.storage.*;
import happydb.storage.Record;
import lombok.Getter;
import happydb.execution.AggregatorNode.*;

import java.util.*;

/**
 * 聚合器
 *
 * @Author happysnaker
 * @Date 2022/11/23
 * @Email happysnaker@foxmail.com
 */
public class Aggregator {
    /**
     * 表示没有分组
     */
    public static int NO_GROUPING = -1;
    @Getter
    private final TableDesc tableDesc;
    @Getter
    private final int groupByField;
    @Getter
    private final List<AggregatorNode> nodes;


    /**
     * 创建一个数字类型的聚合器，聚合器支持多个聚合对象，但最多支持一种分组对象
     * <p>
     * 例如 <code>SELECT country, SUM(x), AVG(x), MIN(y) FROM xxx</code>，在这个例子中，
     * country 是分组字段，SUM(x), AVG(x), MIN(y) 是聚合列表，参数 nodes 顺序严格符合 select 列表中给出的聚合顺序
     * </P>
     * <P>请注意，<strong>String 列上只能存在 COUNT 聚合运算符，否则，此运算符可能无法正常工作</strong></P>
     * @param tableDesc    在哪种表上进行分组聚合
     * @param groupByField 分组的字段，可能不需要分组，则此字段为 {@link #NO_GROUPING}
     * @param nodes        待聚合的对象，由 {@link #iterator()} 返回的结果中的列顺序必须要与此参数一致，HelloDb 保证 nodes 非空
     */
    public Aggregator(TableDesc tableDesc, int groupByField, List<AggregatorNode> nodes) {
        this.groupByField = groupByField;
        this.nodes = nodes;
        this.tableDesc = getAggregateTableDesc(tableDesc, groupByField, nodes);

        for (AggregatorNode node : nodes) {
            if (node.op != AggregatorNode.Op.COUNT && tableDesc.getFieldType(node.aggregateField) == Type.STRING_TYPE) {
                throw new IllegalArgumentException("STRING 类型不允许相应分组运算符 " + node.op);
            }
        }
    }

    /**
     * 创建一个投影模式，第一列为分组字段（如果有的话），剩下的列是聚合字段，顺序与参数 nodes 一致
     */
    public TableDesc getAggregateTableDesc(TableDesc tableDesc, int groupByField, List<AggregatorNode> nodes) {
        return getTableDesc(tableDesc, groupByField, nodes, groupByField == NO_GROUPING);
    }

    static TableDesc getTableDesc(TableDesc tableDesc, int groupByField, List<AggregatorNode> nodes, boolean noGrouping) {
        int n = noGrouping ? nodes.size() : nodes.size() + 1;
        String[] fieldNameAr = new String[n];
        Type[] typeAr = new Type[n];
        int i = 0;
        if (groupByField != -1) {
            fieldNameAr[i] = tableDesc.getFieldName(groupByField);
            typeAr[i] = tableDesc.getFieldType(groupByField);
            i++;
        }
        for (AggregatorNode node : nodes) {
            fieldNameAr[i] = node.toString(tableDesc);
            typeAr[i] = node.getType(tableDesc);
            i++;
        }
        return new TableDesc(fieldNameAr, typeAr);
    }


    /**
     * key 是待聚合的对象，val 是一层嵌套的 map，此嵌套 map 的 key 为分组的值，val 为聚合的结果
     */
    private final Map<AggregatorNode, Map<Field, Double>> resultMap = new HashMap<>();
    /**
     * 此属性仅用于计算 AVG 时统计元组数量
     */
    private final Map<AggregatorNode, Map<Field, Integer>> countMap = new HashMap<>();


    /**
     * 将新记录合并到聚合中以获得不同的组值；如果尚未遇到组值，则创建一个新的组聚合结果。
     *
     * @param record 包含聚合字段和分组字段的记录
     */
    public void mergeRecordIntoGroup(Record record) {
        // HashMap 的 Key 允许为空，但 ConcurrentHashMap 不允许
        Field groupByField = this.groupByField == NO_GROUPING ? null : record.getField(this.groupByField);
        for (AggregatorNode node : nodes) {
            resultMap.putIfAbsent(node, new HashMap<>());
            countMap.putIfAbsent(node, new HashMap<>());
            Map<Field, Double> rMap = resultMap.get(node);
            Map<Field, Integer> cMap = countMap.get(node);

            Field aggregatorField = record.getField(node.getAggregateField());
            switch (node.op) {
                case MIN -> {
                    rMap.putIfAbsent(groupByField, toDouble(aggregatorField.getObject()));
                    rMap.put(groupByField, Math.min(rMap.get(groupByField), toDouble(aggregatorField.getObject())));
                }
                case MAX -> {
                    rMap.putIfAbsent(groupByField, toDouble(aggregatorField.getObject()));
                    rMap.put(groupByField, Math.max(rMap.get(groupByField), toDouble(aggregatorField.getObject())));
                }
                case SUM, AVG -> {
                    rMap.put(groupByField, rMap.getOrDefault(groupByField, 0.0) + toDouble(aggregatorField.getObject()));
                    if (node.op == Op.AVG) {
                        cMap.put(groupByField, cMap.getOrDefault(groupByField, 0) + 1);
                    }
                }
                case COUNT -> {
                    rMap.put(groupByField, rMap.getOrDefault(groupByField, 0.0) + 1.0);
                }
                default -> {
                    throw new RuntimeException("不支持的聚合操作符：" + node.op);
                }
            }
        }
    }

    /**
     * 在组聚合结果上创建一个 OpIterator，如果存在聚合，那么第一列应该是聚合字段，剩下的列按照构造函数中传递的聚合字段顺序进行构造
     * <P>如果没有分组，迭代器应该只返回一行数据</P>
     * <P>否则，返回的行数与组数一致</P>
     */
    public OpIterator iterator() {
        List<Record> records = new ArrayList<>();
        // 分组的组数，如果没有分组，应该是 1，他们的 key 为 null
        Set<Field> groups = resultMap.get(nodes.get(0)).keySet();
        for (Field group : groups) {
            Record record = new Record(tableDesc);
            int i = 0;
            if (groupByField != NO_GROUPING) {
                record.setField(i, group);
                i++;
            }
            for (AggregatorNode node : nodes) {
                Double value = resultMap.get(node).get(group);
                if (node.op == Op.AVG) {
                    record.setField(i, new DoubleField(value / countMap.get(node).get(group)));
                }

                else if (node.op == Op.COUNT) {
                    record.setField(i, new IntField(value.intValue()));
                }

                else {
                    if (tableDesc.getFieldType(i) == Type.INT_TYPE) {
                        record.setField(i, new IntField(value.intValue()));
                    } else {
                        record.setField(i, new DoubleField(value));
                    }
                }
                i++;
            }

            records.add(record);
        }
        return new RecordIterator(tableDesc, records);
    }

    public static double toDouble(Object obj) {
        return Double.parseDouble(obj.toString());
    }
}
