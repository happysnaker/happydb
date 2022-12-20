package happydb.execution;

import happydb.storage.TableDesc;
import happydb.storage.Type;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.io.Serializable;
import java.util.Map;

/**
 * 聚合器
 *
 * @Author happysnaker
 * @Date 2022/11/23
 * @Email happysnaker@foxmail.com
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AggregatorNode {
    /**
     * 聚合字段名称，a.x 形式
     */
    String aggregateFieldName;
    /**
     * 聚合字段下标，在表连接之前，这个下标可能是不确定的，他必须要根据 aggregateFieldName 字段确定
     */
    int aggregateField;
    /**
     * 聚合运算符
     */
    Op op;

    // for test
    public AggregatorNode(int aggregateField, Op op) {
        this.aggregateField = aggregateField;
        this.op = op;
    }

    public AggregatorNode(String aggregateFieldName, Op op) {
        this.aggregateFieldName = aggregateFieldName;
        this.op = op;
    }

    public String toString(TableDesc tableDesc) {
        return String.format("%s(%s)", op.toString(), tableDesc.getFieldName(aggregateField));
    }

    public Type getType(TableDesc tableDesc) {
        if (op == Op.AVG)
            return Type.DOUBLE_TYPE;
        if (op == Op.COUNT)
            return Type.INT_TYPE;
        return tableDesc.getFieldType(aggregateField);
    }

    public static Map<String, Op> OP_MAP = Map.of(
            "MIN", Op.MIN,
            "MAX", Op.MAX,
            "SUM", Op.SUM,
            "AVG", Op.AVG,
            "COUNT", Op.COUNT
    );

    /**
     * 聚合运算符
     */
    public enum Op implements Serializable {
        MIN, MAX, SUM, AVG, COUNT;

        public String toString() {
            for (Map.Entry<String, Op> it : OP_MAP.entrySet()) {
                if (this == it.getValue()) {
                    return it.getKey();
                }
            }
            throw new IllegalStateException("impossible to reach here");
        }
    }
}
