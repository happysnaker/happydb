package happydb.execution;

import happydb.storage.Field;
import happydb.storage.Record;
import lombok.Getter;

import java.io.Serializable;
import java.util.Map;

/**
 * 谓词将元组与指定的字段值进行比较。
 *
 * @Author happysnaker
 * @Date 2022/11/16
 * @Email happysnaker@foxmail.com
 */
public record Predicate(@Getter int field, @Getter happydb.execution.Predicate.Op op,
                        @Getter Field operand) implements Serializable {
    /**
     * 存储字符串到 Op 的映射
     */
    public final static Map<String, Op> OP_MAP = Map.of(
            "=", Op.EQUALS,
            ">", Op.GREATER_THAN,
            ">=", Op.GREATER_THAN_OR_EQ,
            "<", Op.LESS_THAN,
            "<=", Op.LESS_THAN_OR_EQ,
            "!=", Op.NOT_EQUALS,
            "<>", Op.NOT_EQUALS,
            "LIKE", Op.LIKE
    );

    /**
     * Field.compare 中用于返回码的常量
     */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        public String toString() {
            for (Map.Entry<String, Op> it : OP_MAP.entrySet()) {
                if (this == it.getValue()) {
                    return it.getKey();
                }
            }
            throw new IllegalStateException("impossible to reach here");
        }

        /**
         * 返回调换两边字段后的操作符
         */
        public Op getReverseOp() {
            return switch (this) {
                case GREATER_THAN -> LESS_THAN_OR_EQ;
                case GREATER_THAN_OR_EQ -> LESS_THAN;
                case EQUALS, LIKE, NOT_EQUALS -> this;
                case LESS_THAN -> GREATER_THAN_OR_EQ;
                case LESS_THAN_OR_EQ -> GREATER_THAN;
            };
        }
    }

    /**
     * 构造函数。示例：
     * <P>例如对于 SQL SELECT * FROM t WHERE x >= 2 而言，
     * x 是待比较的字段，>= 是操作符，2 是 operand 操作数</P>
     * <P>在 {@link #filter(Record)} 中，会传入一个记录进行预言，例如如果记录的 x = 3，那么返回真</P>
     *
     * @param field   传递给要比较的元组的字段下标。
     * @param op      用于比较的操作
     * @param operand 操作数
     */
    public Predicate {
    }

    /**
     * 运用运算符进行过滤，如果记录满足谓词，则为真
     */
    public boolean filter(Record record) {
        return record.getField(field).compare(op, operand);
    }
}