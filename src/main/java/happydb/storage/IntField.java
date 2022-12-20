package happydb.storage;

import happydb.common.ByteArray;
import happydb.execution.Predicate;
import lombok.Data;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @Author happysnaker
 * @Date 2022/11/16
 * @Email happysnaker@foxmail.com
 */
public record IntField(int value) implements Field {


    public Object getObject() {
        return value;
    }


    @Override
    public ByteArray serialized() {
        return new ByteArray(value);
    }

    /**
     * 将指定字段与此字段的值进行比较。返回语义由 Field.compare 指定
     *
     * @see Field#compare
     */
    public boolean compare(Predicate.Op op, Field val) {
        if (val instanceof DoubleField) {
            val = new IntField((int) val.getObject());
        }
        IntField otherVal = (IntField) val;

        return switch (op) {
            case EQUALS, LIKE -> value == otherVal.value;
            case NOT_EQUALS -> value != otherVal.value;
            case GREATER_THAN -> value > otherVal.value;
            case GREATER_THAN_OR_EQ -> value >= otherVal.value;
            case LESS_THAN -> value < otherVal.value;
            case LESS_THAN_OR_EQ -> value <= otherVal.value;
        };

    }

    /**
     * 返回此字段的类型。
     *
     * @return Type.INT_TYPE
     */
    public Type getType() {
        return Type.INT_TYPE;
    }
}
