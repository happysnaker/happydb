package happydb.storage;

import happydb.common.ByteArray;
import happydb.common.ByteList;
import happydb.execution.Predicate;
import lombok.Getter;

import java.nio.charset.StandardCharsets;

import static happydb.storage.Type.STRING_LEN;

/**
 * @Author happysnaker
 * @Date 2022/11/16
 * @Email happysnaker@foxmail.com
 */

public record StringField(String value) implements Field {
    public StringField(String value) {
        if (value.getBytes(StandardCharsets.UTF_8).length > STRING_LEN)
            throw new IllegalArgumentException("字符串长度超出限制");
        this.value = value;
    }

    @Override
    public Object getObject() {
        return value;
    }


    @Override
    public ByteArray serialized() {
        ByteArray byteAr = ByteArray.allocate(getType().getLen());
        byteAr.writeInt(value.getBytes(StandardCharsets.UTF_8).length);
        byteAr.writeString(value);
        return byteAr;
    }

    /**
     * 将指定字段与此字段的值进行比较。返回语义由 Field.compare 指定
     *
     * @see Field#compare
     */
    public boolean compare(Predicate.Op op, Field val) {

        StringField otherVal = (StringField) val;

        int cmpVal = value.compareTo(otherVal.value);

        return switch (op) {
            case EQUALS -> cmpVal == 0;
            case NOT_EQUALS -> cmpVal != 0;
            case GREATER_THAN -> cmpVal > 0;
            case GREATER_THAN_OR_EQ -> cmpVal >= 0;
            case LESS_THAN -> cmpVal < 0;
            case LESS_THAN_OR_EQ -> cmpVal <= 0;
            case LIKE -> value.contains(otherVal.value);
        };

    }

    /**
     * @return 该字段的类型
     */
    public Type getType() {
        return Type.STRING_TYPE;
    }
}

