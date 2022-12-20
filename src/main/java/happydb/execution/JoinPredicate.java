package happydb.execution;

import happydb.storage.Record;
import lombok.Getter;

import java.io.Serializable;

/**
 * @Author happysnaker
 * @Date 2022/11/23
 * @Email happysnaker@foxmail.com
 */
public record JoinPredicate(@Getter int field1, @Getter Predicate.Op op, @Getter int field2) implements Serializable {

    /**
     * 构造函数——在两个元组的两个字段上创建一个新谓词。
     * <p>
     * 例如 <code>SELECT * FROM a, b WHERE a.x = b.y</code> 中，
     * x 是第一个表的字段，y 是第二个表的字段，操作符是 =
     * <P>{@link #filter(Record, Record)} 中会传入两个记录，通过 field1 和 field2 获取对应的字段，进行过滤</P>
     *
     * @param field1 谓词中第一个元组的字段索引
     * @param field2 谓词中第二个元组的字段索引
     * @param op     要应用的操作（在 Predicate.Op 中定义）
     * @see Predicate
     */
    public JoinPredicate {
    }

    /**
     * 将谓词应用于两个指定的 {@link Record}。可以通过 Field 的 compare 方法进行比较。
     *
     * @return 如果满足谓词，则为真。
     */
    public boolean filter(Record t1, Record t2) {
        return t1.getField(field1).compare(op, t2.getField(field2));
    }
}
