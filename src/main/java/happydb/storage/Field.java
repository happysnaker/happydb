package happydb.storage;


import happydb.common.ByteArray;
import happydb.common.DbSerializable;
import happydb.execution.Predicate;

import java.io.Serializable;

/**
 * SimpleDB 中行记录字段值的接口。
 * @Author happysnaker
 * @Date 2022/11/16
 * @Email happysnaker@foxmail.com
 */
public interface Field extends DbSerializable {
    /**
     * 将此字段对象的值与传入的值进行比较，<strong>传入字段将作为右侧的操作数</strong>
     * <p>例如此字段值为 3，传入参数为 > 和 2，那么此方法返回 3 > 2，即 true</p>
     * <P><strong>请注意，int 与 double 字段应该具备可比性</strong></P>
     * @param op 操作符
     * @param value 与此字段进行比较的值，此值作为操作数
     * @return 比较结果是否为真。
     */
    boolean compare(Predicate.Op op, Field value);

    /**
     * 返回该字段的类型 (see {@link Type})
     * @return 该字段的类型
     */
    Type getType();


    /**
     * 获取字段的值
     * @return 返回字段的值
     */
    Object getObject();
}