package happydb.optimizer;

import happydb.common.Pair;
import happydb.execution.Predicate;
import happydb.storage.Field;

import java.io.Serializable;
import java.util.List;

/**
 * 某个字段上的查询直方图
 * @Author happysnaker
 * @Date 2022/11/26
 * @Email happysnaker@foxmail.com
 */
public interface Histogram extends Serializable {

    /**
     * 向直方图中添加一个字段，如果这个字段超过直方图最大最小范围，他应该被视为最大值或最小值
     * @param field 字段
     */
    void addValue(Field field);


    /**
     * 向直方图中移除一个字段，如果这个字段超过直方图最大最小范围，他应该被视为最大值或最小值
     * @param field 字段
     */
    void deleteValue(Field field);

    /**
     * 给定一个谓词和一个操作数，用于评估选择性，例如谓词与操作数为 {@link happydb.execution.Predicate.Op#GREATER_THAN} 和 10，
     * 如果表中有一半的元组在直方图字段上大于 10，则选择性为 0.5，注意，选择性只是个预估的概念，他不一定严格准确
     * @param op 操作符
     * @param operand 操作数
     * @return 选择性
     */
    double estimateSelectivity(Predicate.Op op, Field operand);

    /**
     * 获取此直方图中的直方对
     * @return 直方对
     */
    List<Pair<Field, Integer>> getHistogramPair();
}
