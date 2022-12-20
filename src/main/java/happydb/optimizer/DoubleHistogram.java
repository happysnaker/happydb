package happydb.optimizer;

import happydb.common.Pair;
import happydb.execution.Predicate;
import happydb.storage.DoubleField;
import happydb.storage.Field;
import happydb.storage.IntField;

import java.util.List;

import static happydb.execution.Aggregator.toDouble;

/**
 * Double 直方图利用 Int 直方图进行粗略的评估
 *
 * @Author happysnaker
 * @Date 2022/11/26
 * @Email happysnaker@foxmail.com
 */
public class DoubleHistogram implements Histogram {

    IntHistogram histogram;

    public DoubleHistogram(int buckets, double min, double max) {
        histogram = new IntHistogram(buckets, roundingInt(min), roundingInt(max));
    }

    @Override
    public void addValue(Field field) {
        histogram.addValue(new IntField(roundingInt(toDouble(field.getObject()))));
    }

    @Override
    public void deleteValue(Field field) {
        histogram.deleteValue(new IntField(roundingInt(toDouble(field.getObject()))));
    }

    @Override
    public double estimateSelectivity(Predicate.Op op, Field operand) {
        return histogram.estimateSelectivity(op, new IntField(roundingInt(toDouble(operand.getObject()))));
    }

    @Override
    public List<Pair<Field, Integer>> getHistogramPair() {
        return histogram.getHistogramPair()
                .stream()
                .peek(p -> p.setKey(new DoubleField(toDouble(p.getKey().getObject()))))
                .toList();
    }


    public static int roundingInt(double v) {
        return (int) Math.floor(v + 0.5);
    }
}
