package happydb.optimizer;

import happydb.common.Pair;
import happydb.execution.Predicate;
import happydb.storage.Field;
import happydb.storage.IntField;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author happysnaker
 * @Date 2022/11/26
 * @Email happysnaker@foxmail.com
 */
public class IntHistogram implements Histogram {

    private volatile int sum;
    private final double width;
    private final int max; // 右侧开区间
    private final int min;
    private final int[] buckets;

    /**
     * 创建一个新的 IntHistogram。
     * <p>
     * 此 IntHistogram 应维护它接收到的整数值的直方图。它应该将直方图分成“桶”桶。
     * <p>
     * 正在绘制直方图的值将通过“addValue()”函数一次一个地提供。
     * <p>
     * 您的实现应该使用空间并具有相对于被直方图化的值的数量而言都是恒定的执行时间。例如，您不应该简单地将看到的每个值存储在排序列表中。
     *
     * @param buckets 将输入值拆分成的桶数。
     * @param min     将传递给此类以进行直方图绘制的最小整数值
     * @param max     将传递给此类以进行直方图绘制的最大整数值
     */
    public IntHistogram(int buckets, int min, int max) {
        this.buckets = new int[Math.min(buckets, max - min)];
        this.min = min;
        this.max = max + 1;     // 右区间开放
        this.width = Math.max(1.0, (this.max - this.min) / (buckets * 1.0F));
    }

    /**
     * 根据值获取对应的通，一个桶总是左闭右开的
     *
     * @param v 值
     * @return 桶的下标
     */
    private int getBucketIndex(int v) {
        int index = (int) ((v - min) / width);
        // 由于使用 double 作为桶宽，可能存在精度问题
        return Math.min(this.buckets.length - 1, Math.max(0, index));
    }

    @Override
    public void addValue(Field field) {
        int v = (int) field.getObject();
        if (v >= min && v < max) {
            this.buckets[getBucketIndex(v)]++;
            sum++;
        } else {
            addValue(new IntField(v >= max ? max - 1 : min));
        }
    }

    @Override
    public void deleteValue(Field field) {
        int v = (int) field.getObject();
        if (v >= min && v < max) {
            this.buckets[getBucketIndex(v)]--;
            sum--;
        } else {
            deleteValue(new IntField(v >= max ? max - 1 : min));
        }
    }

    @Override
    public double estimateSelectivity(Predicate.Op op, Field operand) {
        int v = (int) operand.getObject();
        switch (op) {
            case EQUALS -> {
                if (v < min || v >= max) {
                    return 0.0;
                }
                return (this.buckets[getBucketIndex(v)] / this.width) / this.sum;
            }
            case LESS_THAN -> {
                if (v < min || v >= max) {
                    return v < min ? 0.0 : 1.0;
                }
                int lessThanNums = 0;
                int bucketIndex = getBucketIndex(v);
                for (int i = 0; i < bucketIndex; i++) {
                    lessThanNums += buckets[i];
                }
                double leftBound = (bucketIndex) * width + min, proportion = (v - leftBound) / width;
                return (lessThanNums + proportion * buckets[bucketIndex]) / this.sum;
            }
            case LESS_THAN_OR_EQ -> {
                return estimateSelectivity(Predicate.Op.LESS_THAN, operand) + estimateSelectivity(Predicate.Op.EQUALS, operand);
            }
            case GREATER_THAN -> {
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, operand);
            }
            case GREATER_THAN_OR_EQ -> {
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN, operand);
            }
            case NOT_EQUALS -> {
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, operand);
            }
        }
        throw new RuntimeException();
    }

    @Override
    public List<Pair<Field, Integer>> getHistogramPair() {
        List<Pair<Field, Integer>> pairs = new ArrayList<>();
        for (int i = 0; i < this.buckets.length; i++) {
            int avg = (int) (((width * i + min) + (width * i + min + width)) / 2);
            pairs.add(Pair.create(new IntField(avg), buckets[i]));
        }
        return pairs;
    }
}
