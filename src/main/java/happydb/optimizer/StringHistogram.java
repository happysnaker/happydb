package happydb.optimizer;

import happydb.common.Pair;
import happydb.execution.Predicate;
import happydb.storage.DoubleField;
import happydb.storage.Field;
import happydb.storage.IntField;
import happydb.storage.StringField;

import java.util.List;

/**
 * @Author happysnaker
 * @Date 2022/11/26
 * @Email happysnaker@foxmail.com
 */
public class StringHistogram implements Histogram {

    private final IntHistogram histogram;

    /**
     * 使用指定数量的桶创建一个新的 StringHistogram。
     * <p>
     * 我们的实现是根据 IntHistogram 编写的，方法是将每个 String 转换为整数。
     *
     * @param buckets 桶的数量
     */
    public StringHistogram(int buckets) {
        histogram = new IntHistogram(buckets, minVal(), maxVal());
    }


    private static String intToString(int v) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            int c = ((v) >> (i * 8)) & (0xFF);
            if (c == 0) {
                break;
            }
            sb.append((char) c);
        }
        return sb.reverse().toString();
    }


    /**
     * 将字符串转换为整数，如果返回值(s1) < 返回值(s2)，则s1 < s2
     */
    private static int stringToInt(String s) {
        int i;
        int v = 0;
        for (i = 3; i >= 0; i--) {
            if (s.length() > 3 - i) {
                int ci = s.charAt(3 - i);
                v += (ci) << (i * 8);
            }
        }

        // XXX: hack to avoid getting wrong results for
        // strings which don't output in the range min to max
        if (!(s.equals("") || s.equals("zzzz"))) {
            if (v < minVal()) {
                v = minVal();
            }

            if (v > maxVal()) {
                v = maxVal();
            }
        }

        return v;
    }

    /**
     * @return 直方图索引的最大值
     */
    static int maxVal() {
        return stringToInt("zzzz");
    }

    /**
     * @return 直方图索引的最小值
     */
    static int minVal() {
        return stringToInt("");
    }

    @Override
    public void addValue(Field field) {
        int v = stringToInt((String) field.getObject());
        histogram.addValue(new IntField(v));
    }

    @Override
    public void deleteValue(Field field) {
        int v = stringToInt((String) field.getObject());
        histogram.deleteValue(new IntField(v));
    }

    @Override
    public double estimateSelectivity(Predicate.Op op, Field operand) {
        int v = stringToInt((String) operand.getObject());
        return histogram.estimateSelectivity(op, new IntField(v));
    }


    @Override
    public List<Pair<Field, Integer>> getHistogramPair() {
        return histogram.getHistogramPair()
                .stream()
                .peek(p -> p.setKey(new StringField(intToString((int) p.getKey().getObject()))))
                .toList();
    }
}
