package happydb.optimizer;

import happydb.common.Database;
import happydb.common.Pair;
import happydb.exception.DbException;
import happydb.execution.BTreeSeqScan;
import happydb.execution.OpIterator;
import happydb.execution.Predicate;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.TransactionId;
import lombok.Getter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static happydb.execution.Aggregator.toDouble;

/**
 * 包含某个表所有字段的直方图，并能给出 IO 代价以及预估基数
 *
 * @Author happysnaker
 * @Date 2022/11/26
 * @Email happysnaker@foxmail.com
 */
public class TableState {
    /**
     * TableState 用来查询的表的超级事务
     */
    private static final TransactionId TABLE_STATE_SUPER_TRANSACTION = new TransactionId(-8848);
    /**
     * 直方图的桶数
     */
    public static final int NUM_HIST_BINS = 200;

    @Getter
    private final String tableName;
    @Getter
    private final int ioCostFactorPerPage;

    private final Map<Integer, Histogram> histogramMap = new HashMap<>();

    @Getter
    private volatile int numRecords;

    /**
     * 在某个表上创建 TableState
     *
     * @param tableName           给定的表
     * @param ioCostFactorPerPage 使 IO 成本与 CPU 成本具备可比性，每页的 IO 成本需要乘上此因子
     */
    public TableState(String tableName, int ioCostFactorPerPage) throws DbException {
        this.tableName = tableName;
        this.ioCostFactorPerPage = ioCostFactorPerPage;

        OpIterator iterator = new BTreeSeqScan(TABLE_STATE_SUPER_TRANSACTION, tableName, null, null);
        iterator.open();
        Record[] records = iterator.getRecordAr();


        Map<Integer, double[]> minMaxMap = new HashMap<>();
        for (Record record : records) {
            for (int i = 0; i < record.getNumFields(); i++) {
                if (record.getTableDesc().getFieldType(i) == Type.STRING_TYPE) {
                    this.histogramMap.put(i, new StringHistogram(NUM_HIST_BINS));
                    continue;
                }

                double val = toDouble(record.getField(i).getObject());
                minMaxMap.putIfAbsent(i, new double[]{val, val});
                minMaxMap.put(i, new double[]{Math.min(val, minMaxMap.get(i)[0]), Math.max(val, minMaxMap.get(i)[1])});
            }
        }


        // 根据最大最小值初始化 INT 或 Double 直方图
        // 要注意，可能表中没有元组，因此 histogramMap.get(i) 可能为 null
        for (Map.Entry<Integer, double[]> it : minMaxMap.entrySet()) {
            var min = it.getValue()[0];
            var max = it.getValue()[1];
            int key = it.getKey();
            if (records[0].getTableDesc().getFieldType(key) == Type.INT_TYPE) {
                histogramMap.put(key, new IntHistogram(NUM_HIST_BINS, (int) min, (int) max + 1));
            } else {
                histogramMap.put(key, new DoubleHistogram(NUM_HIST_BINS, min, max + 1));
            }
        }

        // 构建好了之后再添加值进去
        for (Record record : records) {
            insertRecord(record);
        }
    }


    /**
     * 给出扫描表的预估成本。<P>
     * 在此项目中，不考虑索引、缓存等其他条件，扫描成本总是等于页的数量乘以 {@link #ioCostFactorPerPage}
     *
     * @return 扫描表的估计成本。
     */
    public double estimateScanCost() {
        try {
            return ((HeapPageManager) Database.getCatalog().getPageManager(tableName)).numPages() * ioCostFactorPerPage;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 如果应用了具有选择性 selectivityFactor 的谓词，则此方法返回关系中元组的数量。
     *
     * @param selectivityFactor 任何谓词对表的选择性
     * @return 具有指定 selectivityFactor 的扫描的估计基数
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // 选择的可能性 * 总元组数
        return (int) (selectivityFactor * this.numRecords);
    }

    /**
     * 估计谓词 <tt>field op constant<tt> 在表上的选择性。
     *
     * @param field    谓词范围的字段
     * @param op       谓词中的逻辑运算
     * @param constant 与字段进行比较的值
     * @return 估计的选择性（满足谓词的元组的分数）
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (histogramMap.get(field) == null) {
            return 0;
        }
        return histogramMap.get(field).estimateSelectivity(op, constant);
    }

    /**
     * 添加元组后动态维护直方图
     *
     * @param record
     */
    public synchronized void insertRecord(Record record) {
        for (int i = 0; i < record.getNumFields(); i++) {
            if (histogramMap.get(i) != null) {
                histogramMap.get(i).addValue(record.getField(i));
            }
        }
        this.numRecords++;
    }


    /**
     * 删除元组后动态维护直方图
     *
     * @param record
     */
    public synchronized void deleteRecord(Record record) {
        for (int i = 0; i < record.getNumFields(); i++) {
            if (histogramMap.get(i) != null) {
                histogramMap.get(i).deleteValue(record.getField(i));
            }
        }
        this.numRecords--;
    }

    /**
     * 返回某个字段上的直方对
     *
     * @param field 字段下标
     * @return 直方对
     */
    public List<Pair<Field, Integer>> getHistogramPair(int field) {
        if (histogramMap.get(field) == null) {
            return new ArrayList<>();
        }
        return histogramMap.get(field).getHistogramPair();
    }
}
