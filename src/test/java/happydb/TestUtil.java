package happydb;

import happydb.common.Database;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.execution.BTreeSeqScan;
import happydb.execution.OpIterator;
import happydb.index.BTreeIndex;
import happydb.index.Index;
import happydb.index.IndexType;
import happydb.log.RedoLog;
import happydb.log.UndoLog;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.TransactionId;
import lombok.Data;
import org.junit.Assert;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

/**
 * @Author happysnaker
 * @Date 2022/11/15
 * @Email happysnaker@foxmail.com
 */
public class TestUtil {

    public static boolean checkExhausted(OpIterator it)
            throws DbException {

        if (it.hasNext()) return false;

        try {
            Record t = it.next();
            System.out.println("Got unexpected tuple: " + t);
            return false;
        } catch (NoSuchElementException e) {
            return true;
        }
    }

    @Data
    public static abstract class TestRunnable {
        public volatile boolean done = false;

        public abstract void run() throws Exception;
    }

    /**
     * 调度多个任务同时运行
     *
     * @param tasks         任务列表
     * @param timeoutMillis 最多等待时间，可以设置为无效大表示无限等待
     * @throws IllegalStateException 如果超时后检测到线程任务未完成则抛出
     */
    public static void runManyThread(List<TestRunnable> tasks, long timeoutMillis) throws IllegalStateException {
        long startTime = System.currentTimeMillis();
        CountDownLatch cdl = new CountDownLatch(tasks.size());
        List<Thread> threads = new ArrayList<>();
        for (TestRunnable task : tasks) {
            Thread thread = new Thread(() -> {
                cdl.countDown();
                try {
                    cdl.await();
                    task.run();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            });
            thread.start();
            threads.add(thread);
        }
        for (Thread thread : threads) {
            try {
                thread.join(timeoutMillis + startTime - System.currentTimeMillis() + 300L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (System.currentTimeMillis() - startTime >= timeoutMillis) {
                break;
            }
        }

        for (TestRunnable task : tasks) {
            if (!task.isDone()) {
                for (Thread thread : threads) {
                    if (thread.isAlive()) {
                        thread.interrupt();
                    }
                }
                throw new IllegalStateException("线程任务未完成");
            }
        }
    }


    /**
     * <P> 创建一个用于测试的表模式，前 numsInt 的字段类型为 int，然后是 double，最后是 string</P>
     * <p>字段的名称被设置为他的下标，也可以由参数 Func 指定</p>
     * <P>索引字段被设置为 由参数 indexF 决定，它接受下标并返回字段索引类型</P>
     */
    public static TableDesc createTableDesc(int numsInt, int numsDouble, int numsString, String tableName,
                                            Function<Integer, Integer> indexF, Function<Integer, String> nameF) {
        int n = numsInt + numsDouble + numsString;
        Type[] types = new Type[n];
        String[] names = new String[n];
        int[] indexes = new int[n];
        for (int i = 0; i < n; i++) {
            if (i < numsInt) {
                types[i] = Type.INT_TYPE;
            } else if (i < numsInt + numsDouble) {
                types[i] = Type.DOUBLE_TYPE;
            } else {
                types[i] = Type.STRING_TYPE;
            }

            names[i] = String.valueOf(i);
            if (nameF != null) {
                names[i] = nameF.apply(i);
            }
            indexes[i] = indexF == null ? 0 : indexF.apply(i);
        }
        return new TableDesc(tableName, names, types, indexes);
    }

    public static TableDesc createTableDesc(int numsInt, int numsDouble, int numsString, String tableName,
                                            Function<Integer, Integer> indexF) {
        return createTableDesc(numsInt, numsDouble, numsString, tableName, indexF, null);
    }

    public static TableDesc createTableDesc(int numsInt, int numsDouble, int numsString, String tableName) {
        return createTableDesc(numsInt, numsDouble, numsString, tableName,  null);
    }

    /**
     * <P> 创建一个用于测试的记录，前 numsInt 的字段类型为 int，然后是 double，最后是 string</P>
     * <p>记录的字段值随机设置，有效位设置为 true，其余隐藏字段为默认值</p>
     */
    public static Record createRecord(int numsInt, int numsDouble, int numsString, String tableName) {
        Record record = new Record(createTableDesc(numsInt, numsDouble, numsString, tableName, null));
        IntField[] i1 = (IntField[]) randomArray(numsInt, 0);
        DoubleField[] i2 = (DoubleField[]) randomArray(numsDouble, 1);
        StringField[] i3 = (StringField[]) randomArray(numsString, 2);
        int i = 0;
        for (IntField intField : i1) {
            record.setField(i++, intField);
        }
        for (DoubleField doubleField : i2) {
            record.setField(i++, doubleField);
        }
        for (StringField stringField : i3) {
            record.setField(i++, stringField);
        }
        record.setValid(true);
        return record;
    }


    /**
     * 创建一些由随机元素组成的数组
     *
     * @param nums 数组长度
     * @param type 数组类型，0、1、2 是 Intfield[]、Doublefield[]、Stringfield[]
     * @return 数组
     */
    public static Object randomArray(int nums, int type) {
        Random random = new Random();
        return switch (type) {
            case 0 -> {
                IntField[] ans = new IntField[nums];
                for (int i = 0; i < nums; i++) {
                    ans[i] = new IntField(random.nextInt());
                }
                yield ans;
            }
            case 1 -> {
                DoubleField[] ans = new DoubleField[nums];
                for (int i = 0; i < nums; i++) {
                    ans[i] = new DoubleField(random.nextDouble());
                }
                yield ans;
            }
            case 2 -> {
                StringField[] ans = new StringField[nums];
                for (int i = 0; i < nums; i++) {
                    ans[i] = new StringField(UUID.randomUUID().toString());
                }
                yield ans;
            }
            default -> throw new IllegalStateException("Unexpected value: " + type);
        };
    }


    public static void assertRecordEquals(Record record1, Record record2, boolean compareHidden) {
        if (record1 == null && record2 == null)
            return;
        assert record1 != null && record2 != null;
        Assert.assertEquals(record1.getNumFields(), record2.getNumFields());
        for (int i = 0; i < record1.getNumFields(); i++) {
            Assert.assertEquals(record1.getField(i), record2.getField(i));
        }
        if (compareHidden) {
            Assert.assertEquals(record1.isValid(), record2.isValid());
            Assert.assertEquals(record1.getLogPointer(), record2.getLogPointer());
        }
    }


    public static void assertTableDescEquals(TableDesc t1, TableDesc t2,
                                             boolean compareFieldName, boolean compareIndexType, boolean compareTableName) {
        Assert.assertEquals(t1.numFields(), t2.numFields());
        for (int i = 0; i < t1.numFields(); i++) {
            Assert.assertEquals(t1.getFieldType(i), t2.getFieldType(i));

            if (compareFieldName) {
                Assert.assertEquals(t1.getFieldName(i), t2.getFieldName(i));
            }
            if (compareIndexType) {
                Assert.assertEquals(t1.getIndexType(i), t2.getIndexType(i));
            }
            if (compareTableName) {
                Assert.assertEquals(t1.getTableName(), t2.getTableName());
            }
        }
    }

    /**
     * 创建一个所有列都是 INT 并且元素相等的记录
     */
    public static Record createRecord(int cols, int val, String table) {
        Record record = createRecord(cols, 0, 0, table);
        for (int i = 0; i < cols; i++) {
            record.setField(i, new IntField(val));
        }
        return record;
    }

    /**
     * 创建一个表，插入行记录并构建索引，它具有 cols 列，每一列都是 int 类型，第 0 列作为主键，字段名为它的下标。
     * <P>所有的列的值从 startVal 开始自增 delta</P>
     * <P>事务设置为 0</P>
     */
    public static TableDesc createAndInsert(int cols, int rows, int startVal, int delta, String tableName,
                                            Function<Record, Record> f) throws Exception {
        TableDesc td = createTableDesc(cols, 0, 0, tableName,
                i -> i == 0 ? IndexType.indexSetToInt(Set.of(IndexType.PRIMARY_KEY, IndexType.BTREE, IndexType.BTREE_UNIQUE)) : 0);
        Database.getCatalog().createTable(td);

        HeapPageManager pm = (HeapPageManager) Database.getCatalog().getPageManager(tableName);
        BufferPool bufferPool = Database.getBufferPool();
        BTreeIndex index = (BTreeIndex) Database.getCatalog().getIndex(tableName, 0, IndexType.BTREE);
        for (int i = 0; i < rows; i++) {
            Record record = createRecord(cols, i * delta + startVal, tableName);
            if (f != null) {
                record = f.apply(record);
            }

            RecordId malloc = pm.malloc();

            HeapPage page = (HeapPage) bufferPool.getPage(new TransactionId(0), malloc.getPid(), Permissions.READ_ONLY);
            page.insertRecord(malloc, record);

            index.insert(new TransactionId(0), record.getField(0), malloc);
        }
        return td;
    }

    /**
     * 创建一个简单的表并插入元组，构建主键索引，此表只有三列：INT、DOUBLE、STRING，其中 INT 列为主键列
     * <P>他们的列名被设置为 x, y, z</P>
     * @param f 对待插入 record 应用的函数，record 主键被设置为行数下标
     * @throws Exception
     */
    public static TableDesc createSimpleAndInsert(int rows, String tableName, Function<Record, Record> f) throws Exception {
        TableDesc td = createTableDesc(1, 1, 1, tableName,
                i -> i == 0 ? IndexType.indexSetToInt(Set.of(IndexType.PRIMARY_KEY, IndexType.BTREE, IndexType.BTREE_UNIQUE)) : 0,
                i -> {
                    if (i == 0)
                        return "x";
                    if (i == 1)
                        return "y";
                    if (i == 2)
                        return "z";
                    return null;
                });
        Database.getCatalog().createTable(td);

        HeapPageManager pm = (HeapPageManager) Database.getCatalog().getPageManager(tableName);
        BufferPool bufferPool = Database.getBufferPool();
        BTreeIndex index = (BTreeIndex) Database.getCatalog().getIndex(tableName, 0, IndexType.BTREE);
        for (int i = 0; i < rows; i++) {
            Record record = createRecord(1, 1, 1, tableName);
            record.setField(0, new IntField(i));
            record.setField(1, new DoubleField(i));
            record.setField(2, new StringField(String.valueOf(i)));
            if (f != null) {
                record = f.apply(record);
            }

            RecordId malloc = pm.malloc();

            HeapPage page = (HeapPage) bufferPool.getPage(new TransactionId(0), malloc.getPid(), Permissions.READ_ONLY);
            page.insertRecord(malloc, record);

            index.insert(new TransactionId(0), record.getField(0), malloc);
        }
        Database.getBufferPool().transactionReleaseLock(new TransactionId(0));
        return td;
    }


    public static Record insertAndRunLog(int val, RecordId recordId, TransactionId tid) throws Exception {
        TableDesc td = Database.getCatalog().getTableDesc(recordId.getPid().getTableName());
        Record record = new Record(td);
        record.setRecordId(recordId);

        record.setField(0, new IntField(val));
        record.setField(1, new DoubleField(val));
        record.setField(2, new StringField("abc"));
        record.setValid(true);
        record.setLastModify(tid);

        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, recordId.getPid(), Permissions.READ_ONLY);
        page.insertRecord(recordId, record);

        Database.getCatalog().getIndex(recordId.getPid().getTableName(), td.getPrimaryKeyFieldIndex(), IndexType.BTREE)
                .insert(tid, new IntField(val), recordId);

        UndoLog undoLog = Database.getLogBuffer().createInsertUndoLog(tid, record);
        record.setLogPointer(undoLog.getId());

        Database.getLogBuffer().createInsertRedoLog(tid, record);



        return record;
    }

    public static void updateAndRunLog(Record oldRecord, Record newRecord, TransactionId tid) throws DbException {
        UndoLog undoLog = Database.getLogBuffer().createUpdateUndoLog(tid, oldRecord);

        newRecord.setLastModify(tid);
        newRecord.setLogPointer(undoLog.getId());
        HeapPage page = (HeapPage) Database.getBufferPool()
                .getPage(tid, newRecord.getRecordId().getPid(), Permissions.READ_ONLY);
        page.updateRecord(newRecord.getRecordId(), newRecord);

        Database.getLogBuffer().createUpdateRedoLog(tid, newRecord);
    }


    public static void deleteAndRunLog(Record delRecord,  TransactionId tid) throws DbException {
        UndoLog undoLog = Database.getLogBuffer().createDeleteUndoLog(tid, delRecord);

        delRecord.setLastModify(tid);
        delRecord.setLogPointer(undoLog.getId());
        HeapPage page = (HeapPage) Database.getBufferPool()
                .getPage(tid, delRecord.getRecordId().getPid(), Permissions.READ_ONLY);
        page.updateRecord(delRecord.getRecordId(), delRecord);

        Database.getLogBuffer().createDeleteRedoLog(tid, delRecord.getRecordId());
    }


    public static Record[]  getRecordAr(String tb, TransactionId tid) throws DbException {
        BTreeSeqScan scan = new BTreeSeqScan(tid, tb, null, null);
        scan.open();
        return scan.getRecordAr();
    }
}
