package happydb.parser;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.execution.OpIterator;
import happydb.storage.BufferPool;
import happydb.storage.DoubleField;
import happydb.storage.Record;
import happydb.transaction.TransactionId;
import happydb.transaction.TransactionManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author happysnaker
 * @Date 2022/12/5
 * @Email happysnaker@foxmail.com
 */
public class ParserTest extends TestBase {

    @Before
    public void setUp() throws Exception {
        BufferPool.DEFAULT_PAGES = 1000;
        Database.reset();
    }

    @Test
    public void test() throws Exception {
        String sql = """
                CREATE TABLE `tb` (
                	x int,
                    y double,
                    z char,
                    PRIMARY KEY(x) USING BTREE,
                    KEY `y_normal_btree_index` (y)
                )""";
        Parser.parser(sql, new TransactionId(-1));

        TransactionManager tm = Database.getTransactionManager();
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        int n = 1024;
        for (int i = 0; i < n; i++) {
            int finalI = i * 5;
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    TransactionId tid = tm.begin();

                    StringBuilder insert = new StringBuilder("INSERT INTO tb(x, y, z) VALUES ");
                    for (int i1 = 0; i1 < 5; i1++) {
                        insert.append(String.format("(%d, %d, %s),", finalI + i1, finalI + i1, finalI + i1));
                    }

                    String substring = insert.substring(0, insert.length() - 1);
                    OpIterator opInsert = Parser.parser(substring, tid);
                    assert opInsert != null;
                    opInsert.open();

                    OpIterator opQuery = Parser.parser(
                            "SELECT * FROM tb WHERE x >= " + finalI + " AND x <= " + (finalI + 4), tid);
                    assert opQuery != null;
                    opQuery.open();

                    Assert.assertEquals(5, opQuery.getRecordAr().length);

                    tm.commit(tid, false);
                    setDone(true);
                }
            });
        }
        System.out.println("Start many thread insert and select.");
        TestUtil.runManyThread(tasks, 1000 * 60L * 3);

        OpIterator query = Parser.parser("SELECT * FROM tb;", tm.begin());

        assert query != null;
        query.open();
        Assert.assertEquals(n * 5, query.getRecordAr().length);

        tasks.clear();
        for (int i = 0; i < n; i++) {
            int finalI = i * 5;
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    TransactionId tid = tm.begin();

                    String update = "UPDATE tb SET y = 1024 WHERE x >= "+ finalI + " AND x <= " + (finalI + 4);
                    OpIterator opUpdate = Parser.parser(update, tid);
                    assert opUpdate != null;
                    opUpdate.open();

                    OpIterator opQuery = Parser.parser(
                            "SELECT * FROM tb WHERE x >= " + finalI + " AND x <= " + (finalI + 4), tid);
                    assert opQuery != null;
                    opQuery.open();

                    Record[] recordAr = opQuery.getRecordAr();
                    Assert.assertEquals(5, recordAr.length);
                    for (Record record : recordAr) {
                        Assert.assertEquals(new DoubleField(1024), record.getField(1));
                    }
                    tm.commit(tid, false);
                    setDone(true);
                }
            });
        }

        System.out.println("Start many thread update and select.");
        TestUtil.runManyThread(tasks, 1000 * 60L * 3);

        query = Parser.parser("SELECT * FROM tb;", tm.begin());

        assert query != null;
        query.open();
        Record[] recordAr = query.getRecordAr();
        Assert.assertEquals(n * 5, recordAr.length);
        for (Record record : recordAr) {
            Assert.assertEquals(new DoubleField(1024), record.getField(1));
        }


        tasks.clear();
        for (int i = 0; i < n; i++) {
            int finalI = i;
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    TransactionId tid = tm.begin();

                    String update = "UPDATE tb SET y = y + 1 WHERE x >= 0 AND x <= 4";
                    OpIterator opUpdate = Parser.parser(update, tid);
                    assert opUpdate != null;
                    opUpdate.open();
                    tm.commit(tid, false);
                    setDone(true);
                }
            });
        }

        System.out.println("Start many thread update the same record.");
        TestUtil.runManyThread(tasks, 1000 * 60L * 3);

        query = Parser.parser("SELECT * FROM tb WHERE x >= 0 AND x <= 4;", tm.begin());

        assert query != null;
        query.open();
        recordAr = query.getRecordAr();
        Assert.assertEquals(5, recordAr.length);
        for (Record record : recordAr) {
            Assert.assertEquals(new DoubleField(1024 + n), record.getField(1));
        }


        tasks.clear();
        for (int i = 0; i < n; i++) {
            int finalI = i * 5;
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    TransactionId tid = tm.begin();

                    String delete = "DELETE FROM tb WHERE x >= "+ finalI + " AND x <= " + (finalI + 4);
                    OpIterator opUpdate = Parser.parser(delete, tid);
                    assert opUpdate != null;
                    opUpdate.open();

                    OpIterator opQuery = Parser.parser(
                            "SELECT * FROM tb WHERE x >= " + finalI + " AND x <= " + (finalI + 4), tid);
                    assert opQuery != null;
                    opQuery.open();

                    Record[] recordAr = opQuery.getRecordAr();
                    Assert.assertEquals(0, recordAr.length);
                    tm.commit(tid, false);
                    setDone(true);
                }
            });
        }

        System.out.println("Start many thread delete and select.");
        TestUtil.runManyThread(tasks, 1000 * 60L * 3);

        query = Parser.parser("SELECT * FROM tb;", tm.begin());

        assert query != null;
        query.open();
        Assert.assertEquals(0, query.getRecordAr().length);

        System.out.println("Successfully pass test!");
    }
}
