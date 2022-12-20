package happydb.parser;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.exception.DbException;
import happydb.exception.ParseException;
import happydb.execution.OpIterator;
import happydb.storage.DoubleField;
import happydb.storage.IntField;
import happydb.storage.Record;
import happydb.transaction.TransactionId;
import happydb.transaction.TransactionManager;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author happysnaker
 * @Date 2022/12/5
 * @Email happysnaker@foxmail.com
 */
public class UpdateParserTest extends TestBase {

    @Before
    public void setUp() throws Exception {
        TestUtil.createSimpleAndInsert(3, "tb", null);
    }

    @Test
    public void condition1() throws JSQLParserException, ParseException, DbException {
        String sql = "UPDATE tb SET y = 1.0 WHERE y != 1.0";

        OpIterator update = Parser.parser(sql, new TransactionId(0));
        Assert.assertNotNull(update);
        update.open();

        Record[] recordAr = TestUtil.getRecordAr("tb", new TransactionId(0));
        for (Record record : recordAr) {
            Assert.assertEquals(new DoubleField(1.0), record.getField(1));
        }
    }


    @Test(timeout = 3000L)
    public void condition2() throws Exception {
        TransactionManager tm = Database.getTransactionManager();
        String sql = "UPDATE tb SET y = 1.0 WHERE y != 1.0";

        TransactionId A = tm.begin();
        TransactionId B = tm.begin();

        OpIterator update = Parser.parser(sql, A);
        Assert.assertNotNull(update);
        update.open();

        AtomicBoolean ok = new AtomicBoolean(true);
        Thread thread = new Thread(() -> {
            OpIterator u = null;
            try {
                u = Parser.parser(sql, A);
            } catch (JSQLParserException | DbException | ParseException e) {
                throw new RuntimeException(e);
            }
            try {
                assert u != null;
                u.open();
                if (!u.next().getField(0).equals(new IntField(0))) {
                    ok.set(false);
                }
            } catch (DbException e) {
                throw new RuntimeException(e);
            }
        });

        thread.start();
        tm.commit(A, false);
        thread.join();
    }


    @Test
    public void condition3() throws Exception {
        TransactionManager tm = Database.getTransactionManager();
        TransactionId begin = tm.begin();
        String sql = "UPDATE tb SET y = 1.0 WHERE y = 0 OR y = 1";

        OpIterator update = Parser.parser(sql, begin);
        Assert.assertNotNull(update);
        update.open();

        Record[] recordAr = TestUtil.getRecordAr("tb", new TransactionId(0));
        for (int i = 0; i < recordAr.length - 1; i++) {
            Record record = recordAr[i];
            Assert.assertEquals(new DoubleField(1.0), record.getField(1));
        }

        tm.rollback(begin);
        recordAr = TestUtil.getRecordAr("tb", new TransactionId(0));
        for (int i = 0; i < recordAr.length ; i++) {
            Record record = recordAr[i];
            Assert.assertEquals(new DoubleField(i), record.getField(1));
        }
    }


    @Test
    public void condition4() throws Exception {
        TransactionManager tm = Database.getTransactionManager();
        TransactionId begin = tm.begin();
        String sql = "UPDATE tb SET y = y + tb.x WHERE y = 0 OR y = 1";

        OpIterator update = Parser.parser(sql, begin);
        Assert.assertNotNull(update);
        update.open();

        Record[] recordAr = TestUtil.getRecordAr("tb", new TransactionId(0));
        for (int i = 0; i < recordAr.length - 1; i++) {
            Record record = recordAr[i];
            Assert.assertEquals(new DoubleField(i + i), record.getField(1));
        }

        tm.rollback(begin);
        recordAr = TestUtil.getRecordAr("tb", new TransactionId(0));
        for (int i = 0; i < recordAr.length ; i++) {
            Record record = recordAr[i];
            Assert.assertEquals(new DoubleField(i), record.getField(1));
        }
    }
}
