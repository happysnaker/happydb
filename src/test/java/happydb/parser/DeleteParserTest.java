package happydb.parser;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.exception.DbException;
import happydb.exception.ParseException;
import happydb.execution.OpIterator;
import happydb.storage.Record;
import happydb.transaction.TransactionId;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @Author happysnaker
 * @Date 2022/12/5
 * @Email happysnaker@foxmail.com
 */
public class DeleteParserTest extends TestBase {

    @Before
    public void setUp() throws Exception {
        TestUtil.createSimpleAndInsert(3, "tb", null);
    }

    @Test
    public void condition1() throws JSQLParserException, ParseException, DbException {
        String sql = "DELETE FROM `tb` WHERE y != 1.0";

        OpIterator delete = Parser.parser(sql, new TransactionId(0));
        Assert.assertNotNull(delete);
        delete.open();

        Record[] recordAr = TestUtil.getRecordAr("tb", new TransactionId(0));
        Assert.assertEquals(1, recordAr.length);
    }

    @Test
    public void condition2() throws JSQLParserException, ParseException, DbException {
        String sql = "DELETE FROM `tb` WHERE x = 1 AND z = 'abc'";

        OpIterator delete = Parser.parser(sql, new TransactionId(0));
        Assert.assertNotNull(delete);
        delete.open();

        Record[] recordAr = TestUtil.getRecordAr("tb", new TransactionId(0));
        Assert.assertEquals(3, recordAr.length);
    }

    @Test
    public void condition3() throws JSQLParserException, ParseException, DbException {
        String sql = "DELETE FROM `tb`";

        OpIterator delete = Parser.parser(sql, new TransactionId(0));
        Assert.assertNotNull(delete);
        delete.open();

        Record[] recordAr = TestUtil.getRecordAr("tb", new TransactionId(0));
        Assert.assertEquals(0, recordAr.length);
    }
}
