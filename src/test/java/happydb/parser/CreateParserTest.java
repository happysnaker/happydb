package happydb.parser;

import happydb.TestBase;
import happydb.common.Database;
import happydb.common.Debug;
import happydb.exception.DbException;
import happydb.exception.ParseException;
import happydb.execution.OpIterator;
import happydb.index.IndexType;
import happydb.storage.Record;
import happydb.storage.TableDesc;
import happydb.transaction.TransactionId;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

import static happydb.storage.Type.*;
import static org.junit.Assert.fail;

/**
 * @Author happysnaker
 * @Date 2022/12/4
 * @Email happysnaker@foxmail.com
 */
public class CreateParserTest extends TestBase {
    @Test
    public void testCreateTable() throws JSQLParserException, ParseException, DbException {
        String sql = """
                CREATE TABLE `tb` (
                	x int,
                    y double,
                    z char,
                    PRIMARY KEY(x) USING BTREE,
                    KEY `y_index_hash` (y),
                    UNIQUE KEY(z) USING BTREE
                )""";
        Parser.parser(sql, new TransactionId(-1));

        TableDesc td = Database.getCatalog().getTableDesc("tb");
        Assert.assertEquals("x", td.getFieldName(0));
        Assert.assertEquals(INT_TYPE, td.getFieldType(0));
        Assert.assertEquals("y", td.getFieldName(1));
        Assert.assertEquals(DOUBLE_TYPE, td.getFieldType(1));
        Assert.assertEquals("z", td.getFieldName(2));
        Assert.assertEquals(STRING_TYPE, td.getFieldType(2));

        Assert.assertEquals(0, td.getPrimaryKeyFieldIndex());
        Assert.assertEquals(IndexType.indexSetToInt(Set.of(IndexType.BTREE)), td.getIndexType(1));
        Assert.assertEquals(IndexType.indexSetToInt(Set.of(IndexType.BTREE, IndexType.BTREE_UNIQUE)), td.getIndexType(2));


        OpIterator insert = Parser.parser("INSERT INTO `tb` VALUES(0, 2.5, 'Hello World!')", new TransactionId(0));
        Assert.assertNotNull(insert);
        insert.open();
        System.out.println("insert.next() = " + insert.next());



        try {
            insert = Parser.parser("INSERT INTO `tb` VALUES(0, 2.5, 'Hello World!')", new TransactionId(0));
            insert.open();
            fail();
        } catch (DbException e) {
            // duplicate insert
        }

        OpIterator query = Parser.parser("SELECT * FROM tb;", new TransactionId(0));
        Assert.assertNotNull(query);
        query.open();
        Record[] recordAr = query.getRecordAr();
        Assert.assertEquals(1, recordAr.length);
        Debug.log(Arrays.toString(recordAr));
    }
}

