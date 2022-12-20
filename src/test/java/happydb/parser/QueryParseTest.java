package happydb.parser;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.common.Debug;
import happydb.exception.DbException;
import happydb.exception.ParseException;
import happydb.execution.BTreeSeqScan;
import happydb.execution.OpIterator;
import happydb.execution.Predicate;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.TransactionId;
import net.sf.jsqlparser.JSQLParserException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author happysnaker
 * @Date 2022/11/28
 * @Email happysnaker@foxmail.com
 */
public class QueryParseTest extends TestBase {
    @Before
    public void setUp() throws Exception {
        BufferPool.DEFAULT_PAGES = 1000;
        BufferPool.resetPageSize();
        Database.reset();

        TestUtil.createSimpleAndInsert(100, "a", r -> {
            r.setField(1, new DoubleField(((int) r.getField(0).getObject())));
            r.setField(2, new StringField((int) r.getField(0).getObject() % 10 + "abc"));
            return r;
        });
    }

    @Test
    public void testNormalQuery() throws JSQLParserException, ParseException, DbException {
        OpIterator iterator = Parser.parser(
                "SELECT a.x AS x, * FROM a WHERE a.x <= 10 AND a.y < 5.0 OR a.x >= 90 ORDER BY x;",
                new TransactionId(0));

        assert iterator != null;
        iterator.open();

        Record[] recordAr = iterator.getRecordAr();
        Assert.assertEquals(15, recordAr.length);
        for (int i = 0; i < 15; i++) {
            Debug.log(recordAr[i].toString());
            if (i < 5)
                Assert.assertEquals(new IntField(i), recordAr[i].getField(0));
            else
                Assert.assertEquals(new IntField(i - 5 + 90), recordAr[i].getField(0));
        }
        TableDesc tableDesc = iterator.getTableDesc();
        Assert.assertEquals("x", tableDesc.getFieldName(0));
        Assert.assertEquals("a.x", tableDesc.getFieldName(1));
    }


    @Test
    public void testAggAndGroupByQuery() throws JSQLParserException, ParseException, DbException {
        OpIterator iterator = Parser.parser(
                "SELECT z, SUM(x), AVG(x) as avg_x FROM a WHERE a.z != '0abc' GROUP BY z ORDER BY z DESC;",
                new TransactionId(0));

        assert iterator != null;
        iterator.open();

        Record[] recordAr = iterator.getRecordAr();
        int s = 9;
        for (Record record : recordAr) {
            Debug.log(record);
            Assert.assertEquals(s + "abc", record.getField(0).getObject());
            s--;
        }

        Assert.assertEquals(9, recordAr.length);
        Assert.assertEquals(new DoubleField(54), recordAr[0].getField(2));
        Assert.assertEquals(new DoubleField(46), recordAr[8].getField(2));

        TableDesc tableDesc = iterator.getTableDesc();
        Assert.assertEquals("z", tableDesc.getFieldName(0));
        Assert.assertEquals("avg_x", tableDesc.getFieldName(2));
        Assert.assertEquals(Type.DOUBLE_TYPE, tableDesc.getFieldType(2));
    }

    /**
     * 错误的查询优化可能使得此方法超时
     */
    @Test(timeout = 1000 * 15)
    public void testJoinQuery() throws Exception {
        TestUtil.createSimpleAndInsert(200, "b", null);
        TestUtil.createSimpleAndInsert(300, "c", null);
        TestUtil.createSimpleAndInsert(400, "d", null);

        OpIterator iterator = Parser.parser(
                "SELECT a.x, b.x, c.x, d.x FROM a, b, c, d WHERE b.x >= c.x AND c.x <= d.x AND a.x = b.x AND b.y >= 50",
                new TransactionId(0));

        assert iterator != null;
        iterator.open();
        Record[] recordAr = iterator.getRecordAr();

        for (int i = 0; i < 10; i++) {
            Debug.log(recordAr[i]); // 打印一些验证
        }

        // 10147500 1737900
        Assert.assertEquals(1364175, recordAr.length);
    }

    // 测试索引
    /**
     * 仅仅只是测试一下使用索引查询而不使用索引查询的速度
     */
    @Test
    public void testIndexQuery() throws Exception {
        Debug.debug = false;
        BufferPool.DEFAULT_PAGES = 10000;
        Database.reset();

        System.out.println("\n\n\n");

        int rows = 100000;
        System.out.println("Create " + rows + " rows in table..");
        long startTime = System.currentTimeMillis();
        TestUtil.createSimpleAndInsert(rows, "tb", null);
        System.out.println("Create table cost " + (System.currentTimeMillis() - startTime) + "ms");

        System.out.println("\n");
        Database.getBufferPool().flushAllPages();
        Database.getBufferPool().pagePool.clear();

        BTreeSeqScan scan = new BTreeSeqScan(new TransactionId(0), "tb", null, null);
        System.out.println("测试全表扫描用时..");
        startTime = System.currentTimeMillis();
        scan.open();
        scan.getRecordAr();
        System.out.println("用时 " + (System.currentTimeMillis() - startTime) + "ms");


        System.out.println("\n");
        Database.getBufferPool().flushAllPages();
        Database.getBufferPool().pagePool.clear();

        scan = new BTreeSeqScan(new TransactionId(0), "tb", null,
                new Predicate(0, Predicate.Op.LESS_THAN, new IntField(rows / 2)));
        System.out.println("测试索引列下的范围扫描，范围为开头到中值..");
        startTime = System.currentTimeMillis();
        scan.open();
        scan.getRecordAr();
        System.out.println("用时 " + (System.currentTimeMillis() - startTime) + "ms");


        System.out.println("\n");
        Database.getBufferPool().flushAllPages();
        Database.getBufferPool().pagePool.clear();

        scan = new BTreeSeqScan(new TransactionId(0), "tb", null,
                new Predicate(0, Predicate.Op.EQUALS, new IntField(rows / 2)));
        System.out.println("测试索引列下的等值扫描，等值为中值..");
        startTime = System.currentTimeMillis();
        scan.open();
        scan.getRecordAr();
        System.out.println("用时 " + (System.currentTimeMillis() - startTime) + "ms");
    }


    // 计算 testJoinQuery 的总行数
    public static void main(String[] args) {
        List<Integer> a = new ArrayList<>();
        List<Integer> b = new ArrayList<>();
        List<Integer> c = new ArrayList<>();
        List<Integer> d = new ArrayList<>();
        fill(a, 50, 50);
        fill(b, 50, 50);
        fill(c, 0, 300);
        fill(d, 0, 400);

        List<Integer> nb = new ArrayList<>();
        List<Integer> nc = new ArrayList<>();
        for (Integer x : b) {
            for (Integer y : c) {
                if (x >= y) {
                    nb.add(x);
                    nc.add(y);
                }
            }
        }
        b = new ArrayList<>(nb);
        c = new ArrayList<>(nc);

        List<Integer> nd = new ArrayList<>();
        nc = new ArrayList<>();
        for (Integer x : c) {
            for (Integer y : d) {
                if (x <= y) {
                    nc.add(x);
                    nd.add(y);
                }
            }
        }

        System.out.println("nc.size() = " + nc.size());
    }

    public static void fill(List<Integer> l, int start, int len) {
        for (int i = 0; i < len; i++) {
            l.add(i + start);
        }
    }
}
