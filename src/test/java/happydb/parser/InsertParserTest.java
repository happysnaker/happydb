package happydb.parser;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.common.Debug;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.execution.OpIterator;
import happydb.execution.Project;
import happydb.log.UndoLog;
import happydb.log.UndoLogId;
import happydb.log.UndoLogPage;
import happydb.storage.DoubleField;
import happydb.storage.IntField;
import happydb.storage.Record;
import happydb.storage.StringField;
import happydb.transaction.TransactionId;
import happydb.transaction.TransactionManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.fail;

/**
 * @Author happysnaker
 * @Date 2022/12/4
 * @Email happysnaker@foxmail.com
 */
public class InsertParserTest extends TestBase {

    @Before
    public void setUp() throws Exception {
        TestUtil.createSimpleAndInsert(0, "tb", null);
    }

    public void assertUndo(Record record, int leaf) throws DbException {
        UndoLogId id = record.getLogPointer();
        UndoLogPage page = (UndoLogPage) Database.getBufferPool()
                .getPage(new TransactionId(0), id.pid(), Permissions.READ_ONLY);
        UndoLog undoLog = page.readUndoLog(id);
        undoLog.undo();
        Assert.assertEquals(leaf, TestUtil.getRecordAr("tb", new TransactionId(0)).length);
    }

    @Test
    public void condition1() throws Exception {
        Project project = (Project) Parser.parser("INSERT INTO tb VALUES(1, 2.3, 'I Love You!')",
                new TransactionId(0));
        Assert.assertNotNull(project);

        project.open();
        Debug.log(Arrays.toString(project.getRecordAr()));

        Record record = TestUtil.getRecordAr("tb", new TransactionId(0))[0];
        Debug.log(record);
        Assert.assertEquals(new IntField(1), record.getField(0));
        Assert.assertEquals(new DoubleField(2.3), record.getField(1));
        Assert.assertEquals(new StringField("I Love You!"), record.getField(2));

        assertUndo(record, 0);
    }

    @Test
    public void condition2() throws Exception {
        Project project = (Project) Parser.parser("INSERT INTO tb(x, y, tb.z) VALUES(1, 2.3, 'I Love You!')",
                new TransactionId(0));
        Assert.assertNotNull(project);

        project.open();
        Debug.log(Arrays.toString(project.getRecordAr()));

        Record record = TestUtil.getRecordAr("tb", new TransactionId(0))[0];
        Debug.log(record);
        Assert.assertEquals(new IntField(1), record.getField(0));
        Assert.assertEquals(new DoubleField(2.3), record.getField(1));
        Assert.assertEquals(new StringField("I Love You!"), record.getField(2));

        assertUndo(record, 0);
    }

    @Test
    public void condition3() throws Exception {
        Project project = (Project) Parser.parser("INSERT INTO tb(tb.z, tb.x, y) VALUES('I Love You!', 1, 2.3)",
                new TransactionId(0));
        Assert.assertNotNull(project);

        project.open();
        Debug.log(Arrays.toString(project.getRecordAr()));

        Record record = TestUtil.getRecordAr("tb", new TransactionId(0))[0];
        Debug.log(record);
        Assert.assertEquals(new IntField(1), record.getField(0));
        Assert.assertEquals(new DoubleField(2.3), record.getField(1));
        Assert.assertEquals(new StringField("I Love You!"), record.getField(2));

        assertUndo(record, 0);
    }


    @Test
    public void condition4() throws Exception {
        Project project = (Project) Parser.parser("INSERT INTO tb(tb.z) VALUES('I Love You!')",
                new TransactionId(0));
        Assert.assertNotNull(project);

        project.open();
        Debug.log(Arrays.toString(project.getRecordAr()));

        Record record = TestUtil.getRecordAr("tb", new TransactionId(0))[0];
        Debug.log(record);
        Assert.assertEquals(new IntField(0), record.getField(0));
        Assert.assertEquals(new DoubleField(0), record.getField(1));
        Assert.assertEquals(new StringField("I Love You!"), record.getField(2));

        assertUndo(record, 0);
    }


    @Test
    public void condition5() throws Exception {
        try {
            Project project = (Project) Parser.parser("INSERT INTO tb(tb.z) VALUES('I Love You!'), ('No')",
                    new TransactionId(0));
            Assert.assertNotNull(project);
            project.open();
            fail();
        } catch (Exception e) {
            // 重复使用主键默认值
            e.printStackTrace();
        }
    }

    @Test
    public void condition6() throws Exception {
        Project project = (Project) Parser.parser("INSERT INTO tb(tb.x, y, z) VALUES(1, 2, 'I Love You!'), (2, 3.3, '卧槽 666')",
                new TransactionId(0));
        Assert.assertNotNull(project);

        project.open();
        Debug.log(Arrays.toString(project.getRecordAr()));

        Record record = TestUtil.getRecordAr("tb", new TransactionId(0))[0];
        Debug.log(record);
        Assert.assertEquals(new IntField(1), record.getField(0));
        Assert.assertEquals(new DoubleField(2), record.getField(1));
        Assert.assertEquals(new StringField("I Love You!"), record.getField(2));


        record = TestUtil.getRecordAr("tb", new TransactionId(0))[1];
        Debug.log(record);
        Assert.assertEquals(new IntField(2), record.getField(0));
        Assert.assertEquals(new DoubleField(3.3), record.getField(1));
        Assert.assertEquals(new StringField("卧槽 666"), record.getField(2));
    }

    @Test
    public void condition7() throws Exception {
        Project project = (Project) Parser.parser("INSERT INTO tb(z, x) VALUES('I Love You!', 1), ('卧槽 666', 2)",
                new TransactionId(0));
        Assert.assertNotNull(project);

        project.open();
        Debug.log(Arrays.toString(project.getRecordAr()));

        Record record = TestUtil.getRecordAr("tb", new TransactionId(0))[0];
        Debug.log(record);
        Assert.assertEquals(new IntField(1), record.getField(0));
        Assert.assertEquals(new StringField("I Love You!"), record.getField(2));


        record = TestUtil.getRecordAr("tb", new TransactionId(0))[1];
        Debug.log(record);
        Assert.assertEquals(new IntField(2), record.getField(0));
        Assert.assertEquals(new StringField("卧槽 666"), record.getField(2));
    }


    @Test
    public void condition8() throws Exception {
        TransactionManager tm = Database.getTransactionManager();

        TransactionId A = tm.begin();
        TransactionId B = tm.begin();
        OpIterator parser = Parser.parser("INSERT INTO tb(z, x) VALUES('I Love You!', 1)", A);
        parser.open();

        Record record = TestUtil.getRecordAr("tb", A)[0];
        Record delete = record.clone();
        delete.setValid(false);
        TestUtil.deleteAndRunLog(delete, A);

        Assert.assertEquals(0, TestUtil.getRecordAr("tb", A).length);

        // B 事务应该无法插入
        try {
            parser = Parser.parser("INSERT INTO tb(z, x) VALUES('I Love You!', 1)", B);
            parser.open();

            fail();
        } catch (Exception ignore) {

        }

        tm.commit(A, false);

        parser = Parser.parser("INSERT INTO tb(z, x) VALUES('I Love You!', 1)", B);
        parser.open();
        Assert.assertEquals(1, TestUtil.getRecordAr("tb", B).length);
    }
}
