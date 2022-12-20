package happydb.replication;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.execution.SQLExecutor;
import happydb.storage.DoubleField;
import happydb.storage.Record;
import happydb.transaction.TransactionId;
import happydb.transaction.TransactionManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @Author happysnaker
 * @Date 2022/12/11
 * @Email happysnaker@foxmail.com
 */
public class RaftLogManagerTest extends TestBase {

    LogEntry log1 = new LogEntry();
    LogEntry log2 = new LogEntry();
    LogEntry log3 = new LogEntry();

    @Before
    public void setUp() throws Exception {
        Database.enableReplication = true;
        Database.reset();
        String create = """
                CREATE TABLE `tb` (
                	x int,
                    y double,
                    z char,
                    PRIMARY KEY(x) USING BTREE
                )""";
        log1.setSqlList(List.of(create));
        log2.setSqlList(List.of("insert into tb values(1, 2, abc);", "insert into tb values(2, 3, hello)"));
        log3.setSqlList(List.of("update tb set y = 1", "update tb set y = y + x"));
    }

    @Test
    public void testReadWrite() throws Exception {
        RaftLogManager lm = Database.getRaftLogManager();

        RaftLogManager.HOLDER = 1;


        lm.appendLogEntry(log1);
        lm.appendLogEntry(log2);
        lm.appendLogEntry(log3);

        Assert.assertEquals(log1, lm.getLogEntry(0));
        Assert.assertEquals(log2, lm.getLogEntry(1));
        Assert.assertEquals(log3, lm.getLogEntry(2));

        Database.reset();
        Assert.assertEquals(2, lm.getMaxIndex());
        Assert.assertEquals(log3, lm.getLastLogEntry());
    }


    @Test
    public void testCommit() throws Exception {
        RaftLogManager lm = Database.getRaftLogManager();
        TransactionManager tm = Database.getTransactionManager();

        TransactionId begin = tm.begin();
        log1.setXid(begin.getXid());
        SQLExecutor.createTable(begin.getXid(), log1.getSqlList().get(0));
        RaftLogManager.HOLDER = 1;

        lm.appendLogEntry(log1);
        lm.appendLogEntry(log2);
        lm.appendLogEntry(log3);

        lm.setCommitIndex(1);
        lm.commit();

        Record[] recordAr = TestUtil.getRecordAr("tb", new TransactionId(-1));
        Assert.assertEquals(2, recordAr.length);

        lm.setCommitIndex(121);
        lm.commit();
        recordAr = TestUtil.getRecordAr("tb", new TransactionId(-1));
        Assert.assertEquals(2, recordAr.length);
        Assert.assertEquals(new DoubleField(2), recordAr[0].getField(1));
        Assert.assertEquals(new DoubleField(3), recordAr[1].getField(1));
    }
}
