package happydb.execution;

import happydb.common.Database;
import happydb.exception.DbException;
import happydb.exception.ParseException;
import happydb.parser.Parser;
import happydb.transaction.TransactionId;
import net.sf.jsqlparser.JSQLParserException;

import java.io.IOException;

/**
 * 集中式的 SQL 调用处理程序
 *
 * @Author happysnaker
 * @Date 2022/12/6
 * @Email happysnaker@foxmail.com
 */
public class SQLExecutor {


    /**
     * 开启一个事务
     *
     * @return 事务 ID
     */
    public static long begin() throws IOException {
        return Database.getTransactionManager().begin().getXid();
    }

    public static void createTable(long xid, String sql) throws JSQLParserException, ParseException, DbException, IOException {
        Parser.parser(sql, new TransactionId(xid));
        if (Database.enableReplication) {
            Database.getRaftLogManager().addSql(new TransactionId(xid), sql);
        }
    }

    public static int insert(long xid, String sql) throws JSQLParserException, ParseException, DbException {
        OpIterator parser = Parser.parser(sql, new TransactionId(xid));
        assert parser != null;
        parser.open();
        if (Database.enableReplication) {
            Database.getRaftLogManager().addSql(new TransactionId(xid), sql);
        }
        return (int) parser.next().getField(0).getObject();
    }

    public static int delete(long xid, String sql) throws JSQLParserException, ParseException, DbException {
        OpIterator parser = Parser.parser(sql, new TransactionId(xid));
        assert parser != null;
        parser.open();
        if (Database.enableReplication) {
            Database.getRaftLogManager().addSql(new TransactionId(xid), sql);
        }
        return (int) parser.next().getField(0).getObject();
    }

    public static int update(long xid, String sql) throws JSQLParserException, ParseException, DbException {
        OpIterator parser = Parser.parser(sql, new TransactionId(xid));
        assert parser != null;
        parser.open();
        if (Database.enableReplication) {
            Database.getRaftLogManager().addSql(new TransactionId(xid), sql);
        }
        return (int) parser.next().getField(0).getObject();
    }

    public static OpIterator query(long xid, String sql) throws JSQLParserException, ParseException, DbException {
        OpIterator parser = Parser.parser(sql, new TransactionId(xid));
        if (Database.enableReplication) {
            Database.getRaftLogManager().addSql(new TransactionId(xid), sql);
        }
        return parser;
    }

    public static void commit(long xid) throws IOException, DbException {
        Database.getTransactionManager().commit(new TransactionId(xid), Database.enableReplication);
    }

    public static void rollback(long xid) throws IOException, DbException {
        Database.getTransactionManager().rollback(new TransactionId(xid));
        if (Database.enableReplication) {
            Database.getRaftLogManager().sqlMap.remove(new TransactionId(xid));
        }
    }
}
