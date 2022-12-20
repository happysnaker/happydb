package happydb.parser;

import happydb.common.Database;
import happydb.exception.ParseException;
import happydb.execution.Project;
import happydb.execution.UpdateOperateScan;
import happydb.optimizer.LogicalPlan;
import happydb.storage.TableDesc;
import happydb.transaction.TransactionId;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.statement.update.UpdateSet;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * @Author happysnaker
 * @Date 2022/12/5
 * @Email happysnaker@foxmail.com
 */
public class DeleteParser {


    public static Project parserDelete(net.sf.jsqlparser.statement.delete.Delete delete, TransactionId tid) throws ParseException {
        Table table = delete.getTable();

        if (table.getAlias() != null) {
            throw new ParseException("Update statement can not using alias name");
        }

        String tableName = table.getName().replace("`", "");
        TableDesc td = null;
        try {
            td = Database.getCatalog().getTableDesc(tableName);
        } catch (NoSuchElementException e) {
            throw new ParseException("No such table " + tableName);
        }

        // 解析单表上的 where
        LogicalPlan lp = new LogicalPlan();
        lp.addTable(tableName, tableName);
        QueryParser.processWhereExpression(lp, delete.getWhere(), true);


        UpdateOperateScan scan = new UpdateOperateScan(tid, lp);
        happydb.execution.Delete child = new happydb.execution.Delete(scan, tid);
        return new Project(child);
    }

    public static void main(String[] args) throws JSQLParserException {
        String sql = "DELETE FROM `tb` WHERE tb.x >= 1";
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        net.sf.jsqlparser.statement.delete.Delete delete = (Delete) parserManager.parse(new StringReader(sql));

        System.out.println("update.getTable().getAlias().getName() = " + delete.getTable().getName());

        System.out.println("delete.getWhere() = " + delete.getWhere());
    }
}
