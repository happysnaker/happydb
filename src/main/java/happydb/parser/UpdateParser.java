package happydb.parser;

import happydb.common.Database;
import happydb.exception.ParseException;
import happydb.execution.Project;
import happydb.execution.UpdateOperateScan;
import happydb.index.IndexType;
import happydb.optimizer.LogicalPlan;
import happydb.storage.TableDesc;
import happydb.transaction.TransactionId;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.insert.Insert;
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
public class UpdateParser {


    public static Project parserUpdate(net.sf.jsqlparser.statement.update.Update update, TransactionId tid) throws ParseException {
        Table table = update.getTable();

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
        QueryParser.processWhereExpression(lp, update.getWhere(), true);

        // 解析 Set
        ArrayList<UpdateSet> updateSets = update.getUpdateSets();
        if (updateSets == null)     throw new ParseException("Not a set statement.");
        if (updateSets.size() > 1)  throw new ParseException("Only support one set statement");

        UpdateSet us = updateSets.get(0);
        String column = us.getColumns().get(0).getColumnName();
        int updateField;
        try {
            updateField = td.fieldNameToIndex(column);
        } catch (NoSuchElementException e) {
            throw new ParseException("No such field named " + column);
        }

        if (!IndexType.intToIndexSet(td.getIndexType(updateField)).isEmpty()) {
            throw new ParseException("Can not update index field " + column);
        }

        Expression expression = us.getExpressions().get(0);
        String expr = null;
        if (expression instanceof StringValue c) {
            expr = c.getValue();
        } else {
            expr = expression.toString();
        }

        UpdateOperateScan scan = new UpdateOperateScan(tid, lp);

        happydb.execution.Update child = new happydb.execution.Update(updateField, expr, scan, tid);

        return new Project(child);
    }
}
