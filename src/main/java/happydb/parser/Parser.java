package happydb.parser;

import happydb.exception.DbException;
import happydb.exception.ParseException;
import happydb.execution.OpIterator;
import happydb.execution.Project;
import happydb.execution.Update;
import happydb.transaction.TransactionId;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.ValueListExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.parser.ASTNodeAccessImpl;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;

import java.io.StringReader;
import java.util.List;

/**
 * @Author happysnaker
 * @Date 2022/11/27
 * @Email happysnaker@foxmail.com
 */
public class Parser {

    /**
     * 解析 SQL 表达式，事务的开启与结束不被包含在内，它们可以使用 {@link String#equals(Object)} 进行解析
     *
     * @param sql sql
     * @param tid 事务，如果是
     * @return 增删改查操作会返回 {@link Project}，建表删表操作会返回 NULL
     * @throws JSQLParserException SQL 语句语法错误
     * @throws ParseException      解析错误
     * @throws DbException         内部错误
     */
    public static OpIterator parser(String sql, TransactionId tid) throws JSQLParserException, ParseException, DbException {
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Statement statement = parserManager.parse(new StringReader(sql));

        if (statement instanceof Select select) {
            return QueryParser.parserSelect(select, tid);
        } else if (statement instanceof Insert insert) {
            return InsertParser.parserInsert(insert, tid);
        } else if (statement instanceof CreateTable create) {
            CreateParser.parserCreate(create);
        } else if (statement instanceof net.sf.jsqlparser.statement.update.Update update) {
            return UpdateParser.parserUpdate(update, tid);
        } else if (statement instanceof Delete delete) {
            return DeleteParser.parserDelete(delete, tid);
        }
        return null;
    }


    public static void main(String[] args) throws Exception {
        String sql = "START;";
        CCJSqlParserManager parserManager = new CCJSqlParserManager();
        Insert insert = (Insert) parserManager.parse(new StringReader(sql));
        System.out.println("insert.isUseValues() = " + insert.getItemsList(ExpressionList.class));
        // class net.sf.jsqlparser.expression.operators.relational.ExpressionList
        ExpressionList e = (ExpressionList) insert.getItemsList(ExpressionList.class);
        System.out.println("e = " + e.getClass());
        System.out.println("e.getExpressions().get(0) = " + e.getExpressions().get(0).getClass());
        System.out.println("e.getExpressions().get(0) = " + e.getExpressions().get(1).getClass());
        List<Expression> expressions = e.getExpressions();
        RowConstructor expression = (RowConstructor) expressions.get(0);
        List exprList = (List) expression.getExprList().getExpressions();
        System.out.println(exprList);
    }
}
