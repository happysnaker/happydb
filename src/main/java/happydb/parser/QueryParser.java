package happydb.parser;

import happydb.common.Database;
import happydb.exception.DbException;
import happydb.exception.ParseException;
import happydb.execution.AggregatorNode;
import happydb.execution.Project;
import happydb.optimizer.LogicalFilterNode;
import happydb.optimizer.LogicalPlan;
import happydb.optimizer.LogicalSelectListNode;
import happydb.transaction.TransactionId;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.*;

/**
 * @Author happysnaker
 * @Date 2022/11/27
 * @Email happysnaker@foxmail.com
 */
public class QueryParser {


    public static Project parserSelect(Select select, TransactionId tid) throws JSQLParserException, ParseException, DbException {
        LogicalPlan lp = parseQueryLogicalPlan((PlainSelect) select.getSelectBody(), tid);
        return lp.toPhysicalPlan(tid);
    }


    public static LogicalPlan parseQueryLogicalPlan(PlainSelect select, TransactionId tid) throws JSQLParserException, ParseException {
        LogicalPlan lp = new LogicalPlan();

        // 解析 from
        for (Map.Entry<String, String> it : getTableAndAlias(select).entrySet()) {
            try {
                Database.getCatalog().getTableDesc(it.getValue());
            } catch (NoSuchElementException e) {
                throw new ParseException("表 " + it.getValue() + " 不存在");
            }

            lp.addTable(it.getKey(), it.getValue());
        }


        // 解析 where
        Expression where = select.getWhere();
        if (where != null) {
            processWhereExpression(lp, where, true);
            // 验证，join 限定条件中不允许为 OR，而单表过滤条件中 OR 必须在最后
            boolean hasOr = false, hasAnd = false;
            if (lp.getFilters() != null) {
                List<LogicalFilterNode> filters = lp.getFilters();
                Collections.reverse(filters); // processWhereExpression 是先解析右边在左边，应该反转
                for (int i = filters.size() - 1; i >= 0; i--) {
                    if (filters.get(i).isAnd()) {
                        hasAnd = true;
                    } else {
                        if (hasAnd)
                            throw new ParseException("OR 条件必须出现在限制条件的最后，不能出现在 AND 之前");
                        hasOr = true;
                    }
                }
            }
            if (hasOr && lp.getJoins() != null && !lp.getJoins().isEmpty())
                throw new ParseException("JOIN 限定条件中不允许 ON OR");
        }

        // 解析 group by field
        GroupByElement element = select.getGroupBy();
        if (element != null) {
            List<Expression> expressions = element.getGroupByExpressionList().getExpressions();
            if (expressions.size() >= 2) {
                throw new ParseException("最多允许一个分组字段");
            }
            lp.addGroupBy(expressions.get(0).toString());
        }

        // 解析 order by
        List<OrderByElement> orderByElements = select.getOrderByElements();
        if (orderByElements != null) {
            if (orderByElements.size() >= 2) {
                throw new ParseException("最多允许一个排序字段");
            }
            OrderByElement order = orderByElements.get(0);
            lp.addOrder(order.getExpression().toString(), order.isAsc());
        }

        // 解析 select 以及聚合字段
        List<SelectItem> selectItems = select.getSelectItems();
        if (selectItems == null) {
            throw new ParseException("Not select field.");
        } else {
            boolean hasAggOrGroup = lp.getGroupByFieldName() != null;
            for (SelectItem item : selectItems) {
                String exp = item.toString().trim();
                if (exp.endsWith("*")) {
                    lp.addSelect(exp, null, exp);
                    continue;
                }
                if (!(item instanceof SelectExpressionItem sei)) {
                    throw new ParseException("Unknown select field " + exp);
                }
                exp = sei.getExpression().toString().trim();
                boolean hasAgg = false;
                for (String key : AggregatorNode.OP_MAP.keySet()) {
                    if (exp.toUpperCase(Locale.ROOT).startsWith(key + "(")) {
                        String aggFieldName = exp.substring(key.length() + 1, exp.lastIndexOf(')'));
                        lp.addAggregate(key, aggFieldName);
                        lp.addSelect(aggFieldName, key, sei.getAlias() == null ? exp : sei.getAlias().getName());
                        hasAggOrGroup = hasAgg = true;
                        break;
                    }
                }
                if (!hasAgg) {
                    lp.addSelect(exp, null, sei.getAlias() == null ? exp : sei.getAlias().getName());
                }
            }
            // 如果有分组或者聚合，那么不允许有普通列
            if (hasAggOrGroup) {
                if (lp.getAggregators() == null || lp.getAggregators().isEmpty()) {
                    throw new ParseException("没有任何聚合列，分组无意义");
                }
                for (LogicalSelectListNode node : lp.getSelectList()) {
                    if (node.getFieldQuantifiedName().equals(lp.getGroupByFieldName()))
                        continue;

                    boolean isAgg = false;
                    if (lp.getAggregators() != null) {
                        for (AggregatorNode aggregator : lp.getAggregators()) {
                            if (node.getFieldQuantifiedName().equals(aggregator.getAggregateFieldName())) {
                                isAgg = true;
                                break;
                            }
                        }
                    }
                    if (!isAgg) {
                        throw new ParseException("不允许非聚合列或非分组列 " + node.getFieldPureName() + " 出现在此");
                    }
                }
            }
        }
        return lp;
    }


    /**
     * 解析 where 表达式，它包括单表过滤和 join 过滤
     */
    public static void processWhereExpression(LogicalPlan lp, Expression expression, Boolean isAnd) throws ParseException {
        if (expression == null) {
            return;
        }

        if (expression instanceof OrExpression orExpression) {
            processWhereExpression(lp, orExpression.getRightExpression(), false);
            processWhereExpression(lp, orExpression.getLeftExpression(), true);
            return;
        }
        if (expression instanceof AndExpression andExpression) {
            processWhereExpression(lp, andExpression.getRightExpression(), true);
            processWhereExpression(lp, andExpression.getLeftExpression(), true);
            return;
        }
        if (!(expression instanceof ComparisonOperator compare)) {
            throw new ParseException("不合法的表达式 " + expression + " 未包含运算符");
        }
        String op = compare.getStringExpression();
        String fieldName = null;
        String other = null;
        int column = 0;
        Expression left = compare.getLeftExpression();
        Expression right = compare.getRightExpression();
        if (left instanceof Column c) {
            fieldName = c.getFullyQualifiedName();
            column++;
        } else if (left instanceof DoubleValue || left instanceof LongValue || left instanceof StringValue) {
            if (left instanceof StringValue) {
                other = ((StringValue) left).getValue();
            } else {
                other = left.toString();
            }
        } else {
            throw new ParseException("不支持的表达式 " + left);
        }

        if (right instanceof Column c) {
            if (column == 0)
                fieldName = c.getFullyQualifiedName();
            else
                other = c.getFullyQualifiedName();
            column++;
        } else if (right instanceof DoubleValue || right instanceof LongValue || right instanceof StringValue sv) {
            if (right instanceof StringValue) {
                other = ((StringValue) right).getValue();
            } else {
                other = right.toString();
            }
        } else {
            throw new ParseException("不支持的表达式 " + left);
        }

        if (column == 2) {
            // 两边都是列，说明这是一个 join
            if (!isAnd) {
                throw new ParseException("Join 限定条件中不允许 ON OR");
            }
            lp.addJoin(fieldName, op, other);
        } else if (column == 1) {
            lp.addFilter(fieldName, op, other, isAnd);
        } else {
            throw new ParseException("限定条件中必须要存在一列");
        }
    }


    /**
     * 解析查询中的表与连接中的表与别名
     *
     * @param select
     * @return key 是别名，val 是表名
     */
    public static Map<String, String> getTableAndAlias(PlainSelect select) {
        Map<String, String> map = new HashMap<>();
        Table table = (Table) select.getFromItem();
        if (table.getAlias() != null) {
            map.put(table.getAlias().getName(), table.getName());
        } else {
            map.put(table.getName(), table.getName());
        }

        if (select.getJoins() != null) {
            for (Join join : select.getJoins()) {
                Table joinTable = (Table) join.getRightItem();
                if (joinTable.getAlias() != null) {
                    map.put(joinTable.getAlias().getName(), joinTable.getName());
                } else {
                    map.put(joinTable.getName(), joinTable.getName());
                }
            }
        }

        return map;
    }
}
