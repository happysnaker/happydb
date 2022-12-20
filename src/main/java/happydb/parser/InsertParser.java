package happydb.parser;

import happydb.common.Database;
import happydb.exception.ParseException;
import happydb.execution.OpIterator;
import happydb.execution.Project;
import happydb.execution.RecordIterator;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.TransactionId;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;

import java.util.*;

/**
 * @Author happysnaker
 * @Date 2022/12/4
 * @Email happysnaker@foxmail.com
 */
public class InsertParser {

    public static String expressionToString(Expression exp) {
        if (exp instanceof StringValue c) {
            return c.getValue();
        } else if (exp instanceof LongValue c) {
            return String.valueOf(c.getValue());
        } else if (exp instanceof DoubleValue c) {
            return Double.toString(c.getValue());
        } else if (exp instanceof  net.sf.jsqlparser.schema.Column c) {
            return c.getColumnName().replace("\"", "");
        } else {
            throw new IllegalArgumentException("Not support type for " + exp.getClass());
        }
    }

    public static Field parseField(String input, Type type) {
        return switch (type) {
            case INT_TYPE -> input == null ? new IntField(0) : new IntField(Integer.parseInt(input));
            case DOUBLE_TYPE -> input == null ? new DoubleField(0) : new DoubleField(Double.parseDouble(input));
            case STRING_TYPE -> input == null ? new StringField("") : new StringField(input);
        };
    }

    public static Record createDefaultRecord(TableDesc td) {
        Record record = new Record(td);
        for (int i = 0; i < td.numFields(); i++) {
            record.setField(i, parseField(null, td.getFieldType(i)));
        }
        record.setValid(true);
        return record;
    }


    public static Project parserInsert(Insert insert, TransactionId tid) throws ParseException {
        String tableName = insert.getTable().getName().replace("`", "");
        TableDesc td = null;
        try {
            td = Database.getCatalog().getTableDesc(tableName);
        } catch (NoSuchElementException e) {
            throw new ParseException("No such table " + tableName);
        }

        List<Record> records = new ArrayList<>();

        if (insert.getItemsList() == null) {
            throw new ParseException("Insert statement has no values expression");
        }
        ExpressionList itemsList = (ExpressionList) insert.getItemsList();
        List<Expression> valuesList = itemsList.getExpressions();

        if (valuesList == null || valuesList.isEmpty()) {
            throw new ParseException("Insert statement has no values expression");
        }

        List<Column> columns = insert.getColumns();
        if (columns == null) {
            columns = new ArrayList<>();
            for (int i = 0; i < td.numFields(); i++) {
                columns.add(new Column(td.getFieldName(i)));
            }
        }
        
        if (!(valuesList.get(0) instanceof RowConstructor) && !(valuesList.get(0) instanceof Parenthesis)) {
            Record record = createDefaultRecord(td);
            if (valuesList.size() != columns.size()) {
                throw new ParseException("Inconsistent signature length");
            }
            for (int i = 0; i < valuesList.size(); i++) {
                int index;
                try {
                    index = td.fieldNameToIndex(columns.get(i).getColumnName());
                } catch (NoSuchElementException e) {
                    throw new ParseException("Table " + tableName + " not has field named " + columns.get(i));
                }
                record.setField(index, parseField(expressionToString(valuesList.get(i)), td.getFieldType(index)));
            }
            records.add(record);
        } else {
            // 插入多个 values
            for (Expression values : valuesList) {
                Record record = createDefaultRecord(td);
                Map<String, String> fieldMap = new HashMap<>();

                if (values instanceof Parenthesis p) {
                    // 单个值
                    if (columns.size() != 1) {
                        throw new ParseException("Inconsistent signature length");
                    }
                    fieldMap.put(columns.get(0).getColumnName(), expressionToString(p.getExpression()));
                } else if (values instanceof RowConstructor rcs) {
                    List<Expression> es = rcs.getExprList().getExpressions();
                    if (es.size() != columns.size()) {
                        throw new ParseException("Inconsistent signature length");
                    }
                    for (int i = 0; i < es.size(); i++) {
                        fieldMap.put(columns.get(i).getColumnName(), expressionToString(es.get(i)));
                    }
                } else {
                    throw new ParseException("Unknown error here.");
                }

                for (Map.Entry<String, String> it : fieldMap.entrySet()) {
                    int i;
                    try {
                        i = td.fieldNameToIndex(it.getKey());
                    } catch (NoSuchElementException e) {
                        throw new ParseException("Table " + tableName + " not has field named " + it.getKey());
                    }
                    record.setField(i, parseField(it.getValue(), td.getFieldType(i)));
                }

                records.add(record);
            }
        }

        OpIterator it = new RecordIterator(td, records);
        happydb.execution.Insert child = new happydb.execution.Insert(it, tid);
        return new Project(child);
    }

}
