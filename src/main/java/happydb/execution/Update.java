package happydb.execution;

import happydb.common.Database;
import happydb.common.Debug;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.log.UndoLog;
import happydb.optimizer.TableStateView;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.TransactionId;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 此类负责元组的更新，并返回受影响的行数
 *
 * @Author happysnaker
 * @Date 2022/12/5
 * @Email happysnaker@foxmail.com
 */
public class Update extends AbstractOpIterator {

    String expression;

    boolean first = true;

    int rowsAffected;

    int updateField;


    OpIterator child;

    TransactionId tid;

    /**
     * 构造一个更新操作运算符，<b>不允许对索引列上的更新</b>
     *
     * @param updateField 待更新列的下标
     * @param expression  列上的表达式，例如 <code>tb.x = tb.x + y + 1</code>，
     *                    此参数为等式右边的简单表达式（不包括左边设置列），表达式只能使用加减乘除，可以使用原有列上的值 <br/>
     *                    <strong>当待更新的列为字符串类型时，参数只能是一个字符串，不支持任何表达式</strong>
     * @param child       需要进行更新的记录
     * @param tid         操作的事务 ID
     */
    public Update(int updateField, String expression, UpdateOperateScan child, TransactionId tid) {
        this.updateField = updateField;
        this.expression = expression.trim();
        this.child = child;
        this.tid = tid;
    }


    private void doUpdate(Record record) throws DbException {
        TableDesc td = child.getTableDesc();

        Record update = record.clone();
        update.setValid(true);
        update.setLastModify(tid);

        if (td.getFieldType(updateField) == Type.STRING_TYPE) {
            update.setField(updateField, new StringField(expression));
        } else {
            String expr = expression;
            for (int i = 0; i < td.numFields(); i++) {
                String replacement = record.getField(i).getObject().toString();
                expr = expr
                        .replace(String.format("%s.%s", td.getTableName(), td.getFieldName(i)), replacement)
                        .replace(td.getFieldName(i), replacement);
            }
            if (td.getFieldType(updateField) == Type.INT_TYPE) {
                update.setField(updateField, new IntField((int) calculate(expr, true)));
            } else {
                update.setField(updateField, new DoubleField(calculate(expr, false)));
            }
        }

        UndoLog undoLog = Database.getLogBuffer().createUpdateUndoLog(tid, record);
        update.setLogPointer(undoLog.getId());

        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, record.getRecordId().getPid(), Permissions.READ_ONLY);
        page.updateRecord(record.getRecordId(), update);

        Database.getLogBuffer().createUpdateRedoLog(tid, update);

        TableStateView.getInstance().updateRecord(child.getTableDesc().getTableName(), record, update);
    }

    @Override
    protected void openOpIterator() throws DbException {
        child.open();
        first = true;

        while (child.hasNext()) {
            doUpdate(child.next());
            rowsAffected++;
        }
    }

    @Override
    protected void closeOpIterator() throws DbException {
        child.close();
    }

    @Override
    protected Record fetchNext() throws DbException {
        if (!first) {
            return null;
        }
        first = false;
        Record record = new Record(getTableDesc());
        record.setField(0, new IntField(rowsAffected));
        return record;
    }

    @Override
    public void rewind() throws DbException {
        first = true;
    }

    @Override
    public TableDesc getTableDesc() {
        return new TableDesc(new String[]{"rowsAffected"}, new Type[]{Type.INT_TYPE});
    }


    /**
     * 辅助计算器，用于解析表达式
     *
     * @param expression           表达式
     * @param usingIntegerDivision 除法是否使用整数除法
     */
    public static double calculate(String expression, boolean usingIntegerDivision) {
        char[] str = ("(" + expression + ")")
                .replace(" ", "")
                .replace("(-", "(0-")
                .toCharArray();

        Map<Character, Integer> priority = Map.of('+', 0, '-', 0, '*', 1, '/', 1);

        Deque<Double> numStk = new ArrayDeque<>();
        Deque<Character> opsStk = new ArrayDeque<>();

        Supplier<Double> calculate = () -> {
            double v2 = numStk.pop(), v1 = numStk.pop();
            return switch (opsStk.pop()) {
                case '+' -> v1 + v2;
                case '-' -> v1 - v2;
                case '*' -> v1 * v2;
                case '/' -> usingIntegerDivision ? (int) (v1 / v2) : v1 / v2;
                default -> null;
            };
        };

        int n = str.length, i = 0;
        while (i < n) {
            if (Character.isDigit(str[i]) || str[i] == '.') {
                StringBuilder val = new StringBuilder();
                while (i < n && (Character.isDigit(str[i]) || str[i] == '.')) {
                    val.append(str[i++]);
                }

                numStk.push(Double.parseDouble(val.toString()));
                continue;
            } else if (str[i] == ')') {
                assert !opsStk.isEmpty();
                while (opsStk.peek() != '(') {
                    numStk.push(calculate.get());
                    assert !opsStk.isEmpty();
                }
                opsStk.pop();
            } else if (str[i] == '(') {
                opsStk.push('(');
            } else {
                while (!opsStk.isEmpty() && priority.containsKey(opsStk.peek()) &&
                        priority.get(str[i]) >= priority.get(opsStk.peek())) {
                    numStk.push(calculate.get());
                }
                opsStk.push(str[i]);
            }
            i++;
        }
        return numStk.isEmpty() ? 0 : numStk.pop();
    }
}
