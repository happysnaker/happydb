package happydb.execution;

import happydb.common.Database;
import happydb.exception.DbException;
import happydb.optimizer.LogicalSelectListNode;
import happydb.storage.Record;
import happydb.storage.TableDesc;
import happydb.storage.Type;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

/**
 * 将返回给用户增删改查的最终结果，<B>只有在调用 {@link #open()} 后，此类才会真正的进行工作</B>，
 * 增删改操作仅会返回一行记录，表示影响的行数
 * @Author happysnaker
 * @Date 2022/11/28
 * @Email happysnaker@foxmail.com
 */
public class Project extends AbstractOpIterator {
    @Getter
    @Setter
    private OpIterator child;

    @Getter
    private List<LogicalSelectListNode> selects;
    /**
     * 返回用户, fieldName 是用户指定的
     */
    @Getter
    private TableDesc physicalTableDesc;
    /**
     * 逻辑表, fieldName 是 child 中的字段，但与  physicalTableDesc 顺序一致
     */

    @Getter
    private TableDesc logicalTableDesc;

    @Getter
    TableDesc childTableDesc;


    /**
     * 1 代表是 SELECT，2 代表是增删改，3 代表是其他操作
     */
    @Getter
    int type = 1;



    /**
     * 构造一个查询的结果
     *
     * @param child            最终查询的子运算符
     * @param selects          用户查询的列表
     * @param groupByFieldName 分组字段（如果有的话）
     */
    public Project(OpIterator child, List<LogicalSelectListNode> selects, String groupByFieldName) {
        this.child = child;
        this.selects = selects;


        this.childTableDesc = child.getTableDesc();
        List<String> physicalFieldNameList = new ArrayList<>();
        List<String> logicalFieldNameList = new ArrayList<>();
        List<Type> fieldTypeList = new ArrayList<>();
        int aggPoint = groupByFieldName != null ? 1 : 0;
        for (LogicalSelectListNode select : selects) {
            if (select.getFieldPureName().equals("*")) {
                if (select.getFieldTable() == null) {
                    // 所有字段
                    for (int i = 0; i < childTableDesc.numFields(); i++) {
                        physicalFieldNameList.add(childTableDesc.getFieldName(i));
                        logicalFieldNameList.add(childTableDesc.getFieldName(i));
                        fieldTypeList.add(childTableDesc.getFieldType(i));
                    }
                } else {
                    TableDesc t = Database.getCatalog().getTableDesc(select.getFieldTable());
                    for (int i = 0; i < t.numFields(); i++) {
                        physicalFieldNameList.add(String.format("%s.%s", select.getFieldTable(), t.getFieldName(i)));
                        logicalFieldNameList.add(String.format("%s.%s", select.getFieldTable(), t.getFieldName(i)));
                        fieldTypeList.add(t.getFieldType(i));
                    }
                }
                continue;
            }

            if (select.aggOp != null) {
                // 分组的话，结果应该是固定的，如果有分组，那么第 0 个是分组字段，后面依次是聚合字段
                logicalFieldNameList.add(childTableDesc.getFieldName(aggPoint));
                fieldTypeList.add(childTableDesc.getFieldType(aggPoint));
                aggPoint++;
            } else {
                if (select.getFieldQuantifiedName().equals(groupByFieldName)) {
                    logicalFieldNameList.add(childTableDesc.getFieldName(0));
                    fieldTypeList.add(childTableDesc.getFieldType(0));
                } else {
                    int i = childTableDesc.fieldNameToIndex(select.fieldQuantifiedName);
                    logicalFieldNameList.add(childTableDesc.getFieldName(i));
                    fieldTypeList.add(childTableDesc.getFieldType(i));
                }
            }
            physicalFieldNameList.add(select.getAs());
        }

        this.logicalTableDesc = new TableDesc(logicalFieldNameList.toArray(new String[0]), fieldTypeList.toArray(new Type[0]));
        this.physicalTableDesc = new TableDesc(physicalFieldNameList.toArray(new String[0]), fieldTypeList.toArray(new Type[0]));
    }

    /**
     * 构造一个更新操作的结果，将返回受影响的行
     *
     * @param child 子运算符，表示受影响的行
     */
    public Project(OpIterator child) {
        type = 2;
        this.child = child;
    }


    @Override
    protected void openOpIterator() throws DbException {
        assert child != null;
        child.open();
    }

    @Override
    protected void closeOpIterator() throws DbException {
        child.close();
    }

    @Override
    protected Record fetchNext() throws DbException {
        if (!child.hasNext()) {
            return null;
        }
        Record next = child.next();
        if (type == 1)
            return convert(next);
        return next;
    }

    /**
     * 将查询结果转换为用户所需结果
     *
     * @param child 查询结果
     * @return 用户所需结果
     */
    private Record convert(Record child) {
        Record record = new Record(getTableDesc());
        for (int i = 0; i < physicalTableDesc.numFields(); i++) {
            int index = childTableDesc.fieldNameToIndex(logicalTableDesc.getFieldName(i));

            record.setField(i, child.getField(index));
        }
        return record;
    }

    @Override
    public void rewind() throws DbException {
        child.rewind();
    }

    @Override
    public TableDesc getTableDesc() {
        return type == 1 ? physicalTableDesc : child.getTableDesc();
    }
}
