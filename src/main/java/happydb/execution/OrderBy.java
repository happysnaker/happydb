package happydb.execution;

import happydb.exception.DbException;
import happydb.storage.Field;
import happydb.storage.TableDesc;
import happydb.storage.Record;
import lombok.Getter;

import java.util.*;

/**
 * @Author happysnaker
 * @Date 2022/11/23
 * @Email happysnaker@foxmail.com
 */
public class OrderBy extends AbstractOpIterator {

    @Getter
    private OpIterator child;

    @Getter
    private final int orderByField;
    @Getter
    private final String orderByFieldName;

    @Getter
    private final boolean asc;
    private final TableDesc td;
    private List<Record> childRecords;
    private Iterator<Record> it;

    /**
     * 在迭代器的元组上创建一个新的 OrderBy 节点。
     *
     * @param orderbyField 应用排序的字段。
     * @param asc          如果排序顺序为升序，则为真。
     * @param child        要排序的元组。
     */
    public OrderBy(int orderbyField, boolean asc, OpIterator child) {
        this.child = child;
        td = child.getTableDesc();
        this.orderByField = orderbyField;
        this.orderByFieldName = td.getFieldName(orderbyField);
        this.asc = asc;
    }


    public TableDesc getTableDesc() {
        return td;
    }

    public void openOpIterator() throws DbException, NoSuchElementException {
        this.child.open();
        this.childRecords = new ArrayList<>(Arrays.stream(child.getRecordAr()).toList());
        this.childRecords.sort(new RecordComparator(orderByField, asc));
        this.it = childRecords.iterator();
    }

    public void closeOpIterator() {
        it = null;
    }

    public void rewind() {
        it = childRecords.iterator();
    }

    /**
     * Operator.fetchNext 实现。按顺序从子运算符返回元组
     *
     * @return 顺序中的下一个元组，如果没有更多元组则为 null
     */
    protected Record fetchNext() throws NoSuchElementException {
        if (it != null && it.hasNext()) {
            return it.next();
        } else
            return null;
    }


    record RecordComparator(int field, boolean asc) implements Comparator<Record> {

        public int compare(Record o1, Record o2) {
            Field t1 = (o1).getField(field);
            Field t2 = (o2).getField(field);
            if (t1.compare(Predicate.Op.EQUALS, t2))
                return 0;
            if (t1.compare(Predicate.Op.GREATER_THAN, t2))
                return asc ? 1 : -1;
            else
                return asc ? -1 : 1;
        }

    }
}