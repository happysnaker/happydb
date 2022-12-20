package happydb.execution;

import happydb.exception.DbException;
import happydb.storage.Record;
import happydb.storage.TableDesc;
import lombok.Getter;

import java.util.NoSuchElementException;

/**
 * 使用循环嵌套法的连接运算符
 *
 * @Author happysnaker
 * @Date 2022/11/23
 * @Email happysnaker@foxmail.com
 */
public class NormalJoin extends AbstractOpIterator {
    @Getter
    private final JoinPredicate joinPredicate;
    @Getter
    private final OpIterator child1;
    @Getter
    private final OpIterator child2;


    /**
     * current 用以实现循环嵌套法，他表示外层循环的节点
     */
    private Record current;


    /**
     * 构造函数。接受两个孩子加入和谓词加入他们
     *
     * @param p      用于加入孩子的谓词
     * @param child1 要加入的左（外）关系的迭代器
     * @param child2 加入右（内）关系的迭代器
     */
    public NormalJoin(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.joinPredicate = p;
        this.child1 = child1;
        this.child2 = child2;
        TableDesc td = TableDesc.merge(child1.getTableDesc(), child2.getTableDesc());
    }

    public TableDesc getTableDesc() {
        // some code goes here
        return TableDesc.merge(child1.getTableDesc(), child2.getTableDesc());
    }

    public void openOpIterator() throws DbException, NoSuchElementException {
        child1.open();
        child2.open();
    }

    public void closeOpIterator() throws DbException {
        // some code goes here
        child1.close();
        child2.close();
        this.current = null;
    }

    public void rewind() throws DbException {
        child1.rewind();
        child2.rewind();
    }

    /**
     * 返回连接生成的下一个元组，如果没有更多元组则返回 null。从逻辑上讲，这是 r1 交叉 r2 中满足连接谓词的下一个元组。此类实现的是嵌套循环连接。
     * <p>
     * 请注意，从 Join 的这个特定实现返回的元组只是从左右关系连接元组的串联。因此，如果使用相等谓词，结果中将有两个 join 属性副本。
     * （如果需要，可以使用额外的投影运算符删除此类重复列。）
     * <p>
     * 例如，如果一个元组是 {1,2,3}，另一个元组是 {1,5,6}，根据第一列的相等性连接，则返回 {1,2,3,1,5,6 }.
     *
     * @return 下一个匹配的元组。
     * @see JoinPredicate#filter
     */
    protected Record fetchNext() throws DbException {
        // 第一次调用，初始化外层循环元素
        if (current == null && child1.hasNext()) {
            current = child1.next();
        }
        Record tuple = null;
        while (current != null && child2.hasNext() && tuple == null) {
            Record next = child2.next();
            if (joinPredicate.filter(current, next)) {
                tuple = mergeRecord(current, next);
            }
            // 内层循环到头，外层循环元素 next，内层循环重置
            if (!child2.hasNext()) {
                current = child1.hasNext() ? child1.next() : null;
                child2.rewind();
            }
        }
        return tuple;
    }


    /**
     * 静态方法，用于实现量表的连接，他可能会包含一些重复字段
     *
     * @return 连接后的投影
     */
    public static Record mergeRecord(Record t1, Record t2) {
        TableDesc td = TableDesc.merge(t1.getTableDesc(), t2.getTableDesc());
        Record tuple = new Record(td);
        for (int i = 0; i < td.numFields(); i++) {
            if (i < t1.getTableDesc().numFields()) {
                tuple.setField(i, t1.getField(i));
            } else {
                tuple.setField(i, t2.getField(i - t1.getTableDesc().numFields()));
            }
        }
        return tuple;
    }
}
