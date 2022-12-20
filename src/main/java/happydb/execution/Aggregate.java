package happydb.execution;

import happydb.exception.DbException;
import happydb.storage.TableDesc;
import happydb.storage.Type;
import happydb.storage.Record;
import lombok.Getter;

import java.util.List;
import java.util.NoSuchElementException;

import static happydb.execution.Aggregator.NO_GROUPING;

/**
 * @Author happysnaker
 * @Date 2022/11/24
 * @Email happysnaker@foxmail.com
 */
public class Aggregate extends AbstractOpIterator {
    @Getter
    private final OpIterator child;
    @Getter
    private final int groupByField;
    @Getter
    private final List<AggregatorNode> nodes;
    private final Aggregator aggregator;
    private OpIterator aggregatorIterator;


    /**
     * 构建一个聚合运算符
     * @param child 子运算符
     * @param gField 待分组字段，可以是 {@link Aggregator#NO_GROUPING}
     * @param nodes 待聚合字段，聚合字段的下标必须与 child 输出的 TableDesc 一致
     */
    public Aggregate(OpIterator child, int gField, List<AggregatorNode> nodes) {
        this.child = child;
        this.nodes = nodes;
        this.groupByField = gField;
        this.aggregator = new Aggregator(child.getTableDesc(), groupByField, nodes);
    }


    /**
     * @return 如果此聚合伴随有分组依据，则返回 <b>OUTPUT<b> 记录中分组依据字段的名称。如果不是，返回null
     */
    public String groupFieldName() {
        return groupByField == -1 ? null : child.getTableDesc().getFieldName(groupByField);
    }


    public void openOpIterator() throws NoSuchElementException, DbException {
        // some code goes here
        this.child.open();
        while (this.child.hasNext()) {
            aggregator.mergeRecordIntoGroup(this.child.next());
        }
        this.aggregatorIterator = aggregator.iterator();
        this.aggregatorIterator.open();
    }


    protected Record fetchNext() throws DbException {
        return this.aggregatorIterator.hasNext() ? this.aggregatorIterator.next() : null;
    }

    public void rewind() throws DbException {
        this.aggregatorIterator.rewind();
    }


    public TableDesc getTableDesc() {
        return aggregator.getTableDesc();
    }

    public void closeOpIterator() throws DbException {
        this.aggregatorIterator.close();
    }
}
