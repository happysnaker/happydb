package happydb.execution;

import happydb.exception.DbException;
import happydb.storage.Field;
import happydb.storage.Record;
import happydb.storage.RecordId;
import happydb.storage.TableDesc;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

/**
 * 处理 OR 时的运算符，负责将 OR 两边的子运算符结合起来并返回一个新的运算符。
 * <P>此类通过判断记录 ID 是否相等来判断两个记录是否相等，<strong>因此子运算符必须要是物理记录而非投影记录</strong></P>
 * <P><strong>请注意，连接操作表达式不支持 ON OR，事实上市面上部分数据库也是如此，在连接时，如果需要 ON OR，则需要使用 UNION ALL 关键字</strong></P>
 *
 * @Author happysnaker
 * @Date 2022/11/23
 * @Email happysnaker@foxmail.com
 */
public class UnionOnOr extends AbstractOpIterator {

    private final OpIterator child1;
    private final OpIterator child2;

    private final TableDesc tableDesc;

    /**
     * OR 运算两边的运算符，例如 <code>SELECT * FROM tb WHERE x = 3 OR x = 5</code>，
     * 则 SELECT * FROM tb WHERE x = 3 作为子运算符 1，SELECT * FROM tb WHERE x = 5 作为子运算符 2，
     * 此运算符会结合他们并返回新的运算符
     * <P><strong>请注意，左右运算符的模式应该严格相等</strong></P>
     *
     * @param child1 运算符
     * @param child2 运算符
     */
    public UnionOnOr(OpIterator child1, OpIterator child2) {
        this.child1 = child1;
        this.child2 = child2;
        this.tableDesc = child1.getTableDesc();
    }


    private Set<Node> set = new HashSet<>();
    private Iterator<Node> iterator;

    @Override
    protected void closeOpIterator() throws DbException {
        child1.close();
        child2.close();
        set = null;
        iterator = null;
    }

    @Override
    protected void openOpIterator() throws DbException {
        child1.open();
        child2.open();

        for (Record record : child1.getRecordAr()) {
            set.add(new Node(record));
        }
        for (Record record : child2.getRecordAr()) {
            set.add(new Node(record));
        }
        this.iterator = set.iterator();
    }

    @Override
    protected Record fetchNext() throws DbException {
        if (iterator.hasNext())
            return iterator.next().record;
        return null;
    }

    @Override
    public void rewind() throws DbException {
        this.iterator = set.iterator();
    }

    @Override
    public TableDesc getTableDesc() {
        return tableDesc;
    }

    /**
     * 一个帮助节点，其 hashCode 和 equals 由记录 ID 实现
     */
    static class Node {
        Record record;
        RecordId recordId;

        public Node(Record record) {
            this.record = record;
            this.recordId = record.getRecordId();
            if (recordId == null)
                throw new RuntimeException("非物理记录");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Node node)) return false;

            return recordId.equals(node.recordId);
        }

        @Override
        public int hashCode() {
            return recordId.hashCode();
        }
    }
}
