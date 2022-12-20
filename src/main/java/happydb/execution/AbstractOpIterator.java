package happydb.execution;

import happydb.exception.DbException;
import happydb.storage.Record;

import java.util.NoSuchElementException;

/**
 * 一个便捷的抽象类
 * @Author happysnaker
 * @Date 2022/11/23
 * @Email happysnaker@foxmail.com
 */
public abstract class AbstractOpIterator implements OpIterator {
    private Record next = null;
    private boolean open = false;

    public boolean hasNext() throws DbException {
        if (!this.open)
            throw new IllegalStateException("Operator not yet open");

        if (next == null)
            next = fetchNext();
        return next != null;
    }

    public Record next() throws DbException,
            NoSuchElementException {
        if (next == null) {
            next = fetchNext();
            if (next == null)
                throw new NoSuchElementException();
        }

        next.setTableDesc(getTableDesc());

        Record result = next;
        next = null;
        return result;
    }



    /**
     * 打开迭代器，子类需要实现的方法，此方法应该发生在任何对迭代器的操作之前
     */
    protected abstract void openOpIterator()  throws DbException;
    /**
     * 关闭迭代器，子类需要实现的方法，当调用此方法后，任何迭代器操作都将抛出异常
     */
    protected abstract void closeOpIterator()  throws DbException;
    /**
     * 返回迭代器中的下一个 Record，如果迭代完成则返回 null。运算符使用此方法来实现 <code>next<code> 和 <code>hasNext<code>。
     *
     * @return 迭代器中的下一个 Record，如果迭代完成则为 null。
     */
    protected abstract Record fetchNext() throws DbException;

    /**
     * 子类应该实现 {@link #closeOpIterator()} 方法而不是覆盖此方法
     */
    public void close() throws DbException {
        if (!this.open) {
            return;
        }
        next = null;
        this.open = false;
        this.closeOpIterator();
    }

    /**
     * 子类应该实现 {@link #openOpIterator()} 方法而不是覆盖此方法
     */
    public void open() throws DbException {
        if (this.open) {
            return;
        }
        this.open = true;
        this.openOpIterator();
    }
}
