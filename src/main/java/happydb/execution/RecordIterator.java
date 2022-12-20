package happydb.execution;

import happydb.storage.Record;
import happydb.storage.TableDesc;

import java.util.Iterator;

/**
 * @Author happysnaker
 * @Date 2022/11/23
 * @Email happysnaker@foxmail.com
 */
public class RecordIterator implements OpIterator {
    Iterator<Record> it = null;
    TableDesc td = null;
    Iterable<Record> records = null;

    /**
     * 从指定的 Iterable 和指定的描述符构造一个迭代器。
     *
     * @param records
     *            要迭代的元组集
     */
    public RecordIterator(TableDesc td, Iterable<Record> records) {
        this.td = td;
        this.records = records;
    }

    public void open() {
        it = records.iterator();
    }

    public boolean hasNext() {
        return it.hasNext();
    }

    public Record next() {
        return it.next();
    }

    public void rewind() {
        close();
        open();
    }

    public TableDesc getTableDesc() {
        return td;
    }

    public void close() {
        it = null;
    }
}
