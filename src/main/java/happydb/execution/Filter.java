package happydb.execution;

import happydb.exception.DbException;
import happydb.storage.Record;
import happydb.storage.TableDesc;
import lombok.Getter;

import java.util.NoSuchElementException;

/**
 * {@link Filter} 持有一个 {@link Predicate}，接受一个子运算符输入，在迭代器工作后，{@link Filter}
 * 对子运算符的行记录进行过过滤，输出过滤后的迭代流，此类输出的元组指向输入的元组
 *
 * @Author happysnaker
 * @Date 2022/11/23
 * @Email happysnaker@foxmail.com
 */
public class Filter extends AbstractOpIterator {

    @Getter private final Predicate predicate;
    @Getter private final OpIterator child;


    /**
     * 构造函数接受一个要应用的谓词和一个子运算符来读取 happydb.storage.Records 以进行过滤。
     *
     * @param p     过滤 happydb.storage.Records 的谓词
     * @param child 子运算符
     */
    public Filter(Predicate p, OpIterator child) {
        this.predicate = p;
        this.child = child;
    }


    public TableDesc getTableDesc() {
        return child.getTableDesc();
    }

    public void openOpIterator() throws DbException, NoSuchElementException {
        child.open();
    }

    public void closeOpIterator() throws DbException {
        child.close();
    }

    public void rewind() throws DbException {
        // some code goes here
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext 实现。从子运算符迭代记录，
     * 将谓词应用于它们并返回那些通过谓词的记录（即 Predicate.filter() 返回 true 的记录。）
     *
     * @return The next Record that passes the filter, or null if there are no
     * more Records
     * @see Predicate#filter
     */
    protected Record fetchNext() throws NoSuchElementException,
            DbException {
        // some code goes here
        while (child.hasNext()) {
            Record next = child.next();
            if (predicate.filter(next)) {
                return next;
            }
        }
        return null;
    }
}
