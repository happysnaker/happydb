package happydb.execution;

import happydb.exception.DbException;
import happydb.storage.Record;
import happydb.storage.TableDesc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * HelloDb 中的运算符，接受一些运算符作为输出，并将它们过滤或转换为另一个运算符，<strong>运算符只有在打开后才会开始工作</strong>，
 * 这意味着可以提前构造运算符而不必担心他们提前开始工作
 *
 * <P><strong>所有运算符都不是线程安全的</strong>，经运算符计算输出的运算符元数据与原先的输入可能不一致，模式也可能发生改变</P>
 * @Author happysnaker
 * @Date 2022/11/23
 * @Email happysnaker@foxmail.com
 */
public interface OpIterator extends Serializable {
    /**
     * 打开迭代器。这必须在任何其他方法之前调用。
     *
     * @throws DbException 当打开访问数据库时出现问题。
     */
    void open() throws DbException;

    /**
     * 如果迭代器有更多记录，则返回 true。
     *
     * @return true 如果迭代器有更多记录。
     * @throws IllegalStateException 如果迭代器还没有打开
     */
    boolean hasNext() throws DbException;

    /**
     * 从运算符返回下一条记录（通常通过从子运算符或访问方法读取来实现）。
     *
     * @return 迭代中的下一个记录。
     * @throws NoSuchElementException 如果没有更多记录。
     * @throws IllegalStateException  如果迭代器还没有打开
     */
    Record next() throws DbException, NoSuchElementException;

    /**
     * 将迭代器重置为开始。
     *
     * @throws DbException           当不支持倒带时。
     * @throws IllegalStateException 如果迭代器还没有打开
     */
    void rewind() throws DbException;

    /**
     * 返回与此 OpIterator 关联的 TableDesc。
     * <P>例如连接操作应该返回两表连接的表模式</P>
     * @return 与此 OpIterator 关联的 TableDesc
     */
    TableDesc getTableDesc();

    /**
     * 关闭迭代器。当迭代器关闭时，调用 next()、hasNext() 或 rewind() 应该因抛出 IllegalStateException 而失败。
     * <br><strong>请注意，当手动调用迭代器关闭时，再次打开迭代器可能无法被保证迭代器的正常工作，
     * 换句话说，当此方法被调用时，意味着此对象的生命应该走到尽头</strong>
     */
    void close() throws DbException;

    /**
     * 调用此迭代器，并调用迭代器遍历元组，并封装为数组返回
     *<p><strong>此方法不会调用 open 和 close 方法，子类必须保证迭代器的打开与关闭</strong></p>
     * <P>此方法不会重置迭代器，<strong>它将从当前位置开始遍历</strong></P>
     * @return 由此迭代器遍历得到的所有元组数组
     */
    default Record[] getRecordAr() throws  DbException {
        List<Record> Records = new ArrayList<>();
        while (this.hasNext()) {
            Records.add(this.next());
        }
        return Records.toArray(new Record[0]);
    }
}

