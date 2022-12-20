package happydb.storage;

import happydb.common.DbSerializable;
import happydb.transaction.TransactionId;

import java.util.List;

/**
 * 磁盘中的接口页面
 *
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
public interface Page extends DbSerializable {


    /**
     * 获取页面的 ID
     *
     * @return 页面 ID
     */
    PageId getPageId();

    /**
     * 获取此页面上能容纳最多的条目数
     *
     * @return 此页面上能容纳最多的条目数
     */
    int getMaxNumEntries();

    /**
     * 获取此页面上空闲的插槽
     * <P>如果子页面没有使用插槽法，则调用此方法应该会抛出一个异常</P>
     * @return 此页面上空闲插槽列表，如果没有，返回空列表而不是 null
     */
    List<Integer> getEmptySlots();

    /**
     * 设置页面是否弄脏
     *
     * @param dirty 脏
     */
    void markDirty(boolean dirty);

    /**
     * @return 页面是否为脏页
     */
    boolean isDirty();

    /**
     * @return 给定线程是否持有读锁
     */
    boolean hasReadLock(TransactionId tid);


    /**
     * @return 给定线程是否持有写锁
     */
    boolean hasWriteLock(TransactionId tid);


    /**
     * 以读者模式锁定页面，这会导致堵塞，相同事务多次加锁是可重入的，<strong>但事务只允许释放一次锁</strong>
     * <br>如果同事务持有写锁，这个读锁将是可重入的
     *
     * @param timeoutMillis 超时时间，毫秒，为 0 时表示无限堵塞
     * @return 返回它在指定时间内是否成功获取锁
     */
    boolean tryReadLock(TransactionId tid, long timeoutMillis);


    /**
     * 以写者模式锁定页面，这会导致堵塞，相同事务多次加锁是可重入的，<strong>但事务只允许释放一次锁</strong>
     * <br>如果同事务持有读锁，这个方法也将堵塞
     *
     * @param timeoutMillis 超时时间，毫秒，为 0 时表示无线堵塞
     * @return 返回它在指定时间内是否成功获取锁
     */
    boolean tryWriteLock(TransactionId tid, long timeoutMillis);


    /**
     * 以写者模式锁定页面，这会导致堵塞，相同事务多次加锁是可重入的，<strong>但事务只允许释放一次锁</strong>
     * <br>如果同事务持有读锁，这个方法也将堵塞
     */
    void writeLock(TransactionId tid);

    /**
     * 以读者模式锁定页面，这会导致堵塞，相同事务多次加锁是可重入的，<strong>但事务只允许释放一次锁</strong>
     * <br>如果同事务持有写锁，这个读锁将是可重入的，事务将同时持有两把锁
     */
    void readLock(TransactionId tid);


    /**
     * 以读者模式释放页面
     *
     * @throws IllegalMonitorStateException 如果事务没有持有锁
     */
    void readUnLock(TransactionId tid);


    /**
     * 以写者模式释放页面
     *
     * @throws IllegalMonitorStateException 如果事务没有持有锁
     */
    void writeUnLock(TransactionId tid);
}
