package happydb.log;

import happydb.storage.Page;
import happydb.storage.PageId;

/**
 * 继承此接口意味着 <strong>页将受重做日志、检查点机制管控</strong>
 *
 * @Author happysnaker
 * @Date 2022/11/29
 * @Email happysnaker@foxmail.com
 */
public interface DataPage {
    /**
     * 设置此页面的 LSN
     *
     * @param lsn 待设置的 LSN，如果参数给定的 lsn 没有页原来的大，则什么也不做
     */
    void setLsn(long lsn);


    /**
     * 获取页面上的 LSN
     *
     * @return LSN
     */
    long getLsn();


    /**
     * 获取第一次将此页面弄脏时的 LSN
     * @return 第一次将此页面弄脏时的 LSN
     */
    long getFirstDirtyLsn();

    /**
     * 设置页面是否弄脏，如果此前页面非脏，则此次弄脏须记录下当前 LSN
     *
     * @param dirty 脏
     */
    void markDirty(boolean dirty);

    /**
     * @return 页面是否为脏页
     */
    boolean isDirty();

    /**
     * 获取此页面的 ID
     * @return
     */
    PageId getPageId();
}
