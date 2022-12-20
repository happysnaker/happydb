package happydb.log;

import happydb.common.ByteArray;
import happydb.common.DbSerializable;
import happydb.exception.DbException;
import happydb.storage.PageId;
import happydb.transaction.TransactionId;

import java.text.ParseException;
import java.util.NoSuchElementException;

/**
 * 重做日志是数据库崩溃恢复的关键
 *
 * @Author happysnaker
 * @Date 2022/11/29
 * @Email happysnaker@foxmail.com
 */
public interface RedoLog extends DbSerializable {
    /**
     * 占位日志
     */
    byte SPACE_REDO = 0;
    /**
     * 由记录插入产生的重做日志
     */
    byte INSET_REDO = 1;
    /**
     * 由 Undo Log 插入产生的重做日志
     */
    byte INSET_UNDO_REDO = 2;
    /**
     * 由记录删除产生的重做日志
     */
    byte DELETE_REDO = 3;
    /**
     * 由记录更新产生的重做日志
     */
    byte UPDATE_REDO = 4;
    /**
     * 标识一个事务回滚
     */
    byte TRANSACTION_ABORT = 5;

    /**
     * 设置此日志的 LSN
     * @param lsn 待设置的 LSN，如果参数给定的 lsn 没有页原来的大，则什么也不做
     */
    void setLsn(long lsn);

    /**
     * 获取日志上的 LSN
     * @return LSN
     */
    long getLsn();

    /**
     * 返回此重做日志的类型
     *
     * @return 类型
     */
    byte getType();


    /**
     * 返回此重做日志的大小
     *
     * @return 大小
     */
    int size();


    /**
     * 根据此重做日志进行重做，重做是一次幂等的操作
     * <P><strong>对于重做页面 LSN 大于等于他们自身的页面，不应该重做</strong></P>
     * @throws DbException 任何异常抛出
     */
    void redoIfNecessary() throws DbException;


    /**
     * 返回此重做日志的事务 ID
     *
     * @return 事务
     */
    TransactionId xid();

    /**
     * 获取待重做数据所处的页 ID
     *
     * @return 待重做数据所处的页 ID
     */
    PageId getPageId();


    /**
     * 从字节数组解析下一个重做日志，此方法会将读取点推动 {@link #size()} 字节
     *
     * @param byteArray 字节数组
     * @return 重做日志，如果是 {@link #SPACE_REDO} ，则返回 null
     * @throws NoSuchElementException 读到末尾
     */
    static RedoLog parse(ByteArray byteArray) throws ParseException {
        if (!byteArray.hasNextInt()) {
            throw new NoSuchElementException();
        }
        byte type = byteArray.readByte(byteArray.getReadPos() + Integer.BYTES);
        return switch (type) {
            case SPACE_REDO -> {
                // 推进读指针
                int size = byteArray.readInt();
                byteArray.readByteArray(size);
                yield null;
            }
            case INSET_REDO, INSET_UNDO_REDO -> new InsertRedoLog(byteArray);
            case UPDATE_REDO -> new UpdateRedoLog(byteArray);
            case DELETE_REDO -> new DeleteRedoLog(byteArray);
            case TRANSACTION_ABORT -> new AbortRedoLog(byteArray);
            default -> throw new IllegalStateException("Unexpected value: " + type);
        };
    }
}
