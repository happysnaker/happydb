package happydb.log;

import happydb.storage.Record;
import happydb.storage.RecordId;

/**
 * Purge 线程，负责清理逻辑删除的行记录和 undo log
 * <P>此类的实现不是必要的，旨在学习 MVCC 与逻辑删除之间的关系</P>
 * @Author happysnaker
 * @Date 2022/11/29
 * @Email happysnaker@foxmail.com
 */
public class Purge {

    public static void addLogicalDeleteRecord(RecordId record) {

    }


    public static void addLogicalDeleteUndoLog(UndoLogId log) {

    }
}
