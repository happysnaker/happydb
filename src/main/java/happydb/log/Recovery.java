package happydb.log;

import happydb.common.Database;
import happydb.common.Debug;
import happydb.transaction.TransactionId;
import happydb.transaction.TransactionManager;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @Author happysnaker
 * @Date 2022/11/30
 * @Email happysnaker@foxmail.com
 */
public class Recovery {

    /**
     * 恢复例程
     */
    public static void recovery() throws Exception {
        Debug.log("Run recovery...");
        Set<TransactionId> set = new HashSet<>();

        // 重做所有页 LSN 小于 redo LSN 的页，由 redoIfNecessary 驱动
        Iterator<RedoLog> it = Database.getLogBuffer().iterator();
        while (it.hasNext()) {
            RedoLog next = it.next();
            next.redoIfNecessary();
            set.add(next.xid());
        }


        // 回滚所有未提交的事务
        TransactionManager tm = Database.getTransactionManager();
        for (TransactionId tid : tm.getActiveTransactions()) {
            Debug.log("Rollback uncommitted xid " + tid);
            tm.rollbackByRecovery(tid);

            // 交由重做日志进行重做，他必须重做任何页面，因此 LSN 被设置为无穷
            AbortRedoLog redoLog = new AbortRedoLog(tid);
            redoLog.setLsn(Long.MAX_VALUE);
            redoLog.redoIfNecessary();
            set.add(tid);
        }

        // 释放所有锁
        for (TransactionId tid : set) {
            Database.getBufferPool().transactionReleaseLock(tid);
        }

        // 执行一次模糊检查点推进
        Database.getCheckPoint().fuzzleCheckPoint();
    }
}
