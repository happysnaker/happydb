package happydb.transaction;

import happydb.common.Database;
import happydb.storage.Record;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @param trxList    活跃事务列表
 * @param upLimitId  活跃的事务列表中最小的事务 ID
 * @param lowLimitId 尚未分配的下一个事务 ID
 * @param currentId  申请 ReadView 的事务 ID
 * @Author happysnaker
 * @Date 2022/12/3
 * @Email happysnaker@foxmail.com
 */
public record ReadView(@Getter Set<TransactionId> trxList, @Getter TransactionId upLimitId,
                       @Getter TransactionId lowLimitId, @Getter TransactionId currentId) {

    /**
     * 判断记录是否对此读视图可见
     * @param record 记录
     * @return 真则可见
     */
    public boolean isVisible(Record record) {
        // 超级事务，永远可见
        if (currentId.getXid() < 0) {
            return true;
        }

        TransactionId x = record.getLastModify();
        if (x == null) {
            throw new IllegalArgumentException("此记录的事务 ID 为空");
        }

        if (x.equals(currentId)) {
            return true;
        } else if (x.getXid() >= lowLimitId().getXid()) {
            return false;
        } else if (x.getXid() < upLimitId.getXid()) {
            return true;
        } else {
            return !trxList().contains(x);
        }
    }


    public static final int READ_COMMIT = 1;

    public static final int READ_REPEAT = 2;

    /**
     * 当前内存中的版本快照，以访问顺序排序
     */
    static final Map<TransactionId, ReadView> readViewMap = new LinkedHashMap<>(16, 0.75f, true);


    /**
     * 事务完成时需要移除
     *
     * @param tid
     */
    public synchronized static void release(TransactionId tid) {
        readViewMap.remove(tid);
    }

    /**
     * 静态方法，获取读视图，这应该由所有运算符的源头 {@link happydb.execution.BTreeSeqScan} 调用 <P>
     * <p>
     * 此方法会根据隔离级别不同使用不同的逻辑：
     * <ul>
     *     <li>对于读已提交，此方法总是会创建最新的版本快照</li>
     *     <li>对于可重复读，此方法总是会返回第一次为事务创建的版本快照</li>
     * </ul>
     *
     * @param tid   调用事务
     * @param level 隔离级别，{@link #READ_COMMIT}、{@link #READ_REPEAT}
     * @return 合适的版本快照
     */
    public static synchronized ReadView createReadView(TransactionId tid, int level) {
        if (level != READ_COMMIT && level != READ_REPEAT) {
            throw new IllegalArgumentException("不支持的隔离级别 " + level);
        }
        ReadView rv = readViewMap.get(tid);
        if (rv != null && level == READ_REPEAT) {
            return rv;
        }

        long limitId = Database.getTransactionManager().getLowLimitId();
        Set<TransactionId> set = Database.getTransactionManager().getActiveTransactions();
        rv = new ReadView(
                set.stream().filter(p -> p.getXid() < limitId).collect(Collectors.toSet()),
                set.isEmpty() ? new TransactionId(-1) :
                        set.stream().sorted((a, b) -> (int) (a.getXid() - b.getXid())).iterator().next(),
                new TransactionId(limitId),
                tid
        );
        readViewMap.put(tid, rv);
        return rv;
    }

    /**
     * 获取最早创建并且尚未回收的读视图，供 {@link happydb.log.Purge} 线程使用
     */
    public synchronized ReadView getEarliestReadView() {
        if (readViewMap.isEmpty()) {
            return null;
        }
        ReadView rv = null;
        for (Map.Entry<TransactionId, ReadView> it : readViewMap.entrySet()) {
            rv = it.getValue();
        }
        return rv;
    }
}
