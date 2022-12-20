package happydb.transaction;

import happydb.exception.DeadLockException;
import happydb.storage.RecordId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 此类是 Db 中行记录锁获取与释放的入口
 *
 * @Author happysnaker
 * @Date 2022/12/3
 * @Email happysnaker@foxmail.com
 */
public class LockTable {
    /**
     * 表示事务持有那些锁
     */
    private final Map<TransactionId, Set<RecordLock>> transactionHoldMap = new ConcurrentHashMap<>();
    /**
     * 表示行记录被哪个事务所占有
     */
    private final Map<RecordId, TransactionId> lockTable = new ConcurrentHashMap<>();
    /**
     * 事务正在等待哪些记录
     */
    private final Map<TransactionId, RecordId> waitTable = new ConcurrentHashMap<>();


    /**
     * 判断当前图中是否存在死锁
     * <P>由于要获取当前图的一致性快照，因此需要加锁</P>
     *
     * @return 是否含有死锁
     */
    private synchronized boolean hasDeadLock() {
        // 生成等待图，边 x-> y 表示事务 x 正在等待事务 y
        // 拓扑排序检测环
        Map<TransactionId, Set<TransactionId>> graph = new HashMap<>();
        Map<TransactionId, Integer> degree = new HashMap<>(); // 入度
        for (Map.Entry<TransactionId, RecordId> it : waitTable.entrySet()) {
            TransactionId hold = lockTable.get(it.getValue());
            if (hold != null) {
                graph.putIfAbsent(it.getKey(), new HashSet<>());
                graph.putIfAbsent(hold, new HashSet<>());       // 让 graph 存储所有的节点
                graph.get(it.getKey()).add(hold);
                degree.put(hold, degree.getOrDefault(hold, 0) + 1);
            }
        }

        // 存储所有入度为 0 的节点
        Deque<TransactionId> stk = new ArrayDeque<>();
        for (TransactionId tid : graph.keySet()) {
            if (degree.getOrDefault(tid, 0) == 0) {
                stk.push(tid);
            }
        }
        int count = 0;
        while (!stk.isEmpty()) {
            TransactionId pop = stk.pop();
            count++;
            for (TransactionId y : graph.get(pop)) {
                degree.put(y, degree.getOrDefault(y, 0) - 1);
                if (degree.get(y) == 0) {
                    stk.push(y);
                }
            }
        }
        return count != graph.size();
    }

    /**
     * 事务尝试在行记录上获取一个锁
     *
     * @param recordId 行记录
     * @throws DeadLockException 如果检测到死锁
     */
    public void lock(TransactionId tid, RecordId recordId) throws DeadLockException {
        RecordLock lock = RecordLock.getInstance(recordId, tid);
        if (lock.tryLock(tid)) {
            transactionHoldMap.putIfAbsent(tid, new HashSet<>());
            transactionHoldMap.get(tid).add(lock);
            lockTable.put(recordId, tid);
            return;
        }

        waitTable.put(tid, recordId);
        if (hasDeadLock()) {
            // 死锁发生
            waitTable.remove(tid);
            RecordLock.free(recordId, tid);
            throw new DeadLockException(tid + " try lock " + recordId + ", but deadlock happen.");
        }

        // 堵塞获取锁
        lock.lock(tid);

        // 获取锁成功
        waitTable.remove(tid);
        transactionHoldMap.putIfAbsent(tid, new HashSet<>());
        transactionHoldMap.get(tid).add(lock);
        lockTable.put(recordId, tid);
    }


    /**
     * 事务结束时需要释放所有的行记录锁
     *
     * @param tid 事务
     */
    public void releaseAll(TransactionId tid) {
        for (var lock : new HashSet<>(transactionHoldMap.getOrDefault(tid, new HashSet<>()))) {
            lock.unLock(tid);
            transactionHoldMap.get(tid).remove(lock);
            lockTable.remove(lock.getRecordId());
            RecordLock.free(lock.getRecordId(), tid);
        }
    }

    /**
     * 事务释放某个记录上的锁，这是个危险的操作
     * @param tid 事务
     * @param recordId 记录
     * @throws IllegalMonitorStateException 如果未持有锁
     */
    public void unsafeUnLock(TransactionId tid, RecordId recordId) throws IllegalMonitorStateException {
        var lock = RecordLock.getInstance(recordId, tid);
        if (lock.holdLock(tid)) {
            throw new IllegalMonitorStateException();
        }
        lock.unLock(tid);
        transactionHoldMap.get(tid).remove(lock);
        lockTable.remove(lock.getRecordId());
        RecordLock.free(lock.getRecordId(), tid);
    }

    /**
     * 事务是否持有锁
     */
    public boolean holdLock(TransactionId tid, RecordId recordId) {
        return RecordLock.getInstance(recordId, tid).holdLock(tid);
    }


    /**
     * 行记录被谁锁定
     * @param recordId 记录
     * @return 锁定的事务，可能 null
     */
    public TransactionId holdLock(RecordId recordId) {
        return lockTable.get(recordId);
    }
}
