package happydb.transaction;

import happydb.storage.RecordId;
import lombok.Getter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 行记录锁
 *
 * @Author happysnaker
 * @Date 2022/12/3
 * @Email happysnaker@foxmail.com
 */
public class RecordLock {


    private final static Map<RecordId, RecordLock> recordLockPool = new HashMap<>();
    private final static Map<RecordId, Set<TransactionId>> referenceMap = new HashMap<>();
    public static final int POOL_SIZE_HOLDER = 1000;

    /**
     * {@link RecordLock} 是一个可变的对象，我们绝不能在可变对象上加锁，此方法将 recordId 映射到池中不可变的对象中，
     * 这是获取 {@link RecordLock} 的唯一入口。
     * <p>
     * 为了防止池无限制的增长，当池的大小超过一定限制时，我们将释放那些没有事务引用的对象，调用此方法会增加对 {@link RecordLock} 的引用计数
     * <p>
     * 使用池化技术有许多好处，例如可以确保事务引用的是同一个对象、例如可以避免频繁创建锁对象
     *
     * @param recordId 想要获取的行记录锁对象
     * @param tid 待引用的事务
     * @return 行记录锁对象
     */
    public static RecordLock getInstance(RecordId recordId, TransactionId tid) {
        synchronized (RecordLock.recordLockPool) {
            RecordLock lock = recordLockPool.get(recordId);
            if (lock != null) {
                referenceMap.putIfAbsent(recordId, new HashSet<>());
                referenceMap.get(recordId).add(tid);
                return lock;
            }

            lock = new RecordLock(recordId);

            if (recordLockPool.size() >= POOL_SIZE_HOLDER) {
                for (RecordId rid : new HashSet<>(recordLockPool.keySet())) {
                    if (referenceMap.get(rid) == null || referenceMap.get(rid).isEmpty()) {
                        recordLockPool.remove(rid);
                    }
                }
            }

            recordLockPool.put(recordId, lock);
            referenceMap.putIfAbsent(recordId, new HashSet<>());
            referenceMap.get(recordId).add(tid);
            return lock;
        }
    }

    /**
     * 释放事务对{@link RecordLock}的引用，以便他们可以被回收，此方法仅在检测到死锁或事务完成时调用
     */
    public static void free(RecordId rid, TransactionId tid) {
        synchronized (recordLockPool) {
            if (!referenceMap.getOrDefault(rid, new HashSet<>()).contains(tid)) {
                throw new IllegalMonitorStateException();
            }
            referenceMap.getOrDefault(rid, new HashSet<>()).remove(tid);
        }
    }

    @Getter
    private final RecordId recordId;

    private RecordLock(RecordId recordId) {
        this.recordId = recordId;
    }

    private volatile TransactionId owner = null;


    /**
     * 判断给定事务是否持有锁
     *
     * @param tid 事务
     */
    public boolean holdLock(TransactionId tid) {
        try {
            return owner != null && owner.equals(tid);
        } catch (NullPointerException e) {
            return false;
        }
    }

    /**
     * 事务尝试获取锁，获取失败时返回而不是堵塞，<b>同一个事务应该要串行调用</b>
     *
     * @param tid 事务
     * @return 真获取成功，假获取失败
     */
    public boolean tryLock(TransactionId tid) {
        if (holdLock(tid)) {
            return true;
        }
        synchronized (this) {
            if (owner == null) {
                owner = tid;
                return true;
            }
            return false;
        }
    }

    /**
     * 事务获取锁，这会导致堵塞，<b>同一个事务应该要串行调用</b>
     *
     * @param tid
     */
    public void lock(TransactionId tid) {
        if (holdLock(tid)) {
            return;
        }
        synchronized (this) {
            while (owner != null) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            owner = tid;
        }
    }


    /**
     * 事务释放锁，这应该要在事务结束时调用
     *
     * @param tid 事务
     * @throws IllegalMonitorStateException 如果事务未持有锁
     */
    public void unLock(TransactionId tid) throws IllegalMonitorStateException {
        if (!holdLock(tid)) {
            throw new IllegalMonitorStateException();
        }
        synchronized (this) {
            owner = null;
            notifyAll();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RecordLock that)) return false;

        return getRecordId().equals(that.getRecordId());
    }

    @Override
    public int hashCode() {
        return getRecordId().hashCode();
    }
}
