package happydb.transaction;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.exception.DbException;
import happydb.exception.DeadLockException;
import happydb.storage.PageId;
import happydb.storage.RecordId;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

/**
 * @Author happysnaker
 * @Date 2022/12/3
 * @Email happysnaker@foxmail.com
 */
public class LockTableTest extends TestBase {
    LockTable lt = new LockTable();

    private void runThreadAndLock(TransactionId tid, RecordId recordId, boolean shouldAcquire) {
        TestUtil.TestRunnable task = new TestUtil.TestRunnable() {
            @Override
            public void run() throws Exception {
                lt.lock(tid, recordId);
                setDone(true);
            }
        };
        try {
            TestUtil.runManyThread(List.of(task), 1000 * 5);
            if (!shouldAcquire) {
                fail();
            }
        } catch (IllegalStateException e) {
            if (shouldAcquire) {
                fail();
            }
        }
    }


    /**
     * 事务 1 获取 x，事务 2 获取 x，事务 2 应该堵塞
     */
    @Test
    public void testLockSame() {
        TransactionId tid1 = new TransactionId(0);
        TransactionId tid2 = new TransactionId(1);
        RecordId r1 = new RecordId(new PageId("t", 1), 1);

        lt.lock(tid1, r1);
        runThreadAndLock(tid2, r1, false);
    }


    /**
     * 事务 1 获取 x，事务 2 获取 x，事务 2 应该堵塞，然后事务 1 释放锁，事务 2 应该成功获取锁
     */
    @Test
    public void testLockSameAndRelease() {
        TransactionId tid1 = new TransactionId(0);
        TransactionId tid2 = new TransactionId(1);
        RecordId r1 = new RecordId(new PageId("t", 1), 1);

        lt.lock(tid1, r1);
        runThreadAndLock(tid2, r1, false);

        lt.releaseAll(tid1);
        runThreadAndLock(tid2, r1, true);
    }


    /**
     * 事务 1 获取 x，事务 2 获取 y，事务 3 获取 z，然后事务 1 获取 y，事务 2 获取 z，事务 3 获取 x，这应该死锁
     */
    @Test
    public void testDeadLock() throws InterruptedException {
        TransactionId tid1 = new TransactionId(0);
        TransactionId tid2 = new TransactionId(1);
        TransactionId tid3 = new TransactionId(2);
        RecordId r1 = new RecordId(new PageId("t", 1), 1);
        RecordId r2 = new RecordId(new PageId("t", 1), 2);
        RecordId r3 = new RecordId(new PageId("t", 1), 3);

        lt.lock(tid1, r1);
        lt.lock(tid2, r2);
        lt.lock(tid3, r3);

        // tid1 lock r2
        new Thread(() -> lt.lock(tid1, r2)).start();
        // ensure tid1 lock r2
        Thread.sleep(1000);

        // tid2 lock r3
        new Thread(() -> lt.lock(tid2, r3)).start();
        // ensure tid2 lock r3
        Thread.sleep(1000);

        try {
            lt.lock(tid3, r1);
            fail();
        } catch (DeadLockException ignore) {
            // should dead lock
        }
    }


    /**
     * 多线程按顺序获取同一把把锁，不释放，最多只有一个线程能获取锁
     */
    @Test
    public void testLockSameByManyThread() {
        RecordId r = new RecordId(new PageId("t", 1), 1);
        int n = 10240;
        AtomicInteger sum = new AtomicInteger(0);
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    TransactionId tid = Database.getTransactionManager().begin();

                    lt.lock(tid, r);
                    sum.incrementAndGet();
                    setDone(true);
                }
            });
        }

        try {
            TestUtil.runManyThread(tasks, 1000 * 10);
            fail();
        } catch (IllegalStateException e) {
            Assert.assertEquals(1, sum.get());
        }
    }

    /**
     * 多线程按顺序获取 3 把锁，然后释放，不应该死锁，每个线程应该都能获取到锁
     */
    @Test
    public void testOrderedLockByManyThread() {
        RecordId r1 = new RecordId(new PageId("t", 1), 1);
        RecordId r2 = new RecordId(new PageId("t", 1), 2);
        RecordId r3 = new RecordId(new PageId("t", 1), 3);
        int n = 10240;
        // 不安全的集合
        Set<TransactionId> s1 = new HashSet<>();
        Set<TransactionId> s2 = new HashSet<>();
        Set<TransactionId> s3 = new HashSet<>();
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    TransactionId tid = Database.getTransactionManager().begin();

                    lt.lock(tid, r1);
                    s1.add(tid); // 由于获取了锁，应该能安全操作不安全的集合对象
                    lt.lock(tid, r2);
                    s2.add(tid);
                    lt.lock(tid, r3);
                    s3.add(tid);

                    lt.releaseAll(tid);
                    setDone(true);
                }
            });
        }

        TestUtil.runManyThread(tasks, 1000 * 60);
        Assert.assertEquals(s1.size(), n);
        Assert.assertEquals(s2.size(), n);
        Assert.assertEquals(s3.size(), n);
    }
}
