package happydb.storage;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.common.Debug;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.transaction.TransactionId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

/**
 * 此 Test 保证绝不会出现死锁，因为 HelloDb 中不会有死锁的发生
 *
 * @Author happysnaker
 * @Date 2022/11/18
 * @Email happysnaker@foxmail.com
 */
public class PageLockTest extends TestBase {

    int numPages = 100;

    long timeoutMills = 1000L;
    long longTimeoutMills = 1000L * 10L;

    BufferPool bufferPool;

    TransactionId tid1 = new TransactionId(0);
    TransactionId tid2 = new TransactionId(1);

    PageId p0 = new PageId("tb", 0), p1 = new PageId("tb", 1);

    @Before
    public void setUp() throws Exception {
        BufferPool.DEFAULT_PAGES = numPages;
        Database.reset();
        bufferPool = Database.getBufferPool();

        HeapPageTest test = new HeapPageTest();
        test.setUp();
        for (int i = 0; i < numPages; i++) {
            PageManager pm = Database.getCatalog().getPageManager("tb");
            HeapPage page = test.page;
            page.pid = new PageId("tb", i);
            pm.writePage(page);
        }
    }

    /**
     * 测试获取页面在页面并发驱逐条件下是否正确
     *
     * @throws Exception
     */
    @Test
    public void testManyThreadAcquireOnEvict() throws Exception {
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();


        for (int i = 0; i < BufferPool.DEFAULT_PAGES; i++) {
            bufferPool.getPage(tid1, new PageId("tb", i), Permissions.READ_ONLY);
        }
        bufferPool.transactionReleaseLock(tid1);
        AtomicInteger tDone = new AtomicInteger(0);
        for (int i = 0; i < 512; i++) {
            int finalI = i;
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    PageId pid = new PageId("tb", finalI % BufferPool.DEFAULT_PAGES);

                    if (Math.random() < 0.5) {
                        try {
                            bufferPool.evictPage(1, false, false, true);
                        } catch (DbException ignore) {

                        }
                    }
                    bufferPool.getPage(new TransactionId(finalI), pid, Permissions.READ_WRITE);

                    setDone(true);
                    int get = tDone.incrementAndGet();
                    System.out.println("tDone.incrementAndGet() = " + get + " " + pid);
                    if (get == numPages) {
                        System.out.println("success pass test!!!");
                    }
                }
            });
        }
        try {
            // 需要更长的时间
            TestUtil.runManyThread(tasks, 1000 * 45);
        } catch (IllegalStateException e) {
            e.printStackTrace();
            int sum = 0;
            for (TestUtil.TestRunnable task : tasks) {
                if (task.isDone()) {
                    sum++;
                }
            }
            Assert.assertEquals(BufferPool.DEFAULT_PAGES, sum);
        }
        Assert.assertEquals(numPages, bufferPool.pagePool.size());
    }

    @Test
    public void acquireWriteLockByManyThread() throws Exception {
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        for (int i = 0; i < 512; i++) {
            int finalI = i;
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    PageId pid = new PageId("tb", finalI % 10);
                    bufferPool.getPage(new TransactionId(finalI), pid, Permissions.READ_WRITE);
                    setDone(true);

                    Debug.log(finalI + " get " + pid.toString() + " done.");
                }
            });
        }
        try {
            TestUtil.runManyThread(tasks, longTimeoutMills);
        } catch (IllegalStateException e) {
            int sum = 0;
            for (TestUtil.TestRunnable task : tasks) {
                if (task.isDone()) {
                    sum++;
                }
            }
            Assert.assertEquals(10, sum);
        }
    }


    @Test
    public void acquireWriteReadLockByManyThread() throws Exception {
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        for (int i = 0; i < 512; i++) {
            int finalI = i;
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    PageId pid = new PageId("tb", 0);
                    // 前 256 获取写锁，后 256 获取读锁
                    if (finalI < 256) {
                        bufferPool.getPage(new TransactionId(finalI), pid, Permissions.READ_WRITE);
                    } else {
                        bufferPool.getPage(new TransactionId(finalI), pid, Permissions.READ_ONLY);
                    }
                    setDone(true);
                    Debug.log(finalI + " get " + pid.toString() + " done.");
                }
            });
        }
        try {
            TestUtil.runManyThread(tasks, longTimeoutMills);
        } catch (IllegalStateException e) {
            int sum = 0;
            for (int i = 0; i < 256; i++) {
                var task = tasks.get(i);
                if (task.isDone()) {
                    sum++;
                }
            }
            Assert.assertTrue(sum <= 1);
            if (sum == 0) {
                for (int i = 256; i < tasks.size(); i++) {
                    Assert.assertTrue(tasks.get(i).isDone());
                }
            }
        }
    }

    @Test(timeout = 20000)
    public void acquireLockOnEvict() throws Exception {
        bufferPool.getPage(tid1, p0, Permissions.READ_WRITE);
        new Thread(() -> {
            try {
                bufferPool.evictPage(1000 * 10L, true, false, true);
            } catch (DbException | IOException e) {
                throw new RuntimeException(e);
            }
        }).start();

        new Thread(() -> {
            try {
                Thread.sleep(1000 * 3L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            bufferPool.transactionReleaseLock(tid1);
        }).start();

        bufferPool.getPage(tid2, p0, Permissions.READ_WRITE);
        Assert.assertFalse(bufferPool.pagePool.isEmpty());
    }

    @Test
    public void acquireLockOnSameTid() throws Exception {
        assertHasLock(tid1, p0, Permissions.READ_WRITE,
                tid1, p0, Permissions.READ_ONLY, true, timeoutMills);

        assertHasLock(tid2, p1, Permissions.READ_WRITE,
                tid2, p1, Permissions.READ_WRITE, true, timeoutMills);
    }


    @Test
    public void acquireReadLocksOnSamePage() throws Exception {
        assertHasLock(tid1, p0, Permissions.READ_ONLY,
                tid2, p0, Permissions.READ_ONLY, true, timeoutMills);
    }


    @Test
    public void acquireReadWriteLocksOnSamePage() throws Exception {
        assertHasLock(tid1, p0, Permissions.READ_ONLY,
                tid2, p0, Permissions.READ_WRITE, false, timeoutMills);
    }


    @Test
    public void acquireWriteReadLocksOnSamePage() throws Exception {
        assertHasLock(tid1, p0, Permissions.READ_WRITE,
                tid2, p0, Permissions.READ_ONLY, false, timeoutMills);
    }


    @Test
    public void acquireReadWriteLocksOnTwoPages() throws Exception {
        assertHasLock(tid1, p0, Permissions.READ_ONLY,
                tid2, p1, Permissions.READ_WRITE, true, timeoutMills);
    }


    @Test
    public void acquireWriteLocksOnTwoPages() throws Exception {
        assertHasLock(tid1, p0, Permissions.READ_WRITE,
                tid2, p1, Permissions.READ_WRITE, true, timeoutMills);
    }


    @Test
    public void acquireReadLocksOnTwoPages() throws Exception {
        assertHasLock(tid1, p0, Permissions.READ_ONLY,
                tid2, p1, Permissions.READ_ONLY, true, timeoutMills);
    }


    @Test
    public void lockUpgrade() throws Exception {
        assertHasLock(tid1, p0, Permissions.READ_ONLY,
                tid1, p0, Permissions.READ_WRITE, true, timeoutMills);
        assertHasLock(tid2, p1, Permissions.READ_ONLY,
                tid2, p1, Permissions.READ_WRITE, true, timeoutMills);
    }


    @Test
    public void acquireWriteAndReadLocks() throws Exception {
        assertHasLock(tid1, p0, Permissions.READ_WRITE,
                tid1, p0, Permissions.READ_ONLY, true, timeoutMills);
    }


    @Test(timeout = 3000L)
    public void acquireThenRelease() throws Exception {
        bufferPool.unsafeReleasePage(tid1, bufferPool.getPage(tid1, p0, Permissions.READ_WRITE), Permissions.READ_WRITE);
        Page page0 = bufferPool.getPage(tid2, p0, Permissions.READ_WRITE);


        bufferPool.unsafeReleasePage(tid2, bufferPool.getPage(tid2, p1, Permissions.READ_WRITE), Permissions.READ_WRITE);
        Page page1 = bufferPool.getPage(tid1, p1, Permissions.READ_WRITE);

        Assert.assertTrue(bufferPool.holdsLock(tid1, page1, Permissions.READ_WRITE));
        Assert.assertTrue(bufferPool.holdsLock(tid2, page0, Permissions.READ_WRITE));
        bufferPool.transactionReleaseLock(tid1);
        bufferPool.transactionReleaseLock(tid2);
        Assert.assertFalse(bufferPool.holdsLock(tid1, page1, Permissions.READ_WRITE));
        Assert.assertFalse(bufferPool.holdsLock(tid2, page0, Permissions.READ_WRITE));
    }


    /**
     * 一个便捷方法，此方法判断 t1 以指定权限获取锁后，p2 是否能够正确获得锁，expected 指示 t2 是否需要获取锁，如果不正确，那么将 assert 失败
     * <p>t1 允许为 null</p>
     */
    public static void assertHasLock(TransactionId t1, PageId p1, Permissions perm1,
                              TransactionId t2, PageId p2, Permissions perm2, boolean expected, long timeoutMills) throws DbException {
        var pool = Database.getBufferPool();
        if (t1 != null) {
            pool.getPage(t1, p1, perm1);
        }

        try {
            TestUtil.runManyThread(List.of(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    Page page = pool.getPage(t2, p2, perm2);
                    if (expected) {
                        setDone(true);
                    }
                }
            }), timeoutMills);
        } catch (IllegalStateException e) {
            // 没能获取到指定锁
            e.printStackTrace();
            if (expected) {
                fail("验证失败");
            }
        }
    }
}
