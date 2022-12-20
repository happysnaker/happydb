package happydb.index;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.common.Debug;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.exception.DuplicateValueException;
import happydb.storage.*;
import happydb.transaction.TransactionId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @Author happysnaker
 * @Date 2022/11/22
 * @Email happysnaker@foxmail.com
 */
public class BTreePageHolderTest extends TestBase {
    String tb = "tb-0-" + IndexType.BTREE;
    BTreePageHolder holder;
    BTreeSuperPage superPage;

    @Before
    public void setUp() throws Exception {
        TableDesc td = TestUtil.createTableDesc(10, 0, 0,
                "tb",
                integer -> integer == 0 ? IndexType.indexSetToInt(Set.of(IndexType.BTREE)) : 0);

        Database.getCatalog().createTable(td);
        var pm = Database.getCatalog().getPageManager(tb);
        superPage = (BTreeSuperPage) pm.readPage(new PageId(tb, 0));
        this.holder = new BTreePageHolder(superPage, new TransactionId(0));
    }

    @Test
    public void testGetSuperPage() {
        BTreeSuperPage superPage = holder.getSuperPage(Permissions.READ_ONLY);
        Assert.assertEquals(superPage, holder.getSuperPage());
        Assert.assertTrue(Database.getBufferPool().pagePool.isEmpty());
    }


    @Test
    public void testReleaseAll() throws IOException, DbException {
        BTreeSuperPage superPage = holder.getSuperPage(Permissions.READ_WRITE);
        PageId malloc = superPage.malloc((byte) 1);
        BTreeLeafPage leafPage = (BTreeLeafPage) holder.getBTreePage(malloc, Permissions.READ_WRITE);

        Assert.assertEquals(tb, leafPage.getPageId().getTableName());

        RecordId recordId = new RecordId(new PageId("tb", 1), 1);
        leafPage.insertEntry(new BTreeLeafEntry(
                new IntField(1), recordId));
        leafPage.markDirty(true);

        Assert.assertTrue(Database.getBufferPool().holdsLock(new TransactionId(0), leafPage, Permissions.READ_WRITE));

        holder.releaseAllPages();

        BTreeLeafPage page = (BTreeLeafPage) Database.getCatalog()
                .getPageManager(superPage.getPageId().getTableName())
                .readPage(malloc);
        BTreeLeafEntry next = page.iterator().next();
        Assert.assertEquals(new IntField(1), next.getKey());
        Assert.assertEquals(recordId, next.getRecordId());
    }

    @Test
    public void acquireWriteReadLockByManyThread() throws Exception {
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        var bufferPool = Database.getBufferPool();

        PageId malloc = superPage.malloc((byte) 1);

        for (int i = 0; i < 512; i++) {
            int finalI = i;
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws Exception {
                    var h = new BTreePageHolder(superPage, new TransactionId(finalI));
                    // 前 256 获取写锁，后 256 获取读锁
                    if (finalI < 256) {
                        h.getBTreePage(malloc, Permissions.READ_WRITE);
                    } else {
                        h.getBTreePage(malloc, Permissions.READ_ONLY);
                    }
                    setDone(true);
                    Debug.log(finalI + " get " + malloc.toString() + " done.");
                }
            });
        }
        try {
            TestUtil.runManyThread(tasks, 10000);
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
}
