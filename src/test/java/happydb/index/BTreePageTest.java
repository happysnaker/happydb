package happydb.index;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.exception.DuplicateValueException;
import happydb.storage.*;
import happydb.transaction.TransactionId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.fail;

/**
 * @Author happysnaker
 * @Date 2022/11/21
 * @Email happysnaker@foxmail.com
 */
public class BTreePageTest extends TestBase {

    BufferPool pool;

    String tb = "tb-0-" + IndexType.BTREE;

    PageManager pm;

    @Before
    public void setUp() throws DuplicateValueException, IOException, DbException {
        pool = Database.getBufferPool();
        TableDesc td = TestUtil.createTableDesc(10, 0, 0,
                "tb",
                integer -> integer == 0 ? IndexType.indexSetToInt(Set.of(IndexType.BTREE)) : 0);

        Database.getCatalog().createTable(td);

        pm = Database.getCatalog().getPageManager(tb);
    }

    @Test
    public void testRwSuperPage() throws DbException, IOException {
        BTreeSuperPage page = (BTreeSuperPage) pool.getPage(new TransactionId(0),
                new PageId(tb, 0), Permissions.READ_WRITE);
        Assert.assertNotNull(page);

        PageId pid = page.malloc(BTreePage.LEAF);
        List<Integer> emptySlots = page.getEmptySlots();

        Assert.assertFalse(emptySlots.contains(pid.getPageNumber()));

        pm.writePage(page);

        page = (BTreeSuperPage) pm.readPage(page.getPageId());
        Assert.assertNotNull(page);
        Assert.assertEquals(emptySlots, page.getEmptySlots());
        Assert.assertEquals(tb, page.getPageId().getTableName());

        PageId malloc = page.malloc((byte) 1);
        page = (BTreeSuperPage) pm.readPage(page.getPageId());
        Assert.assertFalse(page.getEmptySlots().contains(malloc.getPageNumber()));
    }


    @Test
    public void testRwInternalPage() throws DbException, IOException {
        BTreeSuperPage superPage = (BTreeSuperPage) pool.getPage(new TransactionId(0),
                new PageId(tb, 0), Permissions.READ_WRITE);
        Assert.assertNotNull(superPage);

        PageId pid = superPage.malloc(BTreePage.INTERNAL);
        BTreeInternalPage page = (BTreeInternalPage) pool.getPage(new TransactionId(0), pid, Permissions.READ_WRITE);
        Assert.assertNotNull(page);
        Assert.assertEquals(BTreePage.INTERNAL, page.category);
        Assert.assertEquals(superPage.getType(), page.getType());
        Assert.assertEquals(page.getMaxNumEntries(), page.getEmptySlots().size());

        BTreeInternalEntry e = new BTreeInternalEntry(
                new IntField(5), new PageId(pid.getTableName(), 3), new PageId(pid.getTableName(), 4));
        page.insertEntry(e);

        pm.writePage(page);
        page = (BTreeInternalPage) pm.readPage(page.getPageId());
        Assert.assertNotNull(page);
        Assert.assertEquals(BTreePage.INTERNAL, page.category);
        Assert.assertEquals(superPage.getType(), page.getType());
        Assert.assertEquals(page.getMaxNumEntries() - 1, page.getEmptySlots().size());
        Assert.assertEquals(1, page.getEntryAr().length);
        Assert.assertEquals(4, page.getEntryAr()[0].getRightChild().getPageNumber());
    }


    @Test
    public void testRwLeafPage() throws DbException, IOException {
        BTreeSuperPage superPage = (BTreeSuperPage) pool.getPage(new TransactionId(0),
                new PageId(tb, 0), Permissions.READ_WRITE);
        Assert.assertNotNull(superPage);

        PageId pid = superPage.malloc(BTreePage.LEAF);
        BTreeLeafPage page = (BTreeLeafPage) pool.getPage(new TransactionId(0), pid, Permissions.READ_WRITE);
        Assert.assertNotNull(page);
        Assert.assertEquals(BTreePage.LEAF, page.category);
        Assert.assertEquals(superPage.getType(), page.getType());
        Assert.assertEquals(page.getMaxNumEntries(), page.getEmptySlots().size());


        var e = new BTreeLeafEntry(new IntField(1), new RecordId(pid, 0));
        page.insertEntry(e);

        pm.writePage(page);
        page = (BTreeLeafPage) pm.readPage(page.getPageId());
        Assert.assertNotNull(page);
        Assert.assertEquals(BTreePage.LEAF, page.category);
        Assert.assertEquals(superPage.getType(), page.getType());
        Assert.assertEquals(page.getMaxNumEntries() - 1, page.getEmptySlots().size());
        Assert.assertEquals(1, page.getEntryAr().length);
    }


    @Test
    public void testUpdateLeafPageEntry() throws DbException, IOException {
        BTreeSuperPage superPage = (BTreeSuperPage) pool.getPage(new TransactionId(0),
                new PageId(tb, 0), Permissions.READ_WRITE);
        Assert.assertNotNull(superPage);

        PageId pid = superPage.malloc(BTreePage.LEAF);
        BTreeLeafPage page = (BTreeLeafPage) pool.getPage(new TransactionId(0), pid, Permissions.READ_WRITE);

        RecordId recordId = new RecordId(pid, 0);
        var e1 = new BTreeLeafEntry(new IntField(1), recordId);
        var e2 = new BTreeLeafEntry(new IntField(2), recordId);
        var e3 = new BTreeLeafEntry(new IntField(2), recordId);

        page.insertEntry(e3);
        page.insertEntry(e2);
        page.insertEntry(e1);

        BTreeLeafEntry[] entryAr = page.getEntryAr();
        for (int i = 0; i < entryAr.length; i++) {
            Assert.assertEquals(new IntField(Math.min(i + 1, 2)), entryAr[i].getKey());
        }
    }

    @Test
    public void testUpdateInternalPageEntry() throws DbException, IOException {
        BTreeSuperPage superPage = (BTreeSuperPage) pool.getPage(new TransactionId(0),
                new PageId(tb, 0), Permissions.READ_WRITE);
        Assert.assertNotNull(superPage);

        PageId pid = superPage.malloc(BTreePage.INTERNAL);
        BTreeInternalPage page = (BTreeInternalPage) pool.getPage(new TransactionId(0), pid, Permissions.READ_WRITE);

        BTreeInternalEntry e1 = new BTreeInternalEntry(
                new IntField(5), new PageId(pid.getTableName(), 3), new PageId(pid.getTableName(), 4));
        BTreeInternalEntry e2 = new BTreeInternalEntry(
                new IntField(4), new PageId(pid.getTableName(), 2), new PageId(pid.getTableName(), 3));
        BTreeInternalEntry e3 = new BTreeInternalEntry(
                new IntField(3), new PageId(pid.getTableName(), 1), new PageId(pid.getTableName(), 2));


        BTreeInternalEntry e4 = new BTreeInternalEntry(
                new IntField(9), new PageId(pid.getTableName(), 20), new PageId(pid.getTableName(), 30));
        page.insertEntry(e1);
        page.insertEntry(e2);
        page.insertEntry(e3);

        try {
            page.insertEntry(e4);
            fail();
        } catch (Exception e) {
            e.printStackTrace();
        }

        BTreeInternalEntry[] entryAr = page.getEntryAr();
        Assert.assertEquals(e3.getKey(), entryAr[0].getKey());
        Assert.assertEquals(e2.getKey(), entryAr[1].getKey());
        Assert.assertEquals(e1.getKey(), entryAr[2].getKey());

        page.deleteKeyAndLeftChild(entryAr[1]);
        Assert.assertEquals(new IntField(3), page.iterator().next().getKey());
        Assert.assertEquals(3, page.iterator().next().getRightChild().getPageNumber());


        BTreeInternalEntry u = new BTreeInternalEntry(
                new IntField(5), new PageId(pid.getTableName(), 6), new PageId(pid.getTableName(), 9));
        u.setEntryId(entryAr[2].getEntryId());
        page.updateEntry(u);
        entryAr = page.getEntryAr();
        Assert.assertEquals(6, entryAr[0].getRightChild().getPageNumber());
        Assert.assertEquals(9, entryAr[1].getRightChild().getPageNumber());
    }

}
