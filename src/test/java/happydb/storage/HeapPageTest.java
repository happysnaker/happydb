package happydb.storage;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.common.Database;
import happydb.common.Debug;
import happydb.exception.DbException;
import happydb.exception.DuplicateValueException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
public class HeapPageTest extends TestBase {
    HeapPage page = null;
    int n1 = 1, n2 = 2, n3 = 1;
    TableDesc td = TestUtil.createTableDesc(n1, n2, n3, "tb", null);
    Record[] records;

    @Before
    public void setUp() throws DuplicateValueException, IOException, DbException {
        try {
            Database.getCatalog().getTableDesc(td.getTableName());
        } catch (NoSuchElementException e) {
            Database.getCatalog().createTable(td);
        }

        page = new HeapPage(new PageId("tb", 0));
//        page.setPageId();

        int maxNumEntries = page.getMaxNumEntries();
        assert maxNumEntries > 0;
        Debug.log("maxNumEntries = %d", maxNumEntries);
        records = new Record[maxNumEntries];
        for (int i = 0; i < maxNumEntries; i++) {
            records[i] = TestUtil.createRecord(n1, n2, n3, "tb");
            page.insertRecord(i, records[i]);
        }
        Assert.assertEquals(new PageId("tb", 0), page.getPageId());
    }

    @Test
    public void testOpRecord() throws DbException {
        Assert.assertEquals(0, page.getEmptySlots().size());

        for (int i = 0; i < records.length; i++) {
            TestUtil.assertRecordEquals(records[i], page.readRecord(new RecordId(page.getPageId(), i)), true);
        }

        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < records.length; i += 3) {
            page.deleteRecord(new RecordId(page.getPageId(), i));
            list.add(i);
        }

        Assert.assertEquals(list.size(), page.getEmptySlots().size());
        Assert.assertEquals(list, page.getEmptySlots());

        try {
            page.deleteRecord(new RecordId(page.getPageId(), 0));
            Assert.fail();
        } catch (DbException ignore) {

        }
    }


    @Test
    public void testReadWrite() throws DbException, IOException {
        Assert.assertEquals(0, page.getEmptySlots().size());

        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < records.length; i += 3) {
            page.deleteRecord(new RecordId(page.getPageId(), i));
            list.add(i);
        }

        PageManager pageManager = Database.getCatalog().getPageManager("tb");

        pageManager.writePage(page);

        HeapPage page1 = (HeapPage) pageManager.readPage(page.getPageId());
        Assert.assertEquals(list, page1.getEmptySlots());

        for (int i = 1; i < records.length; i += 3) {
            TestUtil.assertRecordEquals(records[i], page1.readRecord(new RecordId(page.getPageId(), i)), true);
        }
    }
}
