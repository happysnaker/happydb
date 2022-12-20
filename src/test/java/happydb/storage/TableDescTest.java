package happydb.storage;

import happydb.TestUtil;
import happydb.common.Debug;
import happydb.index.IndexType;
import org.junit.Assert;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.Set;

import static happydb.storage.Type.*;

/**
 * @Author happysnaker
 * @Date 2022/11/16
 * @Email happysnaker@foxmail.com
 */
public class TableDescTest {

    @Test
    public void testMerge() {
        TableDesc td1 = TestUtil.createTableDesc(100, 200, 300, "tb1", null);
        TableDesc td2 = TestUtil.createTableDesc(10, 20, 30, "tb2", null);

        TableDesc td = TableDesc.merge(td1, td2);
        Debug.log(String.valueOf(td));

        Assert.assertEquals(110L * INT_TYPE.getLen() + 220L * DOUBLE_TYPE.getLen() + 330 * (STRING_LEN + 4), td.getRecordSize());
        Assert.assertEquals(660, td.numFields());

        for (int i = 0; i < 660; i++) {
            if (i < 100) {
                Assert.assertEquals(INT_TYPE, td.getFieldType(i));
            } else if (i < 300) {
                Assert.assertEquals(DOUBLE_TYPE, td.getFieldType(i));
            } else if (i < 600) {
                Assert.assertEquals(STRING_TYPE, td.getFieldType(i));
            } else if (i < 610) {
                Assert.assertEquals(INT_TYPE, td.getFieldType(i));
            } else if (i < 630) {
                Assert.assertEquals(DOUBLE_TYPE, td.getFieldType(i));
            } else {
                Assert.assertEquals(STRING_TYPE, td.getFieldType(i));
            }
        }
    }

    @Test
    public void testFindByIndex() {
        TableDesc td = TestUtil.createTableDesc(100, 200, 300, "tb", null);
        Debug.log(String.valueOf(td));
        for (int i = 0; i < 600; i++) {
            if (i < 100) {
                Assert.assertEquals(INT_TYPE, td.getFieldType(i));
                Assert.assertEquals(0, td.getIndexType(i));
                Assert.assertEquals(String.valueOf(i), td.getFieldName(i));
            } else if (i < 300) {
                Assert.assertEquals(DOUBLE_TYPE, td.getFieldType(i));
                Assert.assertEquals(0, td.getIndexType(i));
                Assert.assertEquals(String.valueOf(i), td.getFieldName(i));
            } else {
                Assert.assertEquals(STRING_TYPE, td.getFieldType(i));
                Assert.assertEquals(0, td.getIndexType(i));
                Assert.assertEquals(String.valueOf(i), td.getFieldName(i));
            }
        }
    }


    @Test
    public void testFindByFieldName() {
        TableDesc td = TestUtil.createTableDesc(100, 200, 300, "tb", null);
        Debug.log(String.valueOf(td));
        for (int i = 0; i < 600; i++) {
            Assert.assertEquals(i, td.fieldNameToIndex(String.valueOf(i)));
        }
    }


    @Test
    public void testFindPrimaryKey() {
        TableDesc td = TestUtil.createTableDesc(100, 200, 300, "tb", null);

        for (int i = 0; i < 600; i++) {
            td.getItems()[i] = new TableDesc.TDItem(INT_TYPE, "", 0);
        }

        int indexType = IndexType.indexSetToInt(Set.of(IndexType.PRIMARY_KEY, IndexType.HASH));

        td.getItems()[250] = new TableDesc.TDItem(INT_TYPE, "", indexType);
        Assert.assertEquals(250, td.getPrimaryKeyFieldIndex());

        td.getItems()[250] = new TableDesc.TDItem(INT_TYPE, "", 0);
        td.getItems()[255] = new TableDesc.TDItem(INT_TYPE, "", indexType);
        Assert.assertNotEquals(250, td.getPrimaryKeyFieldIndex());
        Assert.assertEquals(255, td.getPrimaryKeyFieldIndex());

        td.getItems()[255] = new TableDesc.TDItem(INT_TYPE, "", 0);
        try {
            td.getPrimaryKeyFieldIndex();
            Assert.fail("不应该找到任何元素");
        } catch (NoSuchElementException ignore) {
            // ignore
        }
    }
}
