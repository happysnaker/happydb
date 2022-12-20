package happydb.common;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.exception.DbException;
import happydb.exception.DuplicateValueException;
import happydb.index.IndexType;
import happydb.storage.PageManager;
import happydb.storage.TableDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;

import static happydb.storage.Type.INT_TYPE;
import static happydb.storage.Type.STRING_TYPE;
import static org.junit.Assert.fail;

/**
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
public class CatalogTest extends TestBase {

    Catalog catalog;
    int nums = 1000;

    @Before
    public void setup() throws Exception {
        catalog = Database.getCatalog();
        for (int i = 0; i < nums; i++) {
            catalog.createTable(TestUtil.createTableDesc(0, 0, nums, String.valueOf(i), null));
        }
    }

    @Test
    public void testFindByTableName() throws Exception {
        for (int i = nums - 1; i >= 0; i--) {
            String tableName = String.valueOf(i);
            TableDesc tableDesc = catalog.getTableDesc(tableName);
            PageManager pageManager = catalog.getPageManager(tableName);

            Assert.assertEquals(0, tableDesc.getIndexType(i));
            Assert.assertEquals(tableName, tableDesc.getFieldName(i));
            Assert.assertEquals(STRING_TYPE, tableDesc.getFieldType(i));
            Assert.assertEquals(tableName, tableDesc.getTableName());
            Assert.assertEquals(tableName, pageManager.getTableName());
        }
    }


    @Test
    public void testSerializableAndLoad() throws Exception {
        catalog = new Catalog(new DbFile(Database.getDbFile("catalog")));
        catalog.loadCatalog();
        for (int i = nums - 1; i >= 0; i--) {
            String tableName = String.valueOf(i);
            TableDesc tableDesc = catalog.getTableDesc(tableName);
            PageManager pageManager = catalog.getPageManager(tableName);

            Assert.assertEquals(0, tableDesc.getIndexType(i));
            Assert.assertEquals(tableName, tableDesc.getFieldName(i));
            Assert.assertEquals(STRING_TYPE, tableDesc.getFieldType(i));
            Assert.assertEquals(tableName, tableDesc.getTableName());
            Assert.assertEquals(tableName, pageManager.getTableName());
        }
    }

    @Test
    public void testManyThreadCreate() {
        int n = 512;
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            int finalI = i;
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() throws DuplicateValueException, IOException, DbException {
                    catalog.createTable(TestUtil.createTableDesc(1, 0, 0, String.valueOf(finalI + nums), null));
                    setDone(true);
                }
            });
        }
        TestUtil.runManyThread(tasks, 1000 * 60);
        Assert.assertEquals(n + nums, catalog.catalogMap.size());
        for (int i = 0; i < n; i++) {
            Assert.assertEquals(INT_TYPE, catalog.getTableDesc(String.valueOf(i + nums)).getFieldType(0));
        }
    }


    @Test
    public void testFindIndex() throws DuplicateValueException, IOException, DbException {

        TableDesc td = TestUtil.createTableDesc(0, 0, nums, "tb", new Function<Integer, Integer>() {
            @Override
            public Integer apply(Integer integer) {
                return integer == 1 ? IndexType.indexSetToInt(Set.of(IndexType.BTREE)) : 0;
            }
        });

        catalog.createTable(td);

        Assert.assertNotNull(catalog.getIndex("tb", 1, IndexType.BTREE));

        try {
            catalog.getIndex("tb", 1, IndexType.HASH);
            fail();
        } catch (NoSuchElementException ignore) {
        }
    }


    @After
    public void close() {
        catalog.dbFile.close();
        catalog.dbFile.getFile().deleteOnExit();
    }
}
