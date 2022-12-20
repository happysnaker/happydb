package happydb.common;

import happydb.exception.DbException;
import happydb.exception.DuplicateValueException;
import happydb.exception.ParseException;
import happydb.index.BTreeIndex;
import happydb.index.BTreePageManager;
import happydb.index.Index;
import happydb.index.IndexType;
import happydb.log.UndoLog;
import happydb.log.UndoLogId;
import happydb.log.UndoLogPageManager;
import happydb.storage.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 目录，保存表到模式、页面管理类的映射
 *
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
public class Catalog {
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class IndexKey {
        String tableName;
        int fieldIndex;
        IndexType type;
    }

    /**
     * 目录项
     */
    final Map<String, Pair<TableDesc, PageManager>> catalogMap = new ConcurrentHashMap<>();
    /**
     * 索引映射，一个字段上的索引是一个单例类
     */
    final Map<IndexKey, Index> indexMap = new ConcurrentHashMap<>();
    /**
     * 目录文件
     */
    final DbFile dbFile;


    public Catalog(DbFile file) throws FileNotFoundException {
        assert file.getFile().exists();
        this.dbFile = file;
    }


    /**
     * 获取所有的真实表名
     * @return
     */
    public List<String> getAllReallyTableName() {
        List<String> ans = new ArrayList<>();
        for (Map.Entry<String, Pair<TableDesc, PageManager>> it : catalogMap.entrySet()) {
            if (it.getValue().getVal() instanceof HeapPageManager) {
                ans.add(it.getKey());
            }
        }
        return ans;
    }

    /**
     * 将表模式与 pm 添加到映射中
     */
    public void addTable(TableDesc table, PageManager pm) {
        catalogMap.put(table.getTableName(), new Pair<>(table, pm));
    }


    /**
     * 获取 TableDesc
     *
     * @param tableName 表名
     * @return TableDesc
     * @throws java.util.NoSuchElementException 如果不存在
     */
    public TableDesc getTableDesc(String tableName) throws NoSuchElementException {
        if (!catalogMap.containsKey(tableName)) {
            throw new NoSuchElementException();
        }
        return catalogMap.get(tableName).getKey();
    }


    /**
     * 获取 PageManager
     *
     * @param tableName 表名
     * @return PageManager
     * @throws java.util.NoSuchElementException 如果不存在
     */
    public PageManager getPageManager(String tableName) throws NoSuchElementException {
        if (!catalogMap.containsKey(tableName)) {
            throw new NoSuchElementException();
        }
        return catalogMap.get(tableName).getVal();
    }


    /**
     * 获取单例 Index 类
     *
     * @param tableName  实际的表名
     * @param fieldIndex 字段下标
     * @param type       索引类型
     * @return Index
     * @throws NoSuchElementException
     */
    public Index getIndex(String tableName, int fieldIndex, IndexType type) throws NoSuchElementException {
        IndexKey key = new IndexKey(tableName, fieldIndex, type);
        if (!indexMap.containsKey(key)) {
            throw new NoSuchElementException();
        }
        return indexMap.get(key);
    }

    /**
     * 以特定模式创建一个表，这会导致表模式写入文件并加入内存
     *
     * @param table 表模式
     * @throws IOException             io 异常
     * @throws DuplicateValueException 表名重复
     */
    public synchronized void createTable(TableDesc table) throws IOException, DuplicateValueException, DbException {
        assert table.getTableName() != null;
        if (catalogMap.containsKey(table.getTableName())) {
            throw new DuplicateValueException("表名重复");
        }
        ByteArray data = table.serialized();
        dbFile.append(data, true);
        try {
            processCatalog(data.readString(data.readInt()));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public void loadCatalog() {
        ByteArray byteAr = null;
        try {
            byteAr = dbFile.read(0L, (int) dbFile.getLength());
            while (byteAr.hasNextInt()) {
                int len = byteAr.readInt();
                processCatalog(byteAr.readString(len));
            }



            for (Pair<TableDesc, PageManager> it : this.catalogMap.values()) {
                if (it.getVal() instanceof HeapPageManager pm) {
                    pm.loadFreePage();
                }
                if (it.getVal() instanceof UndoLogPageManager pm) {
                    pm.loadFreePage();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void processCatalog(String catalog) throws ParseException {
        // table_name (field_name field_type index_type) (field_name field_type index_type)
        int firstSpaceIndex = catalog.indexOf(' ');
        if (firstSpaceIndex == -1) {
            throw new ParseException("Catalog 格式不正确");
        }
        String tableName = catalog.substring(0, firstSpaceIndex);
        catalog = catalog.substring(firstSpaceIndex + 1);
        int left = -1, right = -1;
        List<String> fieldNameAr = new ArrayList<>();
        List<Type> fieldTypeAr = new ArrayList<>();
        List<Integer> indexTypeAr = new ArrayList<>();
        while ((left = catalog.indexOf('(')) != -1 && (right = catalog.indexOf(')')) != -1) {
            String[] items = catalog.substring(left + 1, right).split("\\s+");
            if (items.length != 3)
                throw new ParseException("Catalog 格式不正确");
            try {
                fieldNameAr.add(items[0]);
                fieldTypeAr.add(Type.valueOf(items[1]));
                indexTypeAr.add(Integer.valueOf(items[2]));
            } catch (Exception e) {
                throw new ParseException(e);
            }
            catalog = catalog.substring(right + 1);
        }
        File heapFile = Database.getDbFile(tableName + ".dat");
        File undoFile = Database.getDbFile(tableName + UndoLogId.UNDO_LOG_TABLE_NAME_SUFFIX + ".log");

        try {
            if (!heapFile.exists()) {
                boolean b = heapFile.createNewFile();
            }
            if (!undoFile.exists()) {
                boolean b = undoFile.createNewFile();
            }
            String[] fieldAr = fieldNameAr.toArray(new String[0]);
            Type[] typeAr = fieldTypeAr.toArray(new Type[0]);
            int[] indexAr = indexTypeAr.stream().mapToInt(a -> a).toArray();
            TableDesc tableDesc = new TableDesc(tableName,
                    fieldAr,
                    typeAr,
                    indexAr);
            HeapPageManager heapPageManager = new HeapPageManager(tableName, new DbFile(heapFile));
            UndoLogPageManager undoLogPageManager = new UndoLogPageManager(
                    tableName + UndoLogId.UNDO_LOG_TABLE_NAME_SUFFIX, new DbFile(undoFile));
            addTable(tableDesc, heapPageManager);
            addTable(new TableDesc(tableName + UndoLogId.UNDO_LOG_TABLE_NAME_SUFFIX,
                    fieldAr,
                    typeAr,
                    indexAr), undoLogPageManager);


            for (int i = 0; i < indexTypeAr.size(); i++) {
                for (IndexType indexType : IndexType.intToIndexSet(indexTypeAr.get(i))) {

                    if (indexType == IndexType.PRIMARY_KEY || indexType == IndexType.BTREE_UNIQUE
                            || indexType == IndexType.HASH_UNIQUE) {
                        continue;
                    }

                    String indexTableName = String.format("%s-%d-%s", tableName, i, indexType.toString());
                    File indexFile = Database.getDbFile(indexTableName + ".index");
                    if (!indexFile.exists()) {
                        boolean b = indexFile.createNewFile();
                    }
                    addTable(
                            new TableDesc(indexTableName, fieldAr, typeAr, indexAr),
                            new BTreePageManager(indexTableName, new DbFile(indexFile)));

                    if (indexType == IndexType.BTREE) {
                        Index index = new BTreeIndex(indexTableName);
                        this.indexMap.put(new IndexKey(getTableNameFromIndexTableName(indexTableName), i, indexType), index);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void close() {
        this.dbFile.close();
        for (Pair<TableDesc, PageManager> it : catalogMap.values()) {
            it.getVal().close();
        }
    }


    /**
     * 根据索引的 tableName 获取索引字段的类型
     *
     * @param tableName 索引 tableName，形式为 tableName-fieldIndex-indexName
     * @return 索引字段类型
     */
    public static Type getFieldTypeFromIndexTableName(String tableName) {
        String[] strings = tableName.split("-");
        assert strings.length >= 3;
        return Database.getCatalog()
                .getTableDesc(tableName)
                .getFieldType(Integer.parseInt(strings[1]));
    }

    /**
     * 根据索引的 tableName 获取真实的表名
     *
     * @param tableName 索引 tableName，形式为 tableName-fieldIndex-indexName
     * @return 真实的表名
     */
    public static String getTableNameFromIndexTableName(String tableName) {
        String[] strings = tableName.split("-");
        assert strings.length >= 3;
        return strings[0];
    }
}
