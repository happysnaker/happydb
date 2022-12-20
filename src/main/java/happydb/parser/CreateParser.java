package happydb.parser;

import happydb.common.Database;
import happydb.exception.DbException;
import happydb.exception.DuplicateValueException;
import happydb.exception.ParseException;
import happydb.index.IndexType;
import happydb.storage.TableDesc;
import happydb.storage.Type;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.Index;
import net.sf.jsqlparser.statement.insert.Insert;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.function.Function;

/**
 * @Author happysnaker
 * @Date 2022/12/4
 * @Email happysnaker@foxmail.com
 */
public class CreateParser {

    private static void assertIndexType(Set<IndexType> already, IndexType curr) throws ParseException {
        switch (curr) {
            case BTREE, HASH -> {
                if (already.contains(IndexType.HASH_UNIQUE) || already.contains(IndexType.BTREE_UNIQUE)) {
                    throw new ParseException("Can not create normal index because it already has unique here.");
                }
            }
            case BTREE_UNIQUE, HASH_UNIQUE -> {
                if ((already.contains(IndexType.HASH) && !already.contains(IndexType.BTREE_UNIQUE))
                        || (already.contains(IndexType.BTREE) && !already.contains(IndexType.HASH_UNIQUE))) {
                    throw new ParseException("Can not create unique index because it already has normal here.");
                }
            }
        }
    }

    /**
     * 解析创建表的 SQL
     */
    public synchronized static void parserCreate(CreateTable createTable) throws ParseException, DbException {
        String tableName = createTable.getTable().getName().replace("`", "");
        try {
            Database.getCatalog().getTableDesc(tableName);
            throw new ParseException("Duplicated table name here.");
        } catch (NoSuchElementException ignore) {
            // avoid table duplicate
        }

        List<ColumnDefinition> columnDefinitions = createTable.getColumnDefinitions();
        if (columnDefinitions == null) {
            throw new ParseException("Empty create statement");
        }
        int n = columnDefinitions.size();
        String[] fieldNameAr = new String[n];
        Type[] fieldTypeAr = new Type[n];
        Set<IndexType>[] setAr = new Set[n];
        for (int i = 0; i < setAr.length; i++) {
            setAr[i] = new HashSet<>();
        }

        // 处理 fieldName 和 fieldType
        for (int i = 0; i < n; i++) {
            ColumnDefinition cd = columnDefinitions.get(i);
            String columnName = cd.getColumnName().replace("`", "");
            if (!Arrays.stream(fieldNameAr).filter(s -> s != null && s.equals(columnName)).toList().isEmpty()) {
                throw new ParseException("Duplicated field name " + columnName + " here.");
            }
            fieldNameAr[i] = columnName;
            if (columnName.contains("-")) {
                throw new ParseException("Char '-' not support here.");
            }
            switch (cd.getColDataType().getDataType().toUpperCase(Locale.ROOT)) {
                case "INT" -> fieldTypeAr[i] = Type.INT_TYPE;
                case "DOUBLE" -> fieldTypeAr[i] = Type.DOUBLE_TYPE;
                case "CHAR" -> fieldTypeAr[i] = Type.STRING_TYPE;
                default -> {
                    throw new ParseException("Not support type named " + cd.getColDataType());
                }
            }
        }

        Function<String, Integer> findIndex = s -> {
            for (int i = 0; i < fieldNameAr.length; i++) {
                if (fieldNameAr[i].equals(s)) {
                    return i;
                }
            }
            return -1;
        };

        boolean pk = false;

        for (Index index : createTable.getIndexes()) {
            List<Index.ColumnParams> columns = index.getColumns();
            if (columns.size() != 1) {
                throw new ParseException("Only support create index on one field.");
            }
            String columnName = columns.get(0).getColumnName().replace("`", "");
            int i = findIndex.apply(columnName);
            if (i == -1) {
                throw new ParseException("Can not find field named " + columns.get(0) + " when create index.");
            }
            boolean usingBtree = true;
            List<String> spec = index.getIndexSpec();
            if (spec != null && !spec.isEmpty()) {
                if (!spec.get(0).equalsIgnoreCase("USING")) {
                    throw new ParseException("Unknown token " + spec.get(0) + " here.");
                }
                if (spec.size() != 2) {
                    throw new ParseException("Using token can only be hash or btree.");
                }
                if (spec.get(1).equalsIgnoreCase("HASH")) {
                    usingBtree = false;
                } else if (!spec.get(1).equalsIgnoreCase("BTREE")) {
                    throw new ParseException("Using token can only be hash or btree.");
                }
            }
            switch (index.getType().toUpperCase(Locale.ROOT).trim()) {
                case "PRIMARY KEY" -> {
                    if (pk)
                        throw new ParseException("Duplicated primary key define.");
                    if (!usingBtree)
                        throw new ParseException("Primary key must using btree.");
                    pk = true;

                    assertIndexType(setAr[i], IndexType.BTREE_UNIQUE);
                    setAr[i].add(IndexType.PRIMARY_KEY);
                    setAr[i].add(IndexType.BTREE);
                    setAr[i].add(IndexType.BTREE_UNIQUE);
                }
                case "KEY", "INDEX" -> {
                    assertIndexType(setAr[i], usingBtree ? IndexType.BTREE : IndexType.HASH);
                    setAr[i].add(usingBtree ? IndexType.BTREE : IndexType.HASH);
                }
                case "UNIQUE KEY", "UNIQUE INDEX" -> {
                    assertIndexType(setAr[i], usingBtree ? IndexType.BTREE_UNIQUE : IndexType.HASH_UNIQUE);
                    setAr[i].add(usingBtree ? IndexType.BTREE : IndexType.HASH);
                    setAr[i].add(usingBtree ? IndexType.BTREE_UNIQUE : IndexType.HASH_UNIQUE);
                }
            }
        }

        if (!pk) {
            throw new ParseException("Not primary key find.");
        }
        int[] indexTypeAr = new int[n];
        for (int i = 0; i < n; i++) {
            indexTypeAr[i] = IndexType.indexSetToInt(setAr[i]);
        }


        TableDesc td = new TableDesc(tableName, fieldNameAr, fieldTypeAr, indexTypeAr);
        try {
            Database.getCatalog().createTable(td);
        } catch (IOException | DuplicateValueException e) {
            throw new RuntimeException(e);
        } catch (DbException e) {
            throw new DbException(e);
        }
    }
}
