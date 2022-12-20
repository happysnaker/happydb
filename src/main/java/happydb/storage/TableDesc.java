package happydb.storage;

import happydb.common.ByteArray;
import happydb.common.ByteList;
import happydb.common.DbSerializable;
import happydb.index.IndexType;
import lombok.Getter;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * 表的模式，包含记录模式、索引类型等信息，此类不一定表示实际的表，也可能表示诸如连接投影后的逻辑表，这种情况下，此类的 {@link #tableName} 为空
 * <p><strong>此类不包含任何隐藏字段的描述，这些字段是约定俗成的</strong></p>
 *
 * @Author happysnaker
 * @Date 2022/11/16
 * @Email happysnaker@foxmail.com
 */
public class TableDesc implements DbSerializable {
    /**
     * 4 字节的长度加上一个字符串，字符串形式严格遵循 table_name (field_name field_type index_type) (field_name field_type index_type) ...
     */
    @Override
    public ByteArray serialized() {
        StringBuilder sb = new StringBuilder();
        sb.append(tableName).append(" ");
        for (TDItem item : this.items) {
            sb.append("(")
                    .append(item.fieldName)
                    .append(" ")
                    .append(item.fieldType)
                    .append(" ")
                    .append(item.indexType)
                    .append(") ");
        }
        String s = sb.toString().trim();
        return new ByteList()
                .writeInt(s.getBytes(StandardCharsets.UTF_8).length)
                .writeString(s);
    }


    /**
     * 一个帮助类，方便整理各个字段的信息
     *
     * @param fieldType 字段类型
     * @param fieldName 字段名称
     * @param indexType 索引类型
     */
    public record TDItem(Type fieldType, String fieldName, int indexType) implements Serializable {
        public String toString() {
            return String.format("%s (%s %d)", fieldType, fieldName, indexType);
        }
    }

    /**
     * 元组的字段数组
     */
    @Getter
    private final TDItem[] items;
    /**
     * 表名
     */
    @Getter
    private final String tableName;

    /**
     * @return 一个迭代器，迭代包含在此 TableDesc 中的所有字段 TDItems
     */
    public Iterator<TDItem> iterator() {
        return Arrays.stream(items).iterator();
    }


    /**
     * 创建一个新的 TableDesc，其 typeAr.length 字段具有指定类型的字段，以及关联的字段名称字段。
     *
     * @param typeAr  指定此 TableDesc 中字段的数量和类型的数组。它必须至少包含一个条目。
     * @param fieldAr 指定字段名称的数组。
     * @param indexAr 指定字段的索引类型数组
     */
    public TableDesc(String tableName, String[] fieldAr, Type[] typeAr, int[] indexAr) {
        assert typeAr.length == fieldAr.length;
        this.tableName = tableName;
        this.items = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            this.items[i] = new TDItem(typeAr[i], fieldAr[i], indexAr[i]);
        }
    }

    /**
     * 创建一个新的 TableDesc，其 typeAr.length 字段具有指定类型的字段，以及关联的字段名称字段。
     *
     * @param typeAr  指定此 TableDesc 中字段的数量和类型的数组。它必须至少包含一个条目。
     * @param fieldAr 指定字段名称的数组。
     * @param indexAr 指定字段的索引类型数组
     */
    public TableDesc(String[] fieldAr, Type[] typeAr, int[] indexAr) {
        this(null, fieldAr, typeAr, indexAr);
    }

    /**
     * 创建一个新的投影 TableDesc，此 td 为逻辑投影表，索引与表名为空
     *
     * @param typeAr  指定此 TableDesc 中字段的数量和类型的数组。它必须至少包含一个条目。
     * @param fieldAr 指定字段名称的数组。
     */
    public TableDesc(String[] fieldAr, Type[] typeAr) {
        this(null, fieldAr, typeAr, new int[typeAr.length]);
    }


    /**
     * @return 此 TableDesc 中的字段数，<strong>不包含隐藏字段</strong>
     */
    public int numFields() {
        return this.items.length;
    }

    /**
     * 获取此 TableDesc 的第 i 个字段的字段名称。
     *
     * @param i 要返回的字段名称的索引。它必须是一个有效的索引。
     * @return 第 i 个字段的名称
     * @throws NoSuchElementException 如果 i 不是有效的字段引用。
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= this.items.length) {
            throw new NoSuchElementException();
        }
        return this.items[i].fieldName;
    }

    /**
     * 获取此 TableDesc 的第 i 个字段的类型。
     *
     * @param i 要获取类型的字段的索引。它必须是一个有效的索引。
     * @return 第 i 个字段的类型
     * @throws NoSuchElementException 如果 i 不是一个有效的字段引用。
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= this.items.length) {
            throw new NoSuchElementException();
        }
        return this.items[i].fieldType;
    }


    /**
     * 获取此 TableDesc 的第 i 个字段的索引类型。
     *
     * @param i 要获取类型的字段的索引。它必须是一个有效的索引。
     * @return 第 i 个字段的索引类型
     * @throws NoSuchElementException 如果 i 不是一个有效的字段引用。
     */
    public int getIndexType(int i) throws NoSuchElementException {
        if (i < 0 || i >= this.items.length) {
            throw new NoSuchElementException();
        }
        return this.items[i].indexType;
    }

    /**
     * 返回此模式中主键的字段索引
     *
     * @return 主键的字段索引
     * @throws NoSuchElementException 如果没有主键
     */
    public int getPrimaryKeyFieldIndex() throws NoSuchElementException {
        for (int i = 0; i < items.length; i++) {
            if (this.items[i].fieldName != null && IndexType.intToIndexSet(items[i].indexType).contains(IndexType.PRIMARY_KEY)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * 查找具有给定名称的字段的索引。
     *
     * @param name 字段名称。
     * @return 第一个具有给定名称的字段的索引。
     * @throws NoSuchElementException 如果没有找到具有匹配名称的字段。
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < this.items.length; i++) {
            if (this.items[i].fieldName != null && this.items[i].fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * 此方法适用于计算页面包含的元组数量，请注意，因为<strong>返回的大小不包含隐藏字段的大小</strong>
     *
     * @return 与此 TableDesc 对应的元组的大小（以字节为单位）
     */
    public int getRecordSize() {
        int size = 0;
        for (TDItem tdItem : this.items) {
            size += tdItem.fieldType.getLen();
        }
        return size;
    }


    /**
     * 将两个 TableDesc 合并为一个，具有 td1.numFields + td2.numFields 字段，在连接的时候可能有用，合并后的表名将置空
     *
     * @param td1 具有新 TableDesc 的第一个字段的 TableDesc
     * @param td2 具有 TableDesc 的最后一个字段的 TableDesc
     * @return 新的 TableDesc
     */
    public static TableDesc merge(TableDesc td1, TableDesc td2) {
        int len = td1.items.length + td2.items.length;
        Type[] typeAr = new Type[len];
        String[] fieldAr = new String[len];
        int[] indexAr = new int[len];
        int index = 0;
        for (TDItem tdItem : td1.items) {
            typeAr[index] = tdItem.fieldType;
            fieldAr[index] = tdItem.fieldName;
            indexAr[index] = tdItem.indexType;
            index++;
        }
        for (TDItem tdItem : td2.items) {
            typeAr[index] = tdItem.fieldType;
            fieldAr[index] = tdItem.fieldName;
            indexAr[index] = tdItem.indexType;
            index++;
        }
        return new TableDesc(null, fieldAr, typeAr, indexAr);
    }


    public String toString() {
        if (this.items.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (TDItem tdItem : this.items) {
            sb.append(String.format("%s(%s-%d), ", tdItem.fieldType.toString(), tdItem.fieldName, tdItem.indexType));
        }
        return sb.substring(0, sb.length() - 2);
    }


}

