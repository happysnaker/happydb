package happydb.index;

import happydb.common.ByteArray;
import happydb.common.ByteList;
import happydb.common.DbSerializable;
import happydb.storage.Field;
import happydb.storage.PageId;
import happydb.storage.RecordId;
import happydb.storage.Type;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.text.ParseException;

/**
 * B+ 树叶子节点存储的条目，由 key 和指向记录的 RecordId 组成
 * <P>条目不会存储 tableName，只会存储 pageNumber 和 RecordNumber</P>
 * <P>条目没有实现 equals 和 hascode 方法</P>
 * @Author happysnaker
 * @Date 2022/11/21
 * @Email happysnaker@foxmail.com
 */
@Data
public class BTreeLeafEntry implements DbSerializable {
    @NonNull
    private Field key;
    @NonNull
    private RecordId recordId;
    @Getter
    @Setter
    private EntryId entryId;


    @Override
    public ByteArray serialized() {
        return new ByteList()
                .writeByteArray(key.serialized())
                .writeInt(recordId.getPid().getPageNumber())
                .writeInt(recordId.getRecordNumber());
    }


    /**
     * 从字节数组中解析 Entry
     *
     * @param byteAr    字节数组
     * @param tableName 索引对应的表名，可通过 {@link happydb.common.Catalog#getTableNameFromIndexTableName(String)} 获取
     * @return 返回条目
     */
    public static BTreeLeafEntry parse(ByteArray byteAr, Type type, String tableName) throws ParseException {
        Field key = type.parse(byteAr);
        int pageNumber = byteAr.readInt();
        int recordNumber = byteAr.readInt();
        return new BTreeLeafEntry(key, new RecordId(new PageId(tableName, pageNumber), recordNumber));
    }
}
