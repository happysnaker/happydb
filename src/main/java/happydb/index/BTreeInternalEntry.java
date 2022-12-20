package happydb.index;

import happydb.storage.Field;
import happydb.storage.PageId;
import happydb.storage.RecordId;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

/**
 * <P>B+ 树内部节点的条目</P>
 * <P>条目没有实现 equals 和 hascode 方法</P>
 * @Author happysnaker
 * @Date 2022/11/21
 * @Email happysnaker@foxmail.com
 */
@Data
public class BTreeInternalEntry {
    /**
     * 本条目的 key
     * */
    @NonNull
    private Field key;

    /**
     * 左子页面id
     * */
    @NonNull
    private PageId leftChild;

    /**
     * 右子页面id
     * */
    @NonNull
    private PageId rightChild;

    /**
     * 本条目的记录 id，如果没有存储在任何页面上则为 null
     * */
    private EntryId entryId;
}
