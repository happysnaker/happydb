package happydb.index;

import happydb.storage.PageId;
import lombok.Data;
import lombok.NonNull;

/**
 * 表示 B+ 树条目的 ID，与 {@link happydb.storage.RecordId} 是一致的，为了区分行记录和条目，我们创建了这个类
 * @Author happysnaker
 * @Date 2022/11/21
 * @Email happysnaker@foxmail.com
 */
@Data
public class EntryId {
    @NonNull
    private PageId pid;
    @NonNull
    private int entryNumber;
}
