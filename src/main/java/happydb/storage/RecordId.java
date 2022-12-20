package happydb.storage;

import lombok.Data;
import lombok.Getter;
import lombok.NonNull;

/**
 * 行记录的 ID，以页号和插槽号唯一标识
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
@Data
public class RecordId {
    @NonNull
    @Getter
    private PageId pid;
    @NonNull
    private int recordNumber;
}
