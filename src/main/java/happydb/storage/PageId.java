package happydb.storage;

import lombok.Data;
import lombok.NonNull;

/**
 * 页面 ID，一个页由表名和页号唯一标识
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
@Data
public class PageId {
    @NonNull
    private String tableName;

    @NonNull
    private int pageNumber;
}
