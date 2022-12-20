package happydb.transaction;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * 事务 ID，以一个长整数唯一标识，用户事务从零开始，小于零的事务 ID 可用作其他用途
 * @Author happysnaker
 * @Date 2022/11/16
 * @Email happysnaker@foxmail.com
 */
@Data
public class TransactionId {
    @NonNull
    private long xid;
}
