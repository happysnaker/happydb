package happydb.replication;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 追加日志请求的响应
 * @Author happysnaker
 * @Date 2022/12/10
 * @Email happysnaker@foxmail.com
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AppendEntriesResponse implements Serializable {
    /**
     * 当前任期
     */
    private long term;

    /**
     * 节点 ID
     */
    private String nodeId;

    /**
     * 是否成功
     */
    private boolean success;
}
