package happydb.replication;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Author happysnaker
 * @Date 2022/12/10
 * @Email happysnaker@foxmail.com
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VoteResponse implements Serializable {
    /**
     * 当前任期
     */
    private long term;

    /**
     * 节点 ID
     */
    private String nodeId;

    /**
     * 标识是否赢得投票
     */
    private boolean success;
}
