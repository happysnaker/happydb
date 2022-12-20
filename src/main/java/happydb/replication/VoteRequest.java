package happydb.replication;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 投票 RPC 的请求参数
 * @Author happysnaker
 * @Date 2022/12/10
 * @Email happysnaker@foxmail.com
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VoteRequest  implements Serializable {
    /**
     * 候选人的任期
     */
    private long term;

    /**
     * 候选人的 ID
     */
    private String nodeId;

    /**
     * 候选人最新日志的下标
     */
    private int lastLogIndex;

    /**
     * 候选人最新日志的任期
     */
    private long lastLogTerm;
}
