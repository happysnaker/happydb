package happydb.replication;

import happydb.common.DbSerializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 领导人要求从节点提交的请求，此请求是异步的，领导人不会关心从节点的响应
 * @Author happysnaker
 * @Date 2022/12/10
 * @Email happysnaker@foxmail.com
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CommitRequest implements Serializable {
    /**
     * 领导人的任期
     */
    private long term;

    /**
     * 领导人的 ID
     */
    private String nodeId;


    /**
     * 要求提交的日志索引
     */
    private long leaderCommitIndex;

    /**
     * 下标 {@link #leaderCommitIndex} 处日志的任期，理论上说，这个任期总是等于 Leader 任期
     */
    private long commitIndexLogTerm;
}
