package happydb.replication;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * 追加日志请求
 * @Author happysnaker
 * @Date 2022/12/10
 * @Email happysnaker@foxmail.com
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AppendEntriesRequest implements Serializable {
    /**
     * 领导人的任期
     */
    private long term;

    /**
     * 领导人的 ID
     */
    private String nodeId;

    /**
     * 前一个日志的索引下标
     */
    private int prevLogIndex = -1;

    /**
     * 前一个日志产生时领导人的任期
     */
    private long prevLogTerm;

    /**
     * 领导人当前的提交索引
     */
    private long leaderCommitIndex;

    /**
     * 待同步的日志
     */
    List<LogEntry> entries;
}
