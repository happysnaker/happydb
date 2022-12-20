package happydb.replication;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Raft 集群间消息传递的载体
 * @Author happysnaker
 * @Date 2022/12/10
 * @Email happysnaker@foxmail.com
 */
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class RpcMessage implements Serializable {
    /**
     * 标识此消息为日志追加 RPC 调用
     */
    public static final int APPEND_LOG_MESSAGE = 1;
    /**
     * 标识此消息为请求提交调用
     */
    public static final int REQUEST_COMMIT_MESSAGE = 2;
    /**
     * 标识此消息为请求投票调用
     */
    public static final int REQUEST_VOTE_MESSAGE = 3;

    /**
     * 消息类型
     */
    private int messageType;

    /**
     * 消息 ID，唯一标识一条消息
     */
    private long id;

    /**
     * 消息主体
     */
    private Object body;
}













