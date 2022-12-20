package happydb;

import lombok.*;

import java.io.Serializable;

/**
 * @author Happysnaker
 * @description
 * @date 2022/5/16
 * @email happysnaker@foxmail.com
 */
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SqlMessage implements Serializable {
    /**
     * 客户端执行时的事务 ID，当 ID 为 0 时表示未开启事务
     */
    private long xid = -1;

    /**
     * 客户端执行的 sql 语句，服务器响应的数据（前提是没有错误）
     */
    private String message = "ok";

    /**
     * 服务器可能响应的错误信息，如果为 null 则代表没有错误发生
     */
    private String error;

    /**
     * 响应的记录数目
     */
    private int numRows;

    /**
     * 执行用时
     */
    private long executionTime;

    /**
     * 服务器 ID
     */
    private String serverId;
}
