package happydb.transport.server;


import happydb.SqlMessage;
import happydb.common.Database;
import happydb.common.Debug;
import happydb.exception.DbException;
import happydb.execution.OpIterator;
import happydb.execution.Project;
import happydb.execution.SQLExecutor;
import happydb.replication.NodeStatus;
import happydb.replication.RaftConfig;
import happydb.storage.Record;
import happydb.storage.TableDesc;
import happydb.transaction.TransactionId;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import net.sf.jsqlparser.schema.Table;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * 与客户端通信的核心服务类
 *
 * @author Happysnaker
 * @description
 * @date 2022/5/16
 * @email happysnaker@foxmail.com
 */
@ChannelHandler.Sharable
public class SimpleDbServerHandler extends SimpleChannelInboundHandler<SqlMessage> {

    @Override
    protected void channelRead0(ChannelHandlerContext context, SqlMessage sqlMessage) throws Exception {
        String sql = sqlMessage.getMessage().replace("`", "");
        long xid = sqlMessage.getXid();

        var response = SqlMessage.builder().xid(xid);

        // 如果为客户端执行事务的服务器不是自身，则拒绝执行
        if (Database.enableReplication &&
                (sqlMessage.getServerId() != null && !RaftConfig.self.getNodeId().equals(sqlMessage.getServerId()))) {
            response.error("Raft cluster elect event happened, please wait some time to check it.");
            context.writeAndFlush(response.build());
            return;
        }

        // 如果自身不是主节点，拒绝执行
        // 如果事务之前已经执行一部分了，则回滚
        if (Database.enableReplication && RaftConfig.self.getStatus() != NodeStatus.LEADER
                && !sql.toUpperCase(Locale.ROOT).trim().startsWith("SELECT")) {
            if (xid != -1) {
                SQLExecutor.rollback(xid);
            }
            // 自身不是主库，重定向
            // 暂时没实现这个逻辑
            context.writeAndFlush(response
                    .error("Server is not leader, the leader may be " + RaftConfig.self.getVoteFor()).build());
            return;
        }


        String token = sql.trim().toUpperCase(Locale.ROOT);
        if (token.startsWith("BEGIN")) {
            try {
                xid = SQLExecutor.begin();
                response.message("Current xid is " + xid);
                // 事务开启，设置为客户端执行事务的服务器 ID
                if (Database.enableReplication) {
                    response.serverId(RaftConfig.self.getNodeId());
                }
                context.writeAndFlush(response.xid(xid).build());
            } catch (Exception e) {
                context.writeAndFlush(response.error(e.getMessage()).build());
            }
            return;
        }


        long startTime = System.currentTimeMillis();

        long tempXid = -1;
        try {
            Debug.log("Receive client[" + context.channel().remoteAddress() + "] sql " + sql + ". xid is " + xid);
            // 如果没有事务，则开启一个临时事务，维护单条语句的 ACID
            tempXid = xid == -1 ? SQLExecutor.begin() : xid;
            if (token.startsWith("CREATE")) {
                SQLExecutor.createTable(tempXid, sql);
            } else if (token.startsWith("INSERT")) {
                int rows = SQLExecutor.insert(tempXid, sql);
                response.numRows(rows);
            } else if (token.startsWith("DELETE")) {
                int rows = SQLExecutor.delete(tempXid, sql);
                response.numRows(rows);
            } else if (token.startsWith("UPDATE")) {
                int rows = SQLExecutor.update(tempXid, sql);
                response.numRows(rows);
            } else if (token.startsWith("SELECT")) {
                formatQuery(SQLExecutor.query(tempXid, sql), response);
                if (xid == -1) {
                    xid = 0; // no commit again
                    Database.getTransactionManager().commit(new TransactionId(tempXid), false);
                }
            } else if (token.startsWith("COMMIT")) {
                if (xid == -1) {
                    throw new Exception("No transaction begin.");
                }
                SQLExecutor.commit(tempXid);
                response.xid(-1);
                response.serverId(null); // 事务结束，清空
            } else if (token.startsWith("ROLLBACK")) {
                if (xid == -1) {
                    throw new Exception("No transaction begin.");
                }
                SQLExecutor.rollback(tempXid);
                response.xid(-1);
                response.serverId(null); // 事务结束，清空
            } else {
                throw new Exception("No such command " + token);
            }
        } catch (Exception e) {
            response.executionTime(System.currentTimeMillis() - startTime);
            // 没有事务，回滚临时事务
            if (xid == -1) {
                try {
                    SQLExecutor.rollback(tempXid);
                    response.serverId(null); // 事务结束，清空
                } catch (Exception e1) {
                    response.error(getErrorInfoFromException(e1));
                }
            }
            e.printStackTrace();
            context.writeAndFlush(response.error(getErrorInfoFromException(e)).build());
            return;
        }
        // 没有事务，提交临时事务
        if (xid == -1) {
            try {
                SQLExecutor.commit(tempXid);
                response.serverId(null); // 事务结束，清空
            } catch (Exception e) {
                response.error(e.getMessage());
            }
        }
        response.executionTime(System.currentTimeMillis() - startTime);
        context.writeAndFlush(response.build());
    }


    public static void formatQuery(OpIterator iterator, SqlMessage.SqlMessageBuilder builder) throws DbException {
        iterator.open();

        StringBuilder sb = new StringBuilder();
        Record[] recordAr = iterator.getRecordAr();

        for (int i1 = 0; i1 < iterator.getTableDesc().numFields(); i1++) {
            String fieldName = iterator.getTableDesc().getFieldName(i1);
            sb.append(String.format("%-10s", fieldName));
        }
        sb.append("\n");
        for (int i = 0; i < recordAr.length; i++) {
            Record record = recordAr[i];
            for (int j = 0; j < record.getNumFields(); j++) {
                sb.append(String.format("%-10s", record.getField(j).getObject().toString()));
            }
            if (i != recordAr.length - 1) {
                sb.append('\n');
            }
        }
        builder.message(sb.toString().trim());
        builder.numRows(recordAr.length);
    }

    /**
     * 格式化错误流
     */
    public static String getErrorInfoFromException(Throwable e) {
        try {
            StringWriter sw = null;
            PrintWriter pw = null;
            try {
                sw = new StringWriter();
                pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                return sw.toString();
            } finally {
                assert sw != null;
                assert pw != null;
                sw.close();
                pw.close();
            }
        } catch (Exception e1) {
            e.printStackTrace();
            return e.getMessage();
        }
    }
}
