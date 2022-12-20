package happydb;

import happydb.common.Database;
import happydb.replication.LogEntry;
import happydb.replication.NodeStatus;
import happydb.replication.RaftBootStrap;
import happydb.replication.RaftConfig;
import happydb.transport.server.ServerBootStrap;

/**
 * @Author happysnaker
 * @Date 2022/12/11
 * @Email happysnaker@foxmail.com
 */
public class RaftServer3 {
    public static void main(String[] args) throws Exception {
        Database.enableReplication = true;
        Database.REPOSITORY_DIR = "repo3";
        Database.run();

        // 启动 raft server
        RaftBootStrap.start(4098);

        // 添加 Raft 集群
        RaftConfig.selfNodeId = "127.0.0.1:4098";
        RaftConfig.addNode("127.0.0.1", 4096);
        RaftConfig.addNode("127.0.0.1", 4097);


        // 刷新为 Follower
        RaftConfig.flushStatus(null, NodeStatus.FOLLOWER, 0, null);

        // 启动 db server
        ServerBootStrap.start("127.0.0.1", 2050);
    }
}
