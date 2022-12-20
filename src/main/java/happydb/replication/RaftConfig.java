package happydb.replication;

import happydb.common.Database;
import happydb.common.Debug;
import happydb.common.Pair;
import happydb.exception.DbException;
import happydb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * RaftConfig 是一个单例类，存储着全局 Raft 节点信息
 * @Author happysnaker
 * @Date 2022/12/10
 * @Email happysnaker@foxmail.com
 */
public class RaftConfig {
    /**
     * 自身节点 ID，(host:port) 形式
     */
    public static String selfNodeId;

    /**
     * 存储着其他节点的地址，不包括自身
     */
    public final static List<Pair<String, Integer>> nodes = new ArrayList<>();

    /**
     * 存储着其他节点最后一个匹配 Leader 日志的下标，不包括自身节点
     */
    public static final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

    /**
     * 对于每一台服务器，发送到该服务器的下一个日志条目的索引，不包括自身节点
     */
    public static final Map<String, Long> nextIndex = new ConcurrentHashMap<>();

    /**
     * 自身节点的地址
     */
    public static Pair<String, Integer> selfAddr;

    /**
     * 当前节点
     */
    public static RaftNode self;

    /**
     * 初始化调用，添加一个集群节点，不包括自身节点
     */
    public synchronized static void addNode(String host, int port) {
        nodes.add(Pair.create(host, port));
        LogEntry last = Database.getRaftLogManager().getLastLogEntry();
        String nodeId = String.format("%s:%d", host, port);

        matchIndex.put(nodeId, -1L);
        nextIndex.put(nodeId, last == null ? 0L : last.getIndex() + 1);
    }

    /**
     * 事务提交时需要开始复制同步消息，在 Raft 算法中，提交一条消息是串行的
     * @param tid 待提交的事务
     * @return 返回真如果同步成功，假如果未能达成共识
     * @throws IllegalStateException 如果自身不是 Leader，或者数据库未开启复制
     */
    public synchronized static boolean commit(TransactionId tid) throws IllegalStateException, DbException {
        if (Database.enableReplication && self instanceof Leader leader) {
            LogEntry log = Database.getRaftLogManager().createLogEntry(tid);
            return leader.syncLog(log);
        }
        throw new IllegalStateException();
    }


    /**
     * 刷新当前节点为新状态，此方法会销毁旧状态并初始化新状态
     *
     * @param curr
     * @param newStatus 新状态
     * @param newTerm   新任期
     * @param voteFor   选票
     */
    public synchronized static void flushStatus(RaftNode curr, NodeStatus newStatus, long newTerm, String voteFor) throws DbException {
        flushStatus(curr, newStatus, newTerm, voteFor, null);
    }


    /**
     * 刷新当前节点为新状态，此方法会销毁旧状态并初始化新状态，<strong>此方法允许调用者安全的对新节点应用一些改变</strong>
     *
     * @param curr
     * @param newStatus 新状态
     * @param newTerm   新任期
     * @param voteFor   选票
     * @param func      对新节点的功能函数，可为 null
     * @return 由 func 指定的返回值，可能为 null
     */
    public synchronized static Object flushStatus(RaftNode curr, NodeStatus newStatus, long newTerm, String voteFor, Function<RaftNode, Object> func) throws DbException {
        if (curr != self) {
            return null;
        }
        if (self != null) {
            self.destroy();
        }

        Debug.log("Status change, new status " + newStatus + ", new term " + newTerm);

        switch (newStatus) {
            case LEADER -> {
                self = new Leader(selfNodeId, newTerm, newStatus, voteFor);
            }
            case FOLLOWER -> {
                self = new Follower(selfNodeId, newTerm, newStatus, voteFor);
            }
            case CANDIDATE -> {
                self = new Candidate(selfNodeId, newTerm, newStatus, voteFor);
            }
        }

        self.init();
        return func == null ? null : func.apply(self);
    }
}
