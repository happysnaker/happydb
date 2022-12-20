package happydb.replication;

import happydb.common.Database;
import happydb.common.Debug;
import happydb.common.Pair;
import happydb.exception.DbException;
import lombok.Getter;

import java.io.IOException;

/**
 * RaftNode 代表 Raft 集群中的具体的节点
 *
 * @Author happysnaker
 * @Date 2022/12/10
 * @Email happysnaker@foxmail.com
 */
public abstract class RaftNode {
    /**
     * 节点 ID，以 "host:port" 形式唯一标识
     */
    @Getter
    protected final String nodeId;
    /**
     * 节点任期，每个节点在不刷新状态的情况下最多一个任期
     */
    @Getter
    protected final long term;
    /**
     * 节点当前状态
     */
    @Getter
    protected final NodeStatus status;
    /**
     * 节点的选票
     */
    @Getter
    protected String voteFor;

    protected final RaftLogManager lm;

    @Getter
    protected final RpcClientHandler client;

    public RaftNode(String nodeId, long term, NodeStatus status, String voteFor) {
        this.nodeId = nodeId;
        this.term = term;
        this.status = status;
        this.voteFor = voteFor;
        this.lm = Database.getRaftLogManager();
        this.client = RpcClientHandler.getRpcClientHandler();
    }


    /**
     * 接受主节点的同步日志请求
     * <ul>
     * <li>如果对方任期小于接收者当前任期，返回假，并告知领导人当前的任期，让领导人转为 Follower。</li>
     * <li>如果对方任期大于自身，或者自身不是 Follower(任期等于自己)，则转为 Follower 追随对方，重试方法。</li>
     * <li>如果自身 prevLogIndex 处的 Log 的任期与对方发送过来的不符合，返回假。</li>
     * <li>向 RaftLogManager 中顺序追加日志，如果 RaftLogManager 中某条日志与 Leader 发送的日志的任期不符合，则丢弃此条日志及之后的日志，以 Leader 发送过来的日志覆盖。返回真。</li>
     * <li>如果对方的 commitIndex 大于自身的 commitIndex，则更新自身 commitIndex 为 <code>Math.min(leaderCommitIndex, maxIndex)</code>，同时递增 lastApplied 并将日志应用到状态机中。</li>
     * <li>重置计时器。重置计时器可以使用一个简单的方法，即刷新状态为 Follower。</li>
     * </ul>
     *
     * @param request 请求
     * @return 答复
     */
    public synchronized AppendEntriesResponse appendLogEntries(AppendEntriesRequest request) throws DbException, IOException {
        if (request.getTerm() < this.term) {
            return new AppendEntriesResponse(term, getNodeId(), false);
        }
        if (request.getEntries() == null) {
            // 心跳
            RaftConfig.flushStatus(this, NodeStatus.FOLLOWER, request.getTerm(), request.getNodeId());
            return new AppendEntriesResponse(term, getNodeId(), true);
        }
        // 日志非空，需要进行日志同步
        if (status != NodeStatus.FOLLOWER || request.getTerm() > this.term) {
            return (AppendEntriesResponse) RaftConfig.flushStatus(this, NodeStatus.FOLLOWER, request.getTerm(),
                    request.getNodeId(), node -> {
                        try {
                            return node.appendLogEntries(request);
                        } catch (DbException | IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        if (request.getPrevLogIndex() != -1) {
            LogEntry prevLog = lm.getLogEntry(request.getPrevLogIndex());
            if (prevLog == null || prevLog.getTerm() != request.getPrevLogTerm()) {
                return new AppendEntriesResponse(term, getNodeId(), false);
            }
        }


        int nextIndex = request.getPrevLogIndex() + 1;
        for (LogEntry entry : request.entries) {
            LogEntry match = lm.getLogEntry(nextIndex);
            if (match == null || match.getTerm() != entry.getTerm()) {
                break;
            }
            nextIndex++;
        }
        // 强制覆盖
        Debug.log("Force sync log and next index is " + nextIndex);
        lm.forceAppendEntries(
                request.entries.subList(nextIndex - request.getPrevLogIndex() - 1, request.entries.size()),
                nextIndex, request.getLeaderCommitIndex());

        RaftConfig.flushStatus(this, NodeStatus.FOLLOWER, request.getTerm(), request.getNodeId());
        return new AppendEntriesResponse(term, getNodeId(), true);
    }


    /**
     * 领导人发起提交请求，要求从节点快速推进一些日志，此方法是异步调用，领导人通常不会关注此方法的返回值。
     * <ul>
     * <li>如果对方任期小于接收者当前任期，直接返回，什么也不做，</li>
     * <li>如果对方任期大于自身，则转为 Follower 追随对方，重试方法。</li>
     * <li>如果待提交的日志任期与对方的不一致，静默返回。</li>
     * <li>否则，如果对方 commitIndex 比自身大，则更新自身 commitIndex，并推进 lastApplied。</li>
     * </ul>
     *
     * @param request 请求
     * @return 任意返回值，领导人可能不会关注返回值
     */
    public synchronized Object requestCommit(CommitRequest request) throws DbException {
        if (request.getTerm() < this.term || request.getLeaderCommitIndex() == -1) {
            return false;
        }
        if (status != NodeStatus.FOLLOWER || request.getTerm() > this.term) {
            return RaftConfig.flushStatus(this, NodeStatus.FOLLOWER, request.getTerm(), request.getNodeId(), node -> {
                try {
                    return node.requestCommit(request);
                } catch (DbException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        LogEntry entry = lm.getLogEntry((int) request.getLeaderCommitIndex());
        if (entry == null || entry.getTerm() != request.getCommitIndexLogTerm()) {
            return false;
        }
        lm.setCommitIndex(request.getLeaderCommitIndex());
        lm.commit();
        return true;
    }


    /**
     * 请求投票 RPC
     * <ul>
     * <li>如果对方 term 小于自身，返回假。</li>
     * <li>如果对方 term 等于自身，但自身选票给了别人，返回假。</li>
     * <li>否则，比较对方最新日志的任期与自身最新日志的任期，如果小于自身，返回假。</li>
     * <li>否则，比较对方最新日志的下标与自身最新日志的下标，如果小于自身，返回假。</li>
     * <li>否则，更新自身任期，并将状态改为 Follower，返回真。</li>
     * </ul>
     *
     * @param request 请求
     * @return 答复
     */
    public synchronized VoteResponse requestVote(VoteRequest request) throws DbException {
        if (request.getTerm() < this.term) {
            return new VoteResponse(this.term, getNodeId(), false);
        }
        if (request.getTerm() == this.term && (this.voteFor != null && !this.voteFor.equals(request.getNodeId()))) {
            return new VoteResponse(this.term, getNodeId(), false);
        }
        LogEntry last = lm.getLastLogEntry();
        if (request.getLastLogIndex() != -1 && last != null && request.getLastLogTerm() < last.getTerm()) {
            return new VoteResponse(this.term, getNodeId(), false);
        }
        if (last != null && request.getLastLogIndex() < last.getIndex()) {
            return new VoteResponse(this.term, getNodeId(), false);
        }
        RaftConfig.flushStatus(this, NodeStatus.FOLLOWER, request.getTerm(), request.getNodeId());
        return new VoteResponse(this.term, getNodeId(), true);
    }


    /**
     * 任何节点都需要进行一些初始化工作
     *
     * @throws DbException 任何异常
     */
    public abstract void init() throws DbException;


    /**
     * 销毁一个节点
     *
     * @throws DbException 任何异常
     */
    public abstract void destroy() throws DbException;


    protected static Pair<String, Integer> toAddress(String nodeId) {
        String[] strings = nodeId.split(":");
        return Pair.create(strings[0].trim(), Integer.parseInt(strings[1]));
    }
}
