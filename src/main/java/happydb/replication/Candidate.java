package happydb.replication;

import happydb.common.Database;
import happydb.common.Debug;
import happydb.common.Pair;
import happydb.exception.DbException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @Author happysnaker
 * @Date 2022/12/11
 * @Email happysnaker@foxmail.com
 */
public class Candidate extends RaftNode {
    public static final int TIMEOUT_MILLS = 5000;

    private ScheduledFuture<?> schedule;

    public Candidate(String nodeId, long term, NodeStatus status, String voteFor) {
        super(nodeId, term, status, voteFor);
    }

    /**
     * <p>发起 RPC 投票请求，附带自身最新日志的编号和任期。</p>
     * <ul>
     * <li>异步发起投票。如果有半数以上节点响应，刷新为 Leader。</li>
     * <li>否则静默等待超时。</li>
     * </ul>
     */
    private void runFor() throws DbException, InterruptedException {
        Debug.log("Candidate " + nodeId + " start run for.");
        int vote = 0;
        List<CompletableFuture<RpcMessage>> futures = new ArrayList<>();
        var last = lm.getLastLogEntry();
        for (Pair<String, Integer> node : RaftConfig.nodes) {
            try {
                futures.add(RpcClientHandler.getRpcClientHandler().connectAndWrite(node.key, node.val,
                        VoteRequest.builder()
                                .nodeId(getNodeId())
                                .term(term)
                                .lastLogIndex(last == null ? -1 : (int) last.getIndex())
                                .lastLogTerm(last == null ? -1 : last.getTerm())
                                .build(), RpcMessage.REQUEST_VOTE_MESSAGE));
            } catch (Exception e) {
                Debug.log("node " + node + " can not write and flush.");
                e.printStackTrace();
            }
        }

        Thread.sleep(100);

        for (CompletableFuture<RpcMessage> future : futures) {
            if (future.isDone()) {
                VoteResponse response = null;
                try {
                    response = (VoteResponse) future.get().getBody();
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }

                if (response.isSuccess()) {
                    vote++;
                    continue;
                }

                if (response.getTerm() > this.term) {
                    RaftConfig.flushStatus(this, NodeStatus.FOLLOWER, response.getTerm(), null);
                }
            }
        }

        if (vote >= RaftConfig.nodes.size() / 2) {
            RaftConfig.flushStatus(this, NodeStatus.LEADER, getTerm(), getNodeId());
        }
    }


    @Override
    public void init() throws DbException {
        schedule = Database.service.schedule(() -> {
            try {
                Debug.log("Run for leader timeout. try again...");
                RaftConfig.flushStatus(this, NodeStatus.CANDIDATE, Candidate.this.term + 1, Candidate.this.nodeId);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, (long) (TIMEOUT_MILLS + Math.random() * TIMEOUT_MILLS), TimeUnit.MILLISECONDS);



        try {
            runFor();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void destroy() throws DbException {
        if (schedule != null)
            schedule.cancel(true);
    }
}
