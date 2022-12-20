package happydb.replication;

import happydb.common.Database;
import happydb.common.Debug;
import happydb.common.Pair;
import happydb.exception.DbException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @Author happysnaker
 * @Date 2022/12/11
 * @Email happysnaker@foxmail.com
 */
public class Leader extends RaftNode {
    public static final int HEART_BEAT_RATE = 3000;

    public static final long TIMEOUT_MILLS = 3000L;

    private ScheduledFuture<?> schedule;

    public Leader(String nodeId, long term, NodeStatus status, String voteFor) {
        super(nodeId, term, status, voteFor);
    }

    /**
     * 事务提交时同步日志，首先写入 RaftLogManager 中，然后开始同步日志。
     * <ul>
     * <li>异步向每个节点发送从 nextIndex（初始为 maxIndex + 1） 到 currentIndex 的日志。</li>
     * <li>同步等待节点答复：<ul>
     * <li>如果节点任期大于自身，则刷新为 Follower，并返回 false 告知事务回滚。</li>
     * <li>如果节点返回 false，则节点的 nextIndex 递减，继续同步日志。</li>
     * <li>如果节点返回 true，则记录下来，并更新节点的 matchIndex 和 nextIndex，若半数以上节点同意，则视为同步成功。</li>
     * <li>若最终失败，则应该重试，Raft 为了一致性会放弃可用性。</li>
     * </ul>
     * </li>
     * <li>枚举 N，假设存在 N &gt; commitIndex，使得大多数的 <code>matchIndex[i] ≥ N</code> 以及 <code>log[N].term == currentTerm</code>  成立，则令 <code>commitIndex = N</code>。</li>
     * <li>应用到状态机中，推进 lastApplied。</li>
     * <li>异步发起 requestCommit 请求告知其他从节点提交，参数为当前 commitIndex 以及对于 commitIndex 的日志任期。</li>
     * </ul>
     *
     * @param log 日志
     * @return 真则同步成功
     * @throws IOException
     */
    public boolean syncLog(LogEntry log) throws DbException {
        Debug.log("Start to sync log " + log);
        log.setTerm(this.getTerm());
        try {
            lm.appendLogEntry(log);
        } catch (IOException e) {
            e.printStackTrace();
            throw new DbException(e);
        }

        Set<String> success = new HashSet<>();
        List<Pair<String, CompletableFuture<RpcMessage>>> futures = new ArrayList<>();
        Map<CompletableFuture, Long> timeoutMap = new HashMap<>();

        for (Map.Entry<String, Long> it : RaftConfig.nextIndex.entrySet()) {
            try {
                CompletableFuture<RpcMessage> f = syncLog(
                        it.getValue(), toAddress(it.getKey()).key, toAddress(it.getKey()).val, timeoutMap);
                futures.add(Pair.create(it.getKey(), f));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        long startTime = System.currentTimeMillis();

        // 能尽量同步所有节点就同步所有节点，如果超时了那就放宽限制仅同步半数节点
//        while ((success.size() < RaftConfig.nodes.size() / 2
//                && System.currentTimeMillis() - startTime >= TIMEOUT_MILLS) || success.size() == RaftConfig.nodes.size()) {


        // 仅同步半数以上节点，RaftConfig.nodes 不包括自身
        // 这可能会导致某些节点一直未同步，可以换成上面注释掉的条件来尝试同步所有节点
        while (success.size() < RaftConfig.nodes.size() / 2) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }


            for (var it : new ArrayList<>(futures)) {
                CompletableFuture<RpcMessage> future = it.getVal();
                String id = it.key;

                if (!future.isDone()) {
                    try {
                        // 如果一个消息超过 5s 则认为超时，重新发送
                        if (System.currentTimeMillis() - timeoutMap.getOrDefault(future, System.currentTimeMillis()) >= TIMEOUT_MILLS) {
                            Debug.log("Try again....");
                            CompletableFuture<RpcMessage> f = syncLog(RaftConfig.nextIndex.get(id),
                                    toAddress(id).key, toAddress(id).val, timeoutMap);
                            futures.add(Pair.create(id, f));
                            futures.remove(it);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    continue;
                }

                futures.remove(it);
                timeoutMap.remove(future);

                AppendEntriesResponse response = null;
                try {
                    response = (AppendEntriesResponse) future.get().getBody();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }


                if (response.isSuccess()) {
                    Debug.log("Sync log to " + id + " done.");
                    success.add(id);
                    RaftConfig.matchIndex.put(id, log.getIndex());
                    RaftConfig.nextIndex.put(id, log.getIndex() + 1);
                    continue;
                }

                if (response.getTerm() > this.term) {
                    RaftConfig.flushStatus(this, NodeStatus.FOLLOWER, response.getTerm(), id);
                    return false;
                }

                try {
                    RaftConfig.nextIndex.put(id, RaftConfig.nextIndex.get(id) - 1);
                    Debug.log("Next index not match, try reduce it and try again, now the next index is "
                            + RaftConfig.nextIndex.get(id));
                    futures.add(Pair.create(
                            it.key, syncLog(RaftConfig.nextIndex.get(id), toAddress(id).key, toAddress(id).val, timeoutMap)));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        Debug.log("Sync success, total sync " + success.size() + " servers.");

        long N = lm.getMaxIndex();
        while (N > lm.getCommitIndex() && lm.getLogEntry((int) N).getTerm() == this.term) {
            int sum = 0;
            for (var it : RaftConfig.matchIndex.entrySet()) {
                if (it.getValue() >= N)
                    sum++;
            }

            if (sum >= RaftConfig.matchIndex.size() / 2) {
                break;
            }
            N--;
        }


        Debug.log("New commit index is " + N);
        lm.setCommitIndex(N);
        lm.commit();

        for (Pair<String, Integer> node : RaftConfig.nodes) {
            try {
                client.connectAndWrite(node.key, node.val,
                        new CommitRequest(term, getNodeId(), lm.getCommitIndex(), term), RpcMessage.REQUEST_COMMIT_MESSAGE);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return true;
    }


    private CompletableFuture<RpcMessage> syncLog(long nextIndex, String host, int port, Map<CompletableFuture, Long> map)
            throws DbException {
        List<LogEntry> entries = lm.getLogEntries((int) nextIndex, (int) lm.getMaxIndex());
        LogEntry prev = lm.getLogEntry((int) (nextIndex - 1));
        AppendEntriesRequest request = AppendEntriesRequest.builder()
                .entries(entries)
                .leaderCommitIndex(lm.getCommitIndex())
                .nodeId(getNodeId())
                .term(this.getTerm())
                .prevLogIndex(prev == null ? -1 : (int) prev.getIndex())
                .prevLogTerm(prev == null ? -1 : prev.getTerm())
                .build();
        try {
            CompletableFuture<RpcMessage> f = client.connectAndWrite(host, port, request, RpcMessage.APPEND_LOG_MESSAGE);
            map.put(f, System.currentTimeMillis());
            return f;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void init() throws DbException {
        // 启动心跳例程
        ScheduledFuture<?> schedule = Database.service.scheduleAtFixedRate(() -> {
            try {
                Debug.log("Leader send heart beat package...");
                List<CompletableFuture<RpcMessage>> futures = new ArrayList<>();
                for (Pair<String, Integer> node : RaftConfig.nodes) {
                    try {
                        CompletableFuture<RpcMessage> future = client.connectAndWrite(node.key, node.val,
                                AppendEntriesRequest.builder()
                                        .nodeId(getNodeId())
                                        .term(term)
                                        .build(), RpcMessage.APPEND_LOG_MESSAGE);
                        futures.add(future);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                Thread.sleep(100);

                for (CompletableFuture<RpcMessage> future : futures) {
                    if (!future.isDone()) {
                        continue;
                    }
                    AppendEntriesResponse response = null;
                    try {
                        response = (AppendEntriesResponse) future.get().getBody();
                        if (response.getTerm() > Leader.this.term) {
                            RaftConfig.flushStatus(Leader.this, NodeStatus.FOLLOWER, response.getTerm(), null);
                            return;
                        }
                    } catch (InterruptedException | ExecutionException | DbException e) {
                        e.printStackTrace();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 1, HEART_BEAT_RATE, TimeUnit.MILLISECONDS);

        // 同步一个空日志
        try {
            syncLog(new LogEntry(new ArrayList<>(), -1, -1, term, -1));
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
