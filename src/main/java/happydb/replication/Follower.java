package happydb.replication;

import happydb.common.Database;
import happydb.common.Debug;
import happydb.exception.DbException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @Author happysnaker
 * @Date 2022/12/11
 * @Email happysnaker@foxmail.com
 */
public class Follower extends RaftNode {
    public static final int TIMEOUT_MILLS = 8000;

    private ScheduledFuture<?> schedule;


    public Follower(String nodeId, long term, NodeStatus status, String voteFor) {
        super(nodeId, term, status, voteFor);
    }

    @Override
    public void init() throws DbException {
        schedule = Database.service.schedule(() -> {
            try {
                RaftConfig.flushStatus(Follower.this, NodeStatus.CANDIDATE, Follower.this.term + 1, Follower.this.nodeId);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, (long) (TIMEOUT_MILLS + Math.random() * TIMEOUT_MILLS), TimeUnit.MILLISECONDS);
    }

    @Override
    public void destroy() throws DbException {
        if (schedule != null)
            schedule.cancel(true);
    }
}
