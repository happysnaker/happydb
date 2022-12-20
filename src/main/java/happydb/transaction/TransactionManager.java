package happydb.transaction;

import happydb.common.ByteArray;
import happydb.common.Database;
import happydb.common.DbFile;
import happydb.exception.DbException;
import happydb.replication.RaftConfig;
import lombok.Getter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author happysnaker
 * @Date 2022/11/30
 * @Email happysnaker@foxmail.com
 */
public class TransactionManager {
    public static byte ABORT = 0;
    public static byte ACTIVE = 1;
    public static byte COMMITED = 2;

    public static final int HEAD_SIZE = 16;

    AtomicLong xid;

    @Getter
    DbFile dbFile;

    Set<TransactionId> activeSet = ConcurrentHashMap.newKeySet();

    public TransactionManager(DbFile dbFile) {
        this.dbFile = dbFile;

        try {
            if (dbFile.getLength() == 0) {
                dbFile.write(0, ByteArray.allocate(HEAD_SIZE).writeLong(0).writeLong(0));
            }
            xid = new AtomicLong(dbFile.read(0, 8).readLong());
            dbFile.setLength(HEAD_SIZE + xid.get() + 1);

            long activeSize = dbFile.read(8, 8).readLong();
            if (activeSize != 0) {
                for (long i = xid.get(); i > 0; i--) {
                    if (isActive(new TransactionId(i))) {
                        activeSet.add(new TransactionId(i));
                    }
                    if (activeSize == activeSet.size()) {
                        break;
                    }
                }
                if (activeSize != activeSet.size()) {
                    dbFile.write(8, ByteArray.allocate(8).writeLong(activeSet.size()));
                }
            }

        } catch (IOException | DbException e) {
            throw new RuntimeException(e);
        }
    }


    public long getLowLimitId() {
        return xid.get();
    }

    /**
     * 开启一个事务
     *
     * @return 返回新事务 xid
     */
    public synchronized TransactionId begin() throws IOException {
        long x = xid.getAndIncrement();
        ByteArray byteArray = ByteArray.allocate(16)
                .writeLong(x)
                .writeLong(activeSet.size() + 1);

        dbFile.write(0, byteArray);
        dbFile.write(HEAD_SIZE + x, new ByteArray((byte) 1));

        TransactionId tid = new TransactionId(x);
        activeSet.add(tid);
        return tid;
    }

    /**
     * 提交一个事务
     *
     * @param xid     提交事务的 xid
     * @param trySync 指示此次提交是否要尝试进行主从同步
     */
    public void commit(TransactionId xid, boolean trySync) throws IOException, DbException {
        if (!isActive(xid)) {
            throw new DbException("Xid is not active.");
        }

        // 日志刷盘
        Database.getLogBuffer().transactionCommit(xid);

        // Raft 日志同步，如果开启了 raft 日志同步则不需要往下面逻辑走，raft 会有逻辑进行日志提交，避免重复提交
        if (trySync) {
            // 失败则回滚此事务
            if (!RaftConfig.commit(xid)) {
                rollback(xid);
                throw new DbException("Raft replication failed, please wait some time to ensure the transaction is committed or abort");
            }
            return;
        }

        // 释放所有行锁
        Database.getLockTable().releaseAll(xid);

        // 释放页面锁
        Database.getBufferPool().transactionReleaseLock(xid);

        // 写入事务文件
        dbFile.write(HEAD_SIZE + xid.getXid(), new ByteArray((byte) COMMITED));

        // 移除内存活跃事务
        activeSet.remove(xid);
        dbFile.write(8, ByteArray.allocate(8).writeLong(activeSet.size()));

        // 释放版本快照
        ReadView.release(xid);
    }

    /**
     * 回滚一个事务
     *
     * @param xid 回滚事务的 xid
     */
    public void rollback(TransactionId xid) throws IOException, DbException {
        if (!isActive(xid)) {
            throw new DbException("Xid is not active.");
        }

        // 日志刷盘
        Database.getLogBuffer().transactionAbort(xid);

        // 释放所有行锁
        Database.getLockTable().releaseAll(xid);

        // 释放页面锁
        Database.getBufferPool().transactionReleaseLock(xid);

        // 写入事务文件
        dbFile.write(HEAD_SIZE + xid.getXid(), new ByteArray((byte) ABORT));

        // 移除内存活跃事务
        activeSet.remove(xid);
        dbFile.write(8, ByteArray.allocate(8).writeLong(activeSet.size()));

        // 释放版本快照
        ReadView.release(xid);
    }


    /**
     * 回滚一个事务，由恢复例程调用
     *
     * @param xid 回滚事务的 xid
     */
    public void rollbackByRecovery(TransactionId xid) throws IOException {
        dbFile.write(HEAD_SIZE + xid.getXid(), new ByteArray((byte) ABORT));

        activeSet.remove(xid);
    }

    /**
     * 判断一个事务是否处于活跃状态
     *
     * @param xid 事务 xid
     * @return
     */
    public boolean isActive(TransactionId xid) throws DbException {
        try {
            return dbFile.read(HEAD_SIZE + xid.getXid(), 1).readByte() == ACTIVE;
        } catch (IOException e) {
            throw new DbException(e);
        }
    }

    /**
     * 判断一个事务是否已提交
     *
     * @param xid 事务 xid
     * @return
     */
    public boolean isCommitted(TransactionId xid) throws DbException {
        try {
            return dbFile.read(HEAD_SIZE + xid.getXid(), 1).readByte() == COMMITED;
        } catch (IOException e) {
            throw new DbException(e);
        }
    }

    /**
     * 判断一个事务是否已放弃
     *
     * @param xid 事务 xid
     * @return
     */
    public boolean isAborted(TransactionId xid) throws DbException {
        try {
            return dbFile.read(HEAD_SIZE + xid.getXid(), 1).readByte() == ABORT;
        } catch (IOException e) {
            throw new DbException(e);
        }
    }


    /**
     * 获取当前系统中活跃的事务集合
     *
     * @return
     */
    public Set<TransactionId> getActiveTransactions() {
        return new HashSet<>(activeSet);
    }
}
