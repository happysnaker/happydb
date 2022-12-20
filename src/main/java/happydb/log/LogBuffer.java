package happydb.log;

import happydb.common.*;
import happydb.exception.DbException;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.TransactionId;
import lombok.Getter;
import lombok.NonNull;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 日志集中缓存，并持久化
 * <P><B>创建的任何 Log 存储的数据都应该是不可变的对象，这应该由对应的 Log 保证，此类可不做任何保证</B></P>
 *
 * @Author happysnaker
 * @Date 2022/11/30
 * @Email happysnaker@foxmail.com
 */
public class LogBuffer {

    public static final int HEAD_SIZE = 24;

    /**
     * 当检查点推进多少字节时截断日志
     */
    public static int TRUNCATE_HOLDER = 0;

    private final DbFile dbFile;

    public LogBuffer(DbFile dbFile) {
        this.dbFile = dbFile;
        ByteArray data;
        try {
            if (dbFile.getLength() == 0) {
                dbFile.write(0, ByteArray.allocate(HEAD_SIZE));
            }
            data = dbFile.read(0, HEAD_SIZE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        maxLsn = new AtomicLong(data.readLong());
        checkPoint = data.readLong();
        truncate = data.readLong();
    }

    /**
     * 当前日志可供分配的最新的 LSN，LSN 分配后自增一个日志的大小
     */
    private final AtomicLong maxLsn;

    /**
     * 当前日志文件的检查点，由检查点进程推进
     */
    @Getter
    private volatile long checkPoint;

    /**
     * 当前日志已截断的总数
     */
    @Getter
    private volatile long truncate;


    final Map<TransactionId, List<RedoLog>> redoLogMap = new ConcurrentHashMap<>();
    final Map<TransactionId, List<UndoLog>> undoLogMap = new ConcurrentHashMap<>();
    final Set<DataPage> flushSet = new HashSet<>();

    /**
     * 获取写入或读取偏移
     */
    private long getOffset(long lsn) {
        return lsn + HEAD_SIZE - truncate;
    }

    /**
     * 写入日志文件持久化
     */
    private synchronized void writeRedoLog(RedoLog log) throws IOException {
        if (log.getLsn() < checkPoint) {
            // 当这些日志还在内存中，检查点发生了，因此这些日志可以丢弃，否则文件被截断，偏移可能计算出错
            return;
        }
        dbFile.write(getOffset(log.getLsn()), log.serialized());
    }

    /**
     * 一些通用的方法，设置 LSN，写入占位符，写入 MAX_LSN，写入 redoLogMap，
     * 更新对应页的 LSN
     */
    private RedoLog processRedoLog(RedoLog redoLog) throws DbException {
        TransactionId tid = redoLog.xid();
        int size = redoLog.size();
        redoLog.setLsn(maxLsn.getAndAdd(size));
        long newMaxLsn = redoLog.getLsn() + size;
        try {
            synchronized (this) {
                if (maxLsn.get() <= newMaxLsn) {
                    dbFile.write(0, new ByteArray(newMaxLsn));
                }
                if (dbFile.getLength() < getOffset(redoLog.getLsn()) + size) {
                    dbFile.write(getOffset(redoLog.getLsn()), ByteArray.allocate(size));
                }

                // 占位
                dbFile.write(getOffset(redoLog.getLsn()), ByteArray.allocate(5)
                        .writeInt(size - 4)  // 不包括自身 4 字节
                        .writeByte(RedoLog.SPACE_REDO));
            }
        } catch (IOException e) {
            throw new DbException(e);
        }
        redoLogMap.putIfAbsent(tid, new ArrayList<>());
        redoLogMap.get(tid).add(redoLog);
        if (!(redoLog instanceof AbortRedoLog)) {
            DataPage data = (DataPage) Database.getBufferPool().getPage(
                    redoLog.xid(), redoLog.getPageId(), Permissions.READ_ONLY);
            data.setLsn(redoLog.getLsn());
            data.markDirty(true);
            synchronized (flushSet) {
                flushSet.add(data);
            }
        } else {
            List<UndoLog> undoLogs = undoLogMap.getOrDefault(tid, new ArrayList<>());
            Collections.reverse(undoLogs);
            for (UndoLog undoLog : undoLogs) {
                DataPage data = (DataPage) Database.getBufferPool().getPage(
                        redoLog.xid(), undoLog.getRecordId().getPid(), Permissions.READ_ONLY);
                data.setLsn(redoLog.getLsn());
                data.markDirty(true);
                synchronized (flushSet) {
                    flushSet.add(data);
                }
            }
        }
        return redoLog;
    }

    /**
     * 获取按照 {@link DataPage#getFirstDirtyLsn()} 从小到大排序后的脏页列表
     */
    public Iterator<Pair<Long, DataPage>> getFlushList() {
        synchronized (flushSet) {
            for (DataPage page : new HashSet<>(flushSet)) {
                // 惰性删除
                if (!page.isDirty()) {
                    flushSet.remove(page);
                }
            }

            return flushSet.stream()
                    .map(p -> Pair.create(p.getFirstDirtyLsn(), p))
                    .sorted((p1, p2) -> (int) (p1.getKey() - p2.getKey()))
                    .iterator();
        }
    }

    /**
     * 创建一条关于插入语句的重做日志
     *
     * @param tid    事务
     * @param insert {@link Record} 或者 {@link UndoLog}，insert 必须要具备 ID
     * @return 插入重做日志
     */
    public InsertRedoLog createInsertRedoLog(TransactionId tid, @NonNull DbSerializable insert) throws DbException {
        InsertRedoLog redoLog;
        if (insert instanceof Record record) {
            redoLog = new InsertRedoLog(
                    record.clone(), record.getRecordId().getPid(), record.getRecordId().getRecordNumber(), tid);
        } else if (insert instanceof UndoLog log) {
            redoLog = new InsertRedoLog(log, log.getId().pid(), log.getId().getUndoLogNumber(), tid);
        } else {
            throw new DbException("Insert RedoLog 仅支持创建 Record 或 UndoLog");
        }
        return (InsertRedoLog) processRedoLog(redoLog);
    }


    /**
     * 创建一条关于更新语句的重做日志
     *
     * @param tid    事务
     * @param update 更新后的记录
     * @return 更新语句的重做日志
     * @throws DbException
     */
    public UpdateRedoLog createUpdateRedoLog(TransactionId tid, @NonNull Record update) throws DbException {
        if (update.getRecordId() == null)
            throw new DbException("RecordId can not be null");
        UpdateRedoLog redoLog = new UpdateRedoLog(update.clone(), update.getRecordId(), tid);
        return (UpdateRedoLog) processRedoLog(redoLog);
    }

    /**
     * 创建一条关于删除语句的重做日志
     *
     * @param tid    事务
     * @param delete 待删除的记录 ID
     * @return 删除语句的重做日志
     * @throws DbException
     */
    public DeleteRedoLog createDeleteRedoLog(TransactionId tid, @NonNull RecordId delete) throws DbException {
        return (DeleteRedoLog) processRedoLog(new DeleteRedoLog(delete, tid));
    }

    /**
     * 创建一条关于事务取消语句的重做日志
     *
     * @param tid 事务
     * @return 事务取消语句的重做日志
     * @throws DbException
     */
    public AbortRedoLog createAbortRedoLog(@NonNull TransactionId tid) throws DbException {
        if (tid.getXid() < 0)
            throw new DbException("User xid can not less than zero.");
        return (AbortRedoLog) processRedoLog(new AbortRedoLog(tid));
    }

    /**
     * 创建 undo 的通用办法
     */
    private UndoLog createUndoLog(TransactionId tid, Record record) throws DbException {
        undoLogMap.putIfAbsent(tid, new ArrayList<>());
        List<UndoLog> undoLogs = undoLogMap.get(tid);
        UndoLog undoLog = new UndoLog(undoLogs.size(), record, record.getRecordId(), tid);

        String undoPageTableName = record.getRecordId().getPid().getTableName() + UndoLogId.UNDO_LOG_TABLE_NAME_SUFFIX;

        UndoLogPageManager pm = (UndoLogPageManager) Database.getCatalog().getPageManager(undoPageTableName);
        UndoLogId malloc = null;
        try {
            malloc = pm.malloc();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        undoLog.setId(malloc);

        UndoLogPage page = (UndoLogPage) Database.getBufferPool().getPage(tid, malloc.pid(), Permissions.READ_ONLY);
        page.insertUndoLog(malloc, undoLog);

        UndoLogSuperPage superPage = (UndoLogSuperPage) Database.getBufferPool().getPage(
                tid, new PageId(undoPageTableName, 0), Permissions.READ_ONLY);
        superPage.markTransactionOn(tid, true);
        superPage.markDirty(true);

        undoLogs.add(undoLog);
        createInsertRedoLog(tid, undoLog);
        return undoLog;
    }

    /**
     * 创建关于插入的 undo log，同时创建对应的关于 undo 的 redo log
     *
     * @param tid    事务
     * @param insert 已分配空间并且待插入的记录
     * @return undo log
     */
    public UndoLog createInsertUndoLog(TransactionId tid, @NonNull Record insert) throws DbException {
        if (!insert.isValid())
            throw new DbException("Insert record can not be invalid.");
        Record delete = insert.clone();
        delete.setValid(false);

        return createUndoLog(tid, delete);
    }


    /**
     * 创建关于更新的 undo log，同时创建对应的关于 undo 的 redo log
     *
     * @param tid    事务
     * @param update 更新前的记录
     * @return undo log
     */
    public UndoLog createUpdateUndoLog(TransactionId tid, @NonNull Record update) throws DbException {
        if (!update.isValid())
            throw new DbException("Update record can not be invalid.");

        return createUndoLog(tid, update.clone());
    }


    /**
     * 创建关于更新的 delete log，同时创建对应的关于 undo 的 redo log
     *
     * @param tid    事务
     * @param prev 原先的记录
     * @return undo log
     */
    public UndoLog createDeleteUndoLog(TransactionId tid, @NonNull Record prev) throws DbException {
        Record insert = prev.clone();
        insert.setValid(true);
        return createUndoLog(tid, insert);
    }


    /**
     * 事务提交时，<strong>将 redo log 刷盘，并且将事务 undo log 与删除的记录交由 {@link Purge} 线程清理</strong>
     *
     * @param tid 事务
     * @throws DbException
     */
    public void transactionCommit(TransactionId tid) throws DbException, IOException {
        if (!redoLogMap.containsKey(tid) && !undoLogMap.containsKey(tid)) {
            return;
        }
        for (RedoLog redoLog : redoLogMap.getOrDefault(tid, new ArrayList<>())) {
            if (redoLog instanceof DeleteRedoLog del) {
                Purge.addLogicalDeleteRecord(del.getRecordId());
            }
            writeRedoLog(redoLog);
        }
        for (UndoLog undoLog : undoLogMap.getOrDefault(tid, new ArrayList<>())) {
            Purge.addLogicalDeleteUndoLog(undoLog.getId());
        }
        redoLogMap.remove(tid);
        undoLogMap.remove(tid);
    }

    /**
     * 事务取消时，<strong>逆序重做 undo log，生成 {@link AbortRedoLog} 并刷盘，将回滚待删除的记录交由 {@link Purge} 处理</strong>
     * <P>恢复时不能调用此方法进行回滚，此方法仅适用于内存回滚</P>
     *
     * @param tid 事务
     * @throws DbException
     */
    public void transactionAbort(TransactionId tid) throws DbException, IOException {
        List<UndoLog> undoLogs = undoLogMap.getOrDefault(tid, new ArrayList<>());
        Collections.reverse(undoLogs);
        for (UndoLog undoLog : undoLogs) {
            if (!undoLog.getRecord().isValid()) {
                Purge.addLogicalDeleteRecord(undoLog.getRecordId());
            }
            undoLog.undo();
        }
        createAbortRedoLog(tid);
        for (RedoLog redoLog : redoLogMap.getOrDefault(tid, new ArrayList<>())) {
            writeRedoLog(redoLog);
        }
        redoLogMap.remove(tid);
        undoLogMap.remove(tid);
    }

    static PriorityQueue<RedoLog> queue = new PriorityQueue<RedoLog>((a, b) -> (int) (a.getLsn() - b.getLsn()));

    /**
     * 推进检查点，并决定是否丢弃检查点之前(不包括)的数据
     * <P>如果检查点推进很少，此方法不会截断日志文件，因为截断是个非常耗时的工作</P>
     *
     * @param ckp 新的检查点
     * @see #TRUNCATE_HOLDER
     */
    public synchronized void pushCheckPoint(long ckp) throws IOException {
        long offset = getOffset(ckp);
        long length = dbFile.getLength();
        if (offset <= HEAD_SIZE || length < offset) {
            return;
        }
        this.checkPoint = ckp;

        // 需要截断的长度
        long len = offset - HEAD_SIZE;
        if (len < TRUNCATE_HOLDER) {
            dbFile.write(8, new ByteArray(this.checkPoint));
            return;
        }
        this.truncate += len;
        ByteArray byteAr = new ByteList();
        byteAr.writeLong(maxLsn.get()).writeLong(checkPoint).writeLong(truncate);
        byteAr.writeByteArray(dbFile.read(offset, (int) (length - offset)));

        dbFile.write(0, byteAr);
        dbFile.setLength(byteAr.length());
    }

    /**
     * @return 当前最新的LSN（比所有日志都大）
     */
    public long getCurrentLsn() {
        return maxLsn.get();
    }

    /**
     * 返回从检查点开始之后的有效的日志迭代器
     * <P>此函数仅供恢复使用，他不是线程安全的</P>
     *
     * @return 迭代器
     */
    public Iterator<RedoLog> iterator() throws IOException {
        List<RedoLog> ans = new ArrayList<>();
        long l = checkPoint, r = maxLsn.get();
        if (l >= r) {
            return ans.iterator();
        }
        ByteArray byteAr = dbFile.read(getOffset(l), (int) (r - l));
        while (true) {
            try {
                RedoLog parse = RedoLog.parse(byteAr);
                if (parse != null) {
                    ans.add(parse);
                }
            } catch (ParseException e) {
                throw new RuntimeException(e);
            } catch (NoSuchElementException e) {
                break;
            }
        }

        return ans.iterator();
    }
}
