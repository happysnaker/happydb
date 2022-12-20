package happydb.replication;

import happydb.common.*;
import happydb.exception.DbException;
import happydb.exception.ParseException;
import happydb.execution.OpIterator;
import happydb.parser.Parser;
import happydb.transaction.TransactionId;
import happydb.transaction.TransactionManager;
import lombok.Getter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Raft 集群中的日志管理器，这是 Raft 算法中的核心类
 *
 * @Author happysnaker
 * @Date 2022/12/10
 * @Email happysnaker@foxmail.com
 */
public class RaftLogManager {
    public static int HOLDER = 500;
    /**
     * 事务产生的语句被存储在此变量中
     */
    public final Map<TransactionId, List<String>> sqlMap = new ConcurrentHashMap<>();

    /**
     * 当前最大的日志下标索引（日志文件中最后一个日志）
     */
    @Getter private volatile long maxIndex;

    /**
     * 当前已经提交的最大日志索引，提交总是连续的
     */
    @Getter private volatile long commitIndex;

    /**
     * 上一次被应用到状态机的日志索引编号
     */
    @Getter private volatile long lastApplied;

    /**
     * 日志，内存中只会存储一定量的日志，如果内存中没有日志，说明磁盘上一定不存在日志
     */
    private final ArrayList<LogEntry> entries = new ArrayList<>();

    /**
     * 日志文件
     */
    private DbFile dbFile;

    /**
     * 添加一条事务执行语句
     *
     * @param tid 事务
     * @param sql sql，不包含 begin 和 commit，此 sql 必须要被事务成功执行
     */
    public void addSql(TransactionId tid, String sql) {
        sqlMap.putIfAbsent(tid, new ArrayList<>());
        sqlMap.get(tid).add(sql);
    }

    /**
     * 事务提交时创建一条日志
     *
     * @param tid 待提交的事务
     * @return 日志，仅包含事务 ID 和事务 SQL
     */
    public LogEntry createLogEntry(TransactionId tid) {
        var log = LogEntry.builder()
                .sqlList(sqlMap.get(tid))
                .term(-1)
                .xid(tid.getXid())
                .build();
        sqlMap.remove(tid);
        return log;
    }

    /**
     * 从磁盘中读取一定数量的日志
     *
     * @param endOffset 日志读取总是从后向前读取，此参数表示待读取最后一个日志的结束偏移 + 1，换句话说，是下一个日志的开始偏移
     * @param endIndex  最后一个日志的日志下标
     * @param readSize  待读取的日志数量，如果大于文件中总日志数量，则以文件中总日志数量为准
     * @return 返回读取的日志列表，列表顺序为磁盘中日志顺序，即索引小的在前
     */
    private List<LogEntry> readLogEntry(long endOffset, long endIndex, int readSize) throws IOException, ParseException {
        List<LogEntry> ans = new ArrayList<>();
        while (endOffset > 24 && readSize > 0) {
            int length = dbFile.read(endOffset - 4, 4).readInt(); // 2
            long offset = endOffset - 4 - length;
            ByteArray read = dbFile.read(offset, length + 4);
            LogEntry entry = LogEntry.parse(read);

            entry.setIndex(endIndex);
            entry.setFileOffset(offset);
            ans.add(entry);

            endIndex--;
            readSize--;
            endOffset = offset;
        }
        Collections.reverse(ans);
        return ans;
    }

    /**
     * 找到 {@link #entries} 中日志索引等于参数的下标
     *
     * @param logIndex 日志索引
     * @return {@link #entries} 的下标，如果没有返回 -1
     */
    private int findListIndexByLogIndex(long logIndex) {
        int i = entries.size() - 1;
        while (i >= 0) {
            if (entries.get(i).getIndex() == logIndex) {
                return i;
            }
            --i;
        }
        return i;
    }

    /**
     * 将日志应用到状态机中
     * <ul>
     * <li> 如果 Log 未存在 xid，则开启事务，将事务 ID 写入 Log 文件中，然后重放事务执行。</li>
     * <li> 如果 Log 存在 xid，如果事务处于回滚状态，这种情况重新开启事务，重新写入 Log 文件，重放事务执行即可。</li>
     * <li> 如果 Log 存在 xid，如果事务处于活跃状态，则提交事务，lastApplied 自增。</li>
     * <li> 如果 Log 存在 xid，如果事务处于提交状态，这是由于事务执行完毕，lastApplied 忘记自增而突然宕机，这种情况由于事务已经执行完毕，lastApplied 自增即可。</li>
     * </ul>
     *
     * @param log 待提交的日志
     */
    private void apply(LogEntry log) throws DbException, IOException {
        assert log.getFileOffset() != -1;

        var tid = new TransactionId(log.getXid());
        TransactionManager tm = Database.getTransactionManager();

        if (tid.getXid() != -1 && tm.isActive(tid)) {
            tm.commit(tid, false);
        } else if (tid.getXid() == -1 || tm.isAborted(tid)) {
            TransactionId begin = tm.begin();
            log.setXid(begin.getXid());
            dbFile.write(log.getFileOffset(), new ByteArray(begin.getXid()));

            // 日志的 sql 在主节点能够正确执行，从节点也必须要能够正确执行
            for (String sql : log.getSqlList()) {
                try {
                    OpIterator parser = Parser.parser(sql, begin);
                    if (parser != null) {
                        parser.open();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            tm.commit(begin, false);
        }
    }


    /**
     * 推进 {@link #lastApplied} 到 {@link #commitIndex}，将日志提交
     */
    public synchronized void commit() throws DbException {
        if (lastApplied == commitIndex) {
            return;
        }
        // 提交 lastApplied + 1 到 commitIndex 之间的日志
        List<LogEntry> commitList;
        if (findListIndexByLogIndex(lastApplied + 1) == -1) {
            LogEntry first = entries.get(0);
            try {
                commitList = readLogEntry(
                        first.getFileOffset(), first.getIndex() - 1, (int) (first.getIndex() - lastApplied - 1));
                commitList.addAll(entries.stream()
                        .filter(e -> e.getIndex() <= commitIndex)
                        .sorted((e1, e2) -> (int) (e1.getIndex() - e2.getIndex()))
                        .toList());
            } catch (IOException | ParseException e) {
                throw new DbException(e);
            }
        } else {
            commitList = entries.stream()
                    .filter(e -> e.getIndex() > lastApplied && e.getIndex() <= commitIndex)
                    .sorted((e1, e2) -> (int) (e1.getIndex() - e2.getIndex()))
                    .toList();
        }

        for (LogEntry entry : commitList) {
            try {
                apply(entry);
                lastApplied++;
                dbFile.write(16, new ByteArray(lastApplied)); // 这里可以放在循环外面统一设置...
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        Debug.log("Commit log successfully! Current lastApplied is " + lastApplied);
    }


    /**
     * 追加一条日志，并写入文件
     *
     * @param log 日志，参数会设置日志的偏移与 ID
     */
    public synchronized void appendLogEntry(LogEntry log) throws IOException {
        maxIndex++;
        log.setIndex(maxIndex);
        log.setFileOffset(dbFile.getLength());

        // 我们假定这两个写中间不会宕机，否则可能需要双写技术或预写日志来保障
        dbFile.append(log.serialized(), true);
        dbFile.write(0, new ByteArray(maxIndex), true);

        entries.add(log);
        while (entries.size() > HOLDER) {
            entries.remove(0);
        }

        Debug.log("Append log " + log + " success, current latest id " + getLastLogEntry().getIndex());
    }

    /**
     * 设置提交索引，这会选择参数和当前服务器最后一个日志索引的最小值
     */
    public synchronized void setCommitIndex(long commitIndex) {
        this.commitIndex = Math.min(commitIndex, entries.get(entries.size() - 1).getIndex());
        try {
            dbFile.write(8, new ByteArray(commitIndex));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 获取最新的日志
     *
     * @return 最新的日志，若无则为 null
     */
    public synchronized LogEntry getLastLogEntry() {
        if (entries.size() == 0) {
            return null;
        }
        return entries.get(entries.size() - 1);
    }

    /**
     * 获取匹配下标索引的日志
     *
     * @return 可能为 null，例如不存在任何日志或下标大于最新的日志
     */
    public synchronized LogEntry getLogEntry(int index) {
        if (entries.size() == 0 || index > maxIndex || index < 0) {
            return null;
        }
        int i = findListIndexByLogIndex(index);
        if (i != -1) {
            return entries.get(i);
        }

        LogEntry first = entries.get(0);
        try {
            return readLogEntry(
                    first.getFileOffset(), first.getIndex() - 1, (int) (first.getIndex() - index)).get(0);
        } catch (IOException | ParseException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 强制追加并覆盖已有的日志，此方法会导致原先的日志丢失，此方法还会尝试提交推进 {@link #lastApplied}
     *
     * @param entries           从编号小到大排序的日志列表
     * @param nextIndex        匹配的位置，如果这个位置存在日志，则会丢弃后面(包括这个位置)所有的日志
     * @param leaderCommitIndex 领导人最新的提交索引，作为自身提交索引的参考
     */
    public synchronized void forceAppendEntries(List<LogEntry> entries, int nextIndex, long leaderCommitIndex) throws IOException, DbException {
        // 需要丢弃已有的日志
        if (nextIndex <= maxIndex) {
            // 预先记录，防止等会丢弃了所有的内存日志导致找不到日志
            // getLogEntry 方法假定内存日志为空，则磁盘中不存在日志
            LogEntry entry = getLogEntry(nextIndex);

            // 丢弃内存中的日志
            int i = findListIndexByLogIndex(nextIndex);
            if (i == -1) {
                this.entries.clear();
            } else {
                for (int j = this.entries.size() - 1; j >= 0; j--) {
                    if (this.entries.get(j).getIndex() >= nextIndex) {
                        this.entries.remove(j);
                    }
                }
            }

            // 丢弃磁盘中的日志
            dbFile.setLength(entry.getFileOffset());
        }

        this.maxIndex = nextIndex - 1;

        for (LogEntry entry : entries) {
            appendLogEntry(entry);
        }

        this.commitIndex = Math.min(maxIndex, leaderCommitIndex);
        commit();
    }

    /**
     * 获取日志列表
     * @param fromIndex 起始日志索引包含在内
     * @param toIndex 终止日志索引包含在内
     * @return 日志列表，返回的日志以深拷贝形式返回，并且其中 xid 被设置为 -1
     */
    public synchronized List<LogEntry> getLogEntries(int fromIndex, int toIndex) {
        if (entries.get(0).getIndex() <= fromIndex) {
            return entries.stream()
                    .filter(e -> e.getIndex() >= fromIndex && e.getIndex() <= toIndex)
                    .map(LogEntry::clone)
                    .peek(e -> e.setXid(-1))
                    .sorted((e1, e2) -> (int) (e1.getIndex() - e2.getIndex()))
                    .collect(Collectors.toList());
        }
        List<LogEntry> list = new ArrayList<>();
        for (int i = fromIndex; i <= toIndex; i++) {
            list.add(getLogEntry(i));
        }
        return list;
    }

    public RaftLogManager(DbFile dbFile) throws Exception {
        this.dbFile = dbFile;
        if (dbFile.getLength() == 0) {
            dbFile.write(0, new ByteList()
                    .writeLong(-1)
                    .writeLong(-1)
                    .writeLong(-1));
        }
        ByteArray head = dbFile.read(0, 24);
        this.maxIndex = head.readLong();
        this.commitIndex = head.readLong();
        this.lastApplied = head.readLong();

        entries.addAll(readLogEntry(dbFile.getLength(), maxIndex, HOLDER));
    }
}
