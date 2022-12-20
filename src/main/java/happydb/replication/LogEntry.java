package happydb.replication;

import happydb.common.ByteArray;
import happydb.common.ByteList;
import happydb.common.DbSerializable;
import happydb.exception.ParseException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Raft 日志，包含一整个已提交事务语句（除去 begin 和 commit）
 *
 * @Author happysnaker
 * @Date 2022/12/10
 * @Email happysnaker@foxmail.com
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class LogEntry implements DbSerializable, Cloneable {
    /**
     * 事务按顺序产生的 sql 语句
     */
    private List<String> sqlList;

    /**
     * 事务 ID，-1 代表没有事务开启
     */
    private long xid = -1;

    /**
     * 日志在文件中的偏移
     */
    transient private long fileOffset = -1;

    /**
     * 日志产生时领导人的任期
     */
    private long term;

    /**
     * 日志索引下标
     */
    transient private long index;

    @Override
    public ByteArray serialized() {
        ByteArray byteAr = new ByteList();
        byteAr.writeLong(xid);
        for (String s : sqlList) {
            byteAr.writeInt(s.getBytes(StandardCharsets.UTF_8).length);
            byteAr.writeString(s);
        }
        int length = byteAr.length();
        return byteAr.writeInt(length);
    }

    /**
     * 从磁盘序列中读取字节数组初始化
     */
    public static LogEntry parse(ByteArray data) throws ParseException {
        int length = data.readInt(data.length() - 4);
        data = data.subArray(0, data.length() - 4);
        if (length != data.length()) {
            throw new ParseException("Data length not equals.");
        }

        data.rewindReadPos();

        LogEntry entry = new LogEntry();
        entry.xid = data.readLong();
        entry.sqlList = new ArrayList<>();
        while (data.hasNextInt()) {
            length = data.readInt();
            entry.sqlList.add(data.readString(length));
        }
        if (data.nextBytes() != 0) {
            throw new ParseException("Data length not equals.");
        }
        return entry;
    }

    @Override
    public LogEntry clone() {
        try {
            LogEntry clone = (LogEntry) super.clone();
            // TODO: copy mutable state here, so the clone can't change the internals of the original
            clone.setSqlList(new ArrayList<>(sqlList));
            clone.setXid(xid);
            clone.setTerm(term);
            clone.setFileOffset(fileOffset);
            clone.setIndex(index);
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
