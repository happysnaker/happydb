package happydb.log;

import happydb.common.*;
import happydb.exception.DbException;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.TransactionId;
import lombok.Getter;
import lombok.Setter;

import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.NoSuchElementException;

/**
 * @Author happysnaker
 * @Date 2022/11/29
 * @Email happysnaker@foxmail.com
 */
public class InsertRedoLog implements RedoLog {

    /**
     * 待重新插入的数据，可能是 {@link UndoLog} 或 {@link happydb.storage.Record}<P>
     * <B>如果是页面中的 {@link happydb.storage.Record}，那么必须保存他们的深拷贝，防止记录发生改变</B> <P>
     * {@link UndoLog} 在内存中绝不会被修改（除了引用计数）
     */
    @Getter // for test
    private final DbSerializable data;
    /**
     * 待重做数据所属的页
     */
    private final PageId pid;
    /**
     * 在业内的插槽
     */
    private final int dataId;
    /**
     * 对应的事务 ID
     */
    private final TransactionId tid;
    /**
     * 日志的 LSN
     */
    @Getter
    @Setter
    private long lsn = -1;

    public InsertRedoLog(DbSerializable data, PageId pid, int dataId, TransactionId tid) {
        if (data instanceof Record) {
            this.data = ((Record) data).clone();
        } else {
            this.data = data;
        }
        this.pid = pid;
        this.dataId = dataId;
        this.tid = tid;
    }


    public InsertRedoLog(ByteArray byteAr) throws ParseException {
        byteAr = byteAr.readByteArray(byteAr.readInt());

        byte type = byteAr.readByte();
        if (type != INSET_REDO && type != INSET_UNDO_REDO) {
            throw new IllegalStateException("不合法的类型，日志类型与此类型不符合");
        }

        setLsn(byteAr.readLong());
        this.tid = new TransactionId(byteAr.readLong());

        int tableNameLen = byteAr.readInt();
        String tableName = byteAr.readString(tableNameLen);
        this.pid = new PageId(tableName, byteAr.readInt());
        this.dataId = byteAr.readInt();

        if (type == INSET_REDO) {
            Record record = new Record(Database.getCatalog().getTableDesc(tableName));
            record.deserialize(byteAr);
            record.setRecordId(new RecordId(pid, dataId));
            this.data = record;
        } else {
            UndoLog log = new UndoLog(new UndoLogId(pid, dataId));
            log.deserialize(byteAr);
            this.data = log;
        }
    }


    @Override
    public ByteArray serialized() {
        ByteArray byteAr = new ByteList();
        byteAr.writeByte(getType())
                .writeLong(getLsn())
                .writeLong(xid().getXid())
                .writeInt(pid.getTableName().getBytes(StandardCharsets.UTF_8).length)
                .writeString(pid.getTableName())
                .writeInt(pid.getPageNumber())
                .writeInt(dataId)
                .writeByteArray(data.serialized());
        return new ByteList()
                .writeInt(byteAr.length())
                .writeByteArray(byteAr);
    }

    @Override
    public byte getType() {
        return data instanceof Record ? INSET_REDO : INSET_UNDO_REDO;
    }

    @Override
    public int size() {
        return this.serialized().length();
    }

    @Override
    public void redoIfNecessary() throws DbException {
        if (data instanceof Record record) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if (page.getLsn() >= this.lsn) {
                return;
            }
            try {
                page.insertRecord(dataId, record);

                // 分配池移除
                HeapPageManager pm = (HeapPageManager) Database.getCatalog().getPageManager(pid.getTableName());
                pm.malloc(new RecordId(pid, dataId));
            } catch (DbException ignore) {
                // 由于 fuzzle ckp 可能落后，因此页面可能已被刷回，因此可能插入位置非空
            }
            page.markDirty(true);
            return;
        }
        if (data instanceof UndoLog log) {
            UndoLogPage page = (UndoLogPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            if (page.getLsn() >= this.lsn) {
                return;
            }
            try {
                page.insertUndoLog(dataId, log);

                // 分配池移除
                UndoLogPageManager pm = (UndoLogPageManager) Database.getCatalog().getPageManager(pid.getTableName());
                pm.malloc(new UndoLogId(pid, dataId));
            } catch (DbException ignore) {
                // 由于 fuzzle ckp 可能落后，因此页面可能已被刷回，因此可能插入位置非空
            }
            page.markDirty(true);
            return;
        }
        throw new DbException("未知的数据");
    }

    @Override
    public TransactionId xid() {
        return tid;
    }

    @Override
    public PageId getPageId() {
        return pid;
    }

}
