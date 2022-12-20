package happydb.log;

import happydb.common.ByteArray;
import happydb.common.ByteList;
import happydb.common.Database;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.storage.HeapPage;
import happydb.storage.PageId;
import happydb.storage.Record;
import happydb.storage.RecordId;
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
public class DeleteRedoLog implements RedoLog{
    /**
     * 待重做数据 ID
     */
    @Getter
    private final RecordId recordId;
    /**
     * 对应的事务 ID
     */
    private final TransactionId tid;
    /**
     * 日志的 LSN
     */
    @Setter@Getter
    private long lsn = -1;

    public DeleteRedoLog(RecordId recordId, TransactionId tid) {
        this.recordId = recordId;
        this.tid = tid;
    }


    public DeleteRedoLog(ByteArray byteAr) throws ParseException {
        byteAr = byteAr.readByteArray(byteAr.readInt());

        byte type = byteAr.readByte();
        if (type != DELETE_REDO) {
            throw new IllegalStateException("不合法的类型，日志类型与此类型不符合");
        }

        setLsn(byteAr.readLong());
        this.tid = new TransactionId(byteAr.readLong());

        int tableNameLen = byteAr.readInt();
        String tableName = byteAr.readString(tableNameLen);
        var pid = new PageId(tableName, byteAr.readInt());
        this.recordId = new RecordId(pid, byteAr.readInt());
    }


    @Override
    public ByteArray serialized() {
        ByteArray byteAr = new ByteList();
        byteAr.writeByte(getType())
                .writeLong(getLsn())
                .writeLong(xid().getXid())
                .writeInt(recordId.getPid().getTableName().getBytes(StandardCharsets.UTF_8).length)
                .writeString(recordId.getPid().getTableName())
                .writeInt(recordId.getPid().getPageNumber())
                .writeInt(recordId.getRecordNumber());
        return new ByteList()
                .writeInt(byteAr.length())
                .writeByteArray(byteAr);
    }

    @Override
    public byte getType() {
        return DELETE_REDO;
    }

    @Override
    public int size() {
        return this.serialized().length();
    }

    @Override
    public void redoIfNecessary() throws DbException {
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, recordId.getPid(), Permissions.READ_ONLY);
        if (page.getLsn() >= this.lsn) {
            return;
        }
        try {
            page.readRecord(recordId).setValid(false);
            page.markDirty(true);
        } catch (DbException ignore) {
            // 已经被删除
        }
    }

    @Override
    public TransactionId xid() {
        return tid;
    }

    @Override
    public PageId getPageId() {
        return recordId.getPid();
    }

}
