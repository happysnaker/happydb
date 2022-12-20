package happydb.log;

import happydb.common.*;
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
public class UpdateRedoLog implements RedoLog {

    /**
     * 待重新插入的数据
     */
    private final Record record;
    /**
     * 待重做数据 ID
     */
    private final RecordId recordId;
    /**
     * 对应的事务 ID
     */
    private final TransactionId tid;
    /**
     * 日志的 LSN
     */
    @Getter@Setter
    private long lsn = -1;

    public UpdateRedoLog(Record data, RecordId recordId, TransactionId tid) {
        this.record = data.clone();
        this.recordId = recordId;
        this.tid = tid;
    }


    public UpdateRedoLog(ByteArray byteAr) throws ParseException {
        byteAr = byteAr.readByteArray(byteAr.readInt());

        byte type = byteAr.readByte();
        if (type != UPDATE_REDO) {
            throw new IllegalStateException("不合法的类型，日志类型与此类型不符合");
        }

        setLsn(byteAr.readLong());
        this.tid = new TransactionId(byteAr.readLong());

        int tableNameLen = byteAr.readInt();
        String tableName = byteAr.readString(tableNameLen);
        var pid = new PageId(tableName, byteAr.readInt());
        this.recordId = new RecordId(pid, byteAr.readInt());

        Record record = new Record(Database.getCatalog().getTableDesc(tableName));
        record.deserialize(byteAr);
        record.setRecordId(recordId);
        this.record = record;
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
                .writeInt(recordId.getRecordNumber())
                .writeByteArray(record.serialized());
        return new ByteList()
                .writeInt(byteAr.length())
                .writeByteArray(byteAr);
    }

    @Override
    public byte getType() {
        return UPDATE_REDO;
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
        page.updateRecord(recordId, record);
        page.markDirty(true);
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
