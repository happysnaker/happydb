package happydb.log;

import happydb.common.*;
import happydb.exception.DbException;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.TransactionId;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 重做日志，<strong>重做日志保存的 Record 应该是它们的拷贝，他不应该依赖于传入的参数</strong>
 *
 * @Author happysnaker
 * @Date 2022/11/29
 * @Email happysnaker@foxmail.com
 */
@Data
@Getter
@Setter
public class UndoLog implements DbSerializable {
    public static final int EXTRA_SIZE = 4 + 4 + 4 + 8;
    private UndoLogId id;

    /**
     * 此 undo log 是由事务按顺序产生的第几个 Log
     */
    private int undoLogNo;
    /**
     * 重做位置指定的行记录
     */
    private Record record;
    /**
     * 重做位置
     */
    private RecordId recordId;
    /**
     * 产生此重做日志的事务 ID
     */
    private TransactionId tid;

    /**
     * 引用技术
     */
    private Set<TransactionId> reference = ConcurrentHashMap.newKeySet();

    public UndoLog(int undoLogNo, Record record, RecordId recordId, TransactionId tid) {
        this.undoLogNo = undoLogNo;
        this.record = record.clone();   // deep copy
        this.recordId = recordId;
        this.tid = tid;
        this.record.setRecordId(recordId);
    }

    public UndoLog(UndoLogId id) {
        this.id = id;
    }

    /**
     * 对数据页进行回滚，<strong>调用前需保证在指定行持有锁或在重启恢复时调用，此方法会覆盖指定位置的数据</strong>
     * <P><B>回滚操作是幂等的，回滚会使得页面弄脏</B></P>
     */
    public void undo() throws DbException {
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, recordId.getPid(), Permissions.READ_ONLY);

        try {
            page.updateRecord(recordId, record);
        } catch (DbException e) {
            // 无论指定位置之前是否存在数据，都会强制覆盖
            page.insertRecord(recordId, record);

            HeapPageManager pm = (HeapPageManager) Database.getCatalog()
                    .getPageManager(record.getRecordId().getPid().getTableName());
            pm.malloc(recordId);
        }
        page.markDirty(true);
    }


    /**
     * 获取此 undo log 指向的上一个版本记录，增加此日志的引用计数
     * @param tid 获取引用的事务，用于更新引用计数
     * @return 上一个版本记录，可能为空
     */
    public Record getLastVersion(TransactionId tid) {
        reference.add(tid);
        return this.record.isValid() ? record : null;
    }

    /**
     * 获取正在引用此日志的事务集合，注意，我们没有提供释放引用的接口。
     * <P>
     * {@link Purge} 线程需判断事务集合是否都已完成来判断是否有事务引用此日志
     * </P>
     * @return 引用计数
     */
    public Set<TransactionId> getReferenceCount() {
        return new HashSet<>(reference);
    }

    @Override
    public ByteArray serialized() {
        ByteArray byteAr = new ByteList();
        return byteAr.writeInt(undoLogNo)
                .writeByteArray(record.serialized())
                .writeInt(recordId.getPid().getPageNumber())
                .writeInt(recordId.getRecordNumber())
                .writeLong(tid.getXid());
    }

    /**
     * 当给定 {@link #id} 后，此类可以从字节数组中反序列化
     * @param byteAr 字节数组
     */
    public void deserialize(ByteArray byteAr) throws ParseException {
        this.undoLogNo = byteAr.readInt();

        TableDesc td = Database.getCatalog().getTableDesc(id.getReallyTableName());
        this.record = new Record(td);
        this.record.deserialize(byteAr);

        int pageNo = byteAr.readInt(), recordNo = byteAr.readInt();
        this.recordId = new RecordId(new PageId(id.getReallyTableName(), pageNo), recordNo);
        this.record.setRecordId(this.recordId);

        this.tid = new TransactionId(byteAr.readLong());
    }
}
