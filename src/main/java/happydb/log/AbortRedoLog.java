package happydb.log;

import happydb.common.*;
import happydb.exception.DbException;
import happydb.execution.BTreeSeqScan;
import happydb.execution.Predicate;
import happydb.parser.InsertParser;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author happysnaker
 * @Date 2022/11/29
 * @Email happysnaker@foxmail.com
 */
public class AbortRedoLog implements RedoLog{
    /**
     * 日志的 LSN
     */
    private long lsn = -1;
    /**
     * 回滚的事务
     */
    private final TransactionId tid;

    public AbortRedoLog(TransactionId tid) {
        this.tid = tid;
    }


    public AbortRedoLog(ByteArray byteAr) {
        if (byteAr.nextBytes() < size() || byteAr.readInt() != size() - 4) {
            throw new RuntimeException("字节数组长度不足以容纳此日志");
        }
        if (byteAr.readByte() != TRANSACTION_ABORT) {
            throw new IllegalStateException("不合法的类型，日志类型与此类型不符合");
        }
        setLsn(byteAr.readLong());
        this.tid = new TransactionId(byteAr.readLong());
    }

    @Override
    public ByteArray serialized() {
        return new ByteList()
                .writeInt(size() - 4)
                .writeByte(getType())
                .writeLong(getLsn())
                .writeLong(xid().getXid());
    }

    @Override
    public byte getType() {
        return TRANSACTION_ABORT;
    }

    @Override
    public int size() {
        return 4 + 1 + 8 + 8;
    }

    /**
     * 遍历所有表的 undo log page，获取事务的 udno log，按照 undoLogNo 逆序遍历，进行回滚重做（如果页的 LSN 小于此日志的）
     * @throws DbException
     */
    @Override
    public void redoIfNecessary() throws DbException {
        List<UndoLog> logs = new ArrayList<>();
        for (String tableName : Database.getCatalog().getAllReallyTableName()) {
            UndoLogSuperPage superPage = (UndoLogSuperPage) Database.getBufferPool().getPage(tid, new PageId(
                    tableName + UndoLogId.UNDO_LOG_TABLE_NAME_SUFFIX, 0), Permissions.READ_ONLY);

            // 修复索引已经插入但元组页丢失的问题
            if (superPage.transactionOn(tid)) {
                FixRecordMissingScan scan = new FixRecordMissingScan(tid, tableName, null, null);
                scan.open();
                while (scan.hasNext()) {
                    scan.next();
                }
            }
            try {
                logs.addAll(superPage.getTransactionUndoLog(tid));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        logs.sort((a, b) -> b.getUndoLogNo() - a.getUndoLogNo());
        for (UndoLog log : logs) {
            HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, log.getRecordId().getPid(), Permissions.READ_ONLY);
            if (hp.getLsn() < this.lsn) {
                log.undo();
                hp.markDirty(true);
            }
        }
    }

    @Override
    public TransactionId xid() {
        return tid;
    }

    @Override
    public PageId getPageId() {
        throw new RuntimeException("AbortRedoLog 不支持此操作");
    }

    @Override
    public void setLsn(long lsn) {
        this.lsn = lsn;
    }

    @Override
    public long getLsn() {
        return lsn;
    }

    /**
     * 事务突然崩溃，undo 可能丢失，record 也可能丢失，但索引却建立，这是本项目的缺陷，因此需要扫描索引中存在但数据页中不存在的记录，并逻辑删除
     */
    static class FixRecordMissingScan extends BTreeSeqScan {
        public FixRecordMissingScan(TransactionId tid, String tableName, String tableAlias, Predicate predicate) {
            super(tid, tableName, tableAlias, predicate);
        }

        @Override
        protected Record fetchNext() throws DbException {
            Record record = null;
            while (iterator.hasNext()) {
                RecordId next = iterator.next();
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, next.getPid(), Permissions.READ_ONLY);
                try {
                    record = page.readRecord(next);
                } catch (DbException e) {
                    Debug.log("Fix missing record " + next);
                    // 参考 recovery 讲解，修复事务未提交突然崩溃的问题
                    Record deleteRecord = InsertParser.createDefaultRecord(
                            Database.getCatalog().getTableDesc(tableName));
                    deleteRecord.setValid(false);
                    deleteRecord.setLastModify(new TransactionId(-1));
                    page.insertRecord(next, deleteRecord);

                    HeapPageManager pm = (HeapPageManager) Database.getCatalog().getPageManager(tableName);
                    pm.malloc(next);

                    try {
                        // flush
                        pm.writePage(page);
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
            return null;
        }
    }
}
