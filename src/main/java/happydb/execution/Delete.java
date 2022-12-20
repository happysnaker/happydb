package happydb.execution;

import happydb.common.Database;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.log.UndoLog;
import happydb.optimizer.TableStateView;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.TransactionId;

/**
 * @Author happysnaker
 * @Date 2022/12/5
 * @Email happysnaker@foxmail.com
 */
public class Delete extends AbstractOpIterator {

    boolean first = true;

    int rowsAffected;

    OpIterator child;

    TransactionId tid;

    public Delete(UpdateOperateScan child, TransactionId tid) {
        this.child = child;
        this.tid = tid;
    }

    private void doDelete(Record record) throws DbException {
        TableDesc td = child.getTableDesc();

        Record delete = record.clone();
        delete.setValid(false);
        delete.setLastModify(tid);

        UndoLog undoLog = Database.getLogBuffer().createDeleteUndoLog(tid, record);
        delete.setLogPointer(undoLog.getId());

        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, record.getRecordId().getPid(), Permissions.READ_ONLY);
        page.updateRecord(record.getRecordId(), delete);

        Database.getLogBuffer().createDeleteRedoLog(tid, delete.getRecordId());

        TableStateView.getInstance().deleteRecord(child.getTableDesc().getTableName(), delete);
    }

    @Override
    protected void openOpIterator() throws DbException {
        child.open();
        first = true;

        while (child.hasNext()) {
            doDelete(child.next());
            rowsAffected++;
        }
    }

    @Override
    protected void closeOpIterator() throws DbException {
        child.close();
    }

    @Override
    protected Record fetchNext() throws DbException {
        if (!first) {
            return null;
        }
        first = false;
        Record record = new Record(getTableDesc());
        record.setField(0, new IntField(rowsAffected));
        return record;
    }

    @Override
    public void rewind() throws DbException {
        first = true;
    }

    @Override
    public TableDesc getTableDesc() {
        return new TableDesc(new String[]{"rowsAffected"}, new Type[]{Type.INT_TYPE});
    }
}
