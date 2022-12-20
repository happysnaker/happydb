package happydb.log;

import happydb.common.ByteArray;
import happydb.common.Database;
import happydb.exception.DbException;
import happydb.storage.AbstractPage;
import happydb.storage.BufferPool;
import happydb.storage.PageId;
import happydb.storage.PageManager;
import happydb.transaction.TransactionId;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * {@link UndoLogPage} 中页号为 0 的页为超级页，通过超级页可以快速获取事务是否在此类页面上含有 undo log
 * @Author happysnaker
 * @Date 2022/11/29
 * @Email happysnaker@foxmail.com
 */
public class UndoLogSuperPage extends AbstractPage {

    private final byte[] header = new byte[BufferPool.getPageSize()];

    private final PageManager pm;

    public UndoLogSuperPage(ByteArray byteAr, PageId pid) {
        for (int i = 0; i < header.length; i++) {
            header[i] = byteAr.readByte();
        }
        this.pid = pid;
        this.pm = Database.getCatalog().getPageManager(pid.getTableName());
    }

    @Override
    public ByteArray serialized() {
        ByteArray byteAr = ByteArray.allocate(BufferPool.getPageSize());
        for (byte b : header) {
            byteAr.writeByte(b);
        }
        return byteAr;
    }

    public synchronized boolean transactionOn(TransactionId tid) {
        int xid = (int) tid.getXid();
        if (xid < 0)
            throw new IllegalArgumentException("事务 ID 小于 0");
        if (xid < header.length) {
            return isSlotUsed(xid, header);
        } else {
            return true;
        }
    }

    /**
     * 标记事务在此表页面上含有 undo log
     */
    public synchronized void markTransactionOn(TransactionId tid, boolean on) {
        int xid = (int) tid.getXid();
        if (xid < 0)
            throw new IllegalArgumentException("事务 ID 小于 0");
        if (xid < header.length) {
            boolean prev = isSlotUsed(xid, header);
            markSlotUsed(xid, on, header);
            if (on && !prev) {
                try {
                    pm.writePage(this);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            if (!on && prev) {
                try {
                    pm.writePage(this);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * 返回事务在此表上持有的 Undo Log
     * @param tid 事务
     * @return 按照 {@link UndoLog#getUndoLogNo()} 排序返回，如果没有，会返回空列表
     */
    public List<UndoLog> getTransactionUndoLog(TransactionId tid) throws IOException, DbException {
        int xid = (int) tid.getXid();
        if (xid < 0)
            throw new IllegalArgumentException("事务 ID 小于 0");
        if (xid < header.length && !isSlotUsed(xid, header))
            return new ArrayList<>();

        ArrayList<UndoLog> logs = new ArrayList<>();
        UndoLogPageManager pm = (UndoLogPageManager) Database.getCatalog().getPageManager(pid.getTableName());

        Iterator<UndoLogPage> iterator = pm.iterator(tid);
        while (iterator.hasNext()) {
            var it = iterator.next().iterator();
            while (it.hasNext()) {
                UndoLog next = it.next();
                if (next.getTid().equals(tid)) {
                    logs.add(next);
                }
            }
        }
        logs.sort(Comparator.comparingInt(UndoLog::getUndoLogNo));
        return logs;
    }

    @Override
    public int getMaxNumEntries() {
        throw new RuntimeException("不支持的操作");
    }

    @Override
    public List<Integer> getEmptySlots() {
        throw new RuntimeException("不支持的操作");
    }
}
