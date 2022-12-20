package happydb.log;

import happydb.common.ByteArray;
import happydb.common.Database;
import happydb.common.DbSerializable;
import happydb.exception.DbException;
import happydb.storage.*;
import lombok.Getter;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static happydb.log.UndoLog.EXTRA_SIZE;
import static happydb.storage.Record.HIDDEN_SIZE;

/**
 * 此类与 {@link HeapPage} 具有类似的结构
 *
 * @Author happysnaker
 * @Date 2022/11/29
 * @Email happysnaker@foxmail.com
 */
public class UndoLogPage extends AbstractPage implements DataPage, DbSerializable {
    /**
     * 存储槽位，表示 undo log 是否存在，由于一字节有八位，因此最后一字节可能会有些位数未使用
     */
    byte[] header;
    /**
     * 存储页中的日志
     */
    UndoLog[] logs;
    TableDesc td;
    @Getter
    private long lsn;

    private volatile long firstDirtyLsn;


    /**
     * 从字节数组中初始化页，这会将读指针推进 {@link BufferPool#getPageSize()} 字节
     *
     * @param byteAr 包含一页数据的字节数组
     */
    public UndoLogPage(ByteArray byteAr, PageId pid) {
        this.pid = pid;
        this.td = Database.getCatalog().getTableDesc(new UndoLogId(pid, -1).getReallyTableName());
        this.lsn = byteAr.readLong();

        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = byteAr.readByte();

        logs = new UndoLog[getMaxNumEntries()];
        try {
            // 分配并读取该页的实际记录
            for (int i = 0; i < logs.length; i++)
                logs[i] = readNextLog(byteAr, i);
        } catch (NoSuchElementException | ParseException e) {
            e.printStackTrace();
        }
    }


    public static ByteArray createEmptyPageData() {
        return ByteArray.allocate(BufferPool.getPageSize());
    }

    private UndoLog readNextLog(ByteArray data, int i) throws ParseException {
        int logSize = td.getRecordSize() + HIDDEN_SIZE + EXTRA_SIZE;
        ByteArray byteAr = data.readByteArray(logSize);

        if (isSlotUsed(i, header)) {
            UndoLog log = new UndoLog(new UndoLogId(pid, i));
            log.deserialize(byteAr);
            return log;
        }
        return null;
    }


    @Override
    public synchronized ByteArray serialized() {
        int logSize = td.getRecordSize() + HIDDEN_SIZE + EXTRA_SIZE;
        ByteArray byteAr = ByteArray.allocate(BufferPool.getPageSize());
        byteAr.writeLong(lsn);

        for (byte b : this.header) {
            byteAr.writeByte(b);
        }
        for (int i = 0; i < logs.length; i++) {
            ByteArray array;
            if (!isSlotUsed(i, header)) {
                array = ByteArray.allocate(logSize);
            } else {
                array = logs[i].serialized();
//                Debug.log(array.length() + " <==> " + logSize);
            }
            byteAr.writeByteArray(array);

        }
        int zeroLen = BufferPool.getPageSize() - byteAr.getWritePos();
        int i = BufferPool.getPageSize() - 8 - header.length - logSize * logs.length;


        byteAr.writeByteArray(ByteArray.allocate(zeroLen));
        return byteAr;
    }

    private int getHeaderSize() {
        return (int) Math.ceil(getMaxNumEntries() / 8f);
    }

    @Override
    public int getMaxNumEntries() {
        int pageSize = BufferPool.getPageSize();
        int logSize = td.getRecordSize() + HIDDEN_SIZE + EXTRA_SIZE;
        return (int) Math.floor(((pageSize - 8) * 8f) / (logSize * 8f + 1));
    }

    @Override
    public List<Integer> getEmptySlots() {
        List<Integer> ans = new ArrayList<>();
        for (int i = 0; i < getMaxNumEntries(); i++) {
            if (!isSlotUsed(i, header)) {
                ans.add(i);
            }
        }
        return ans;
    }

    /**
     * 向页面中指定插槽位插入记录，并重新设置记录的 ID
     *
     * @param i   指定插槽
     * @param log
     * @throws DbException 插入位置非空
     */
    public void insertUndoLog(int i, UndoLog log) throws DbException {
        if (i < 0 || i >= logs.length) {
            throw new IndexOutOfBoundsException();
        }
        if (isSlotUsed(i, header)) {
            throw new DbException("插入位置非空");
        }
        logs[i] = log;
        log.setId(new UndoLogId(pid, i));
        markSlotUsed(i, true, header);
    }


    /**
     * 向页面中指定插槽位插入记录，并重新设置记录的 ID
     *
     * @param rid    指定插槽
     * @param record 记录
     * @throws DbException 插入位置非空
     */
    public void insertUndoLog(UndoLogId rid, UndoLog record) throws DbException {
        if (!rid.getPid().equals(pid)) {
            throw new DbException("模式不匹配");
        }
        insertUndoLog(rid.getUndoLogNumber(), record);
    }

    /**
     * 将记录从指定槽位删除
     *
     * @param recordId
     * @throws DbException
     */
    public void deleteUndoLog(UndoLogId recordId) throws DbException {
        int i = recordId.getUndoLogNumber();
        if (i < 0 || i >= logs.length) {
            throw new IndexOutOfBoundsException();
        }
        if (!isSlotUsed(i, header) || !recordId.getPid().equals(pid)) {
            throw new DbException("日志不存在");
        }
        markSlotUsed(i, false, header);
        logs[i] = null;
    }

    /**
     * 读取指定槽位的记录，此方法不会检查记录有效位
     *
     * @param recordId 槽位
     * @return
     * @throws DbException 如果记录不存在
     */
    public UndoLog readUndoLog(UndoLogId recordId) throws DbException {
        int i = recordId.getUndoLogNumber();
        if (i < 0 || i >= logs.length) {
            throw new IndexOutOfBoundsException();
        }
        if (!isSlotUsed(i, header)) {
            throw new DbException("日志不存在");
        }
        return logs[i];
    }


    @Override
    protected HeapPage clone() throws CloneNotSupportedException {
        Page clone = (Page) super.clone();
        return new HeapPage(clone.serialized(), pid);
    }

    @Override
    public synchronized void setLsn(long lsn) {
        this.lsn = Math.max(this.lsn, lsn);
    }

    /**
     * 返回此页面上有效日志的迭代器
     */
    public Iterator<UndoLog> iterator() {
        List<UndoLog> ret = new ArrayList<>();
        for (int i = 0; i < this.logs.length; i++) {
            if (isSlotUsed(i, header)) {
                ret.add(logs[i]);
            }
        }
        return ret.iterator();
    }

    @Override
    public long getFirstDirtyLsn() {
        return firstDirtyLsn;
    }

    @Override
    public void markDirty(boolean dirty) {
        synchronized (Database.getBufferPool()) {
            if (!isDirty() && dirty) {
                this.firstDirtyLsn = getLsn();
            }
            super.markDirty(dirty);
        }
    }
}
