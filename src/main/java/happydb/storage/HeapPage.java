package happydb.storage;

import happydb.common.ByteArray;
import happydb.common.Database;
import happydb.exception.DbException;
import happydb.log.DataPage;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import static happydb.storage.Record.HIDDEN_SIZE;

/**
 * 堆文件页，存储行记录，此页提供了对记录的增删改查操作，<strong>但请注意，此类绝不修改或检查记录的有效位（隐藏字段）</strong>
 *
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
@NoArgsConstructor
public class HeapPage extends AbstractPage implements Cloneable, DataPage {

    /**
     * 存储槽位，表示 tuple 是否存在，由于一字节有八位，因此最后一字节可能会有些位数未使用
     */
    byte[] header;
    /**
     * 存储页中的元组
     */
    Record[] records;
    TableDesc td;
    @Getter
    private volatile long lsn;

    private volatile long firstDirtyLsn;


    /**
     * 从字节数组中初始化页，这会将读指针推进 {@link BufferPool#getPageSize()} 字节
     *
     * @param byteAr 包含一页数据的字节数组
     */
    public HeapPage(ByteArray byteAr, PageId pid) {
        this.pid = pid;
        this.td = Database.getCatalog().getTableDesc(pid.getTableName());
        this.lsn = byteAr.readLong();

        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = byteAr.readByte();

        records = new Record[getMaxNumEntries()];
        try {
            // 分配并读取该页的实际记录
            for (int i = 0; i < records.length; i++)
                records[i] = readNextRecord(byteAr, i);
        } catch (NoSuchElementException | ParseException e) {
            e.printStackTrace();
        }
    }

    /**
     * 仅仅只是为了测试
     * @param pid
     */
    public HeapPage(PageId pid) {
        this.pid = pid;
        this.td = Database.getCatalog().getTableDesc(pid.getTableName());
        header = new byte[getHeaderSize()];
        records = new Record[getMaxNumEntries()];
        Arrays.fill(header, (byte) 0);
    }


    public static ByteArray createEmptyPageData() {
        return ByteArray.allocate(BufferPool.getPageSize());
    }

    private Record readNextRecord(ByteArray data, int i) throws ParseException {
        int recordSize = td.getRecordSize() + HIDDEN_SIZE;
        ByteArray byteAr = data.readByteArray(recordSize);

        if (isSlotUsed(i, header)) {
            Record record = new Record(td);
            record.deserialize(byteAr);
            return record;
        }
        return null;
    }


    @Override
    public synchronized ByteArray serialized() {
        int recordSize = td.getRecordSize() + HIDDEN_SIZE;
        ByteArray byteAr = ByteArray.allocate(BufferPool.getPageSize());
        byteAr.writeLong(lsn);


        for (byte b : this.header) {
            byteAr.writeByte(b);
        }
        for (int i = 0; i < records.length; i++) {
            ByteArray array;
            if (!isSlotUsed(i, header)) {
                array = ByteArray.allocate(recordSize);
            } else {
                array = records[i].serialized();
            }
            byteAr.writeByteArray(array);

        }
        int zeroLen = BufferPool.getPageSize() - byteAr.getWritePos();
        int i = BufferPool.getPageSize() - 8 - header.length - recordSize * records.length;


        byteAr.writeByteArray(ByteArray.allocate(zeroLen));
        return byteAr;
    }

    private int getHeaderSize() {
        return (int) Math.ceil(getMaxNumEntries() / 8f);
    }

    @Override
    public int getMaxNumEntries() {
        int pageSize = BufferPool.getPageSize();
        int recordSize = td.getRecordSize() + HIDDEN_SIZE;
        return (int) Math.floor(((pageSize - 8) * 8f) / (recordSize * 8f + 1));
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
     * @param i 指定插槽
     * @param record 记录
     * @throws DbException 插入位置非空
     */
    public void insertRecord(int i, Record record) throws DbException {
        if (i < 0 || i >= records.length) {
            throw new IndexOutOfBoundsException();
        }
        if (isSlotUsed(i, header)) {
            throw new DbException("插入位置非空");
        }
        records[i] = record;
        record.setRecordId(new RecordId(pid, i));
        // markSlotUsed 不能在 records[i] = record 之前
        markSlotUsed(i, true, header);
    }


    /**
     * 向页面中指定插槽位插入记录，并重新设置记录的 ID
     * @param rid 指定插槽
     * @param record 记录
     * @throws DbException 插入位置非空
     */
    public void insertRecord(RecordId rid, Record record) throws DbException {
        if (!rid.getPid().equals(pid)) {
            throw new DbException("模式不匹配");
        }
        insertRecord(rid.getRecordNumber(), record);
    }

    /**
     * 将记录从指定槽位删除，此方法不会修改记录有效位
     * @param recordId
     * @throws DbException
     */
    public void deleteRecord(RecordId recordId) throws DbException {
        int i = recordId.getRecordNumber();
        if (i < 0 || i >= records.length) {
            throw new IndexOutOfBoundsException();
        }
        if (!isSlotUsed(i, header) || !recordId.getPid().equals(pid)) {
            throw new DbException("元组不存在");
        }
        markSlotUsed(i, false, header);
        records[i] = null;
    }

    /**
     * 读取指定槽位的记录，此方法不会检查记录有效位
     * @param recordId 槽位
     * @return
     * @throws DbException 如果记录不存在
     */
    public Record readRecord(RecordId recordId) throws DbException {
        int i = recordId.getRecordNumber();
        if (i < 0 || i >= records.length) {
            throw new IndexOutOfBoundsException();
        }
        if (!isSlotUsed(i, header)) {
            throw new DbException("元组不存在");
        }
        if (records[i].getRecordId() == null) {
            records[i].setRecordId(new RecordId(pid, i));
        }
        return records[i];
    }

    /**
     * 更新指定槽位的记录
     * @param recordId
     * @param newRecord
     * @throws DbException
     */
    public void updateRecord(RecordId recordId, Record newRecord) throws DbException {
        int i = recordId.getRecordNumber();
        if (i < 0 || i >= records.length) {
            throw new IndexOutOfBoundsException();
        }
        if (!isSlotUsed(i, header)) {
            throw new DbException("元组不存在");
        }
        this.records[i] = newRecord;
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
