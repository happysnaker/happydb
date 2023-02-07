package happydb.index.hash;

import happydb.common.ByteArray;
import happydb.common.Catalog;
import happydb.common.Database;
import happydb.index.EntryId;
import happydb.index.btree.BTreeLeafEntry;
import happydb.storage.*;


import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * 哈希页
 *
 * @Author happysnaker
 * @Date 2023/1/4
 * @Email happysnaker@foxmail.com
 */
public class HashPage extends AbstractPage {

    /**
     * 存储槽位，表示 tuple 是否存在，由于一字节有八位，因此最后一字节可能会有些位数未使用
     */
    byte[] header;
    /**
     * 存储页中的条目
     */
    HashEntry[] entries;

    Type type;

    public HashPage(ByteArray byteAr, PageId pid) {
        super.pid = pid;
        this.type = Catalog.getFieldTypeFromIndexTableName(pid.getTableName());


        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = byteAr.readByte();

        entries = new HashEntry[getMaxNumEntries()];
        try {
            // 分配并读取该页的实际记录
            for (int i = 0; i < entries.length; i++) {
                entries[i] = readNextEntry(byteAr, i);
                if (entries[i] != null) {
                    entries[i].setEntryId(new EntryId(pid, i));
                }
            }
        } catch (NoSuchElementException | ParseException e) {
            e.printStackTrace();
        }
    }

    private HashEntry readNextEntry(ByteArray data, int i) throws ParseException {
        ByteArray byteAr = data.readByteArray(type.getLen() + 4 + 4);
        if (isSlotUsed(i, header)) {
            return HashEntry.parse(byteAr, type, Catalog.getTableNameFromIndexTableName(pid.getTableName()));
        }
        return null;
    }

    @Override
    public ByteArray serialized() {
        ByteArray byteAr = ByteArray.allocate(BufferPool.getPageSize());
        for (byte b : header) {
            byteAr.writeByte(b);
        }
        for (var entry : entries) {
            byteAr.writeByteArray(entry == null ? ByteArray.allocate(type.getLen() + 8) : entry.serialized());
        }
        return byteAr;
    }

    private int getHeaderSize() {
        return (int) Math.ceil(getMaxNumEntries() / 8f);
    }

    @Override
    public int getMaxNumEntries() {
        int pageSize = BufferPool.getPageSize();
        int entrySize = type.getLen() + 4 + 4;
        return (int) Math.floor((pageSize * 8f) / (entrySize * 8f + 1));
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
     * 读取一个 entry
     *
     * @param i 槽位
     * @return
     */
    public HashEntry readEntry(int i) {
        if (!isSlotUsed(i, header)) {
            return null;
        }
        return entries[i];
    }

    /**
     * 如果不存在则添加，此方法是原子的
     *
     * @param i     待放置的下标
     * @param entry 待放置的条目，如果放置成功，条目的 ID 将会被自动设置
     * @return 返回真则放置成功
     */
    public synchronized boolean putIfAbsent(int i, HashEntry entry) {
        if (!isSlotUsed(i, header)) {
            this.entries[i] = entry;
            markSlotUsed(i, true, header);
            entry.setEntryId(new EntryId(pid, i));
            return true;
        }
        return false;
    }

    /**
     * 清空所有的使用未，供扩容使用
     */
    public synchronized void clear() {
        for (int i = 0; i < header.length; i++) {
            markSlotUsed(i, false, header);
        }
    }


    public Iterator<HashEntry> iterator() {
        List<HashEntry> list = new ArrayList<>();
        for (int i = 0; i < entries.length; i++) {
            if (isSlotUsed(i, header)) {
                list.add(entries[i]);
            }
        }
        return list.iterator();
    }
}
