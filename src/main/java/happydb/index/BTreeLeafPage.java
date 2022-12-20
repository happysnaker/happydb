package happydb.index;

import happydb.common.ByteArray;
import happydb.common.Catalog;
import happydb.exception.DbException;
import happydb.execution.Predicate;
import happydb.storage.BufferPool;
import happydb.storage.Field;
import happydb.storage.PageId;
import lombok.Getter;
import lombok.Setter;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @Author happysnaker
 * @Date 2022/11/21
 * @Email happysnaker@foxmail.com
 */
public class BTreeLeafPage extends BTreePage {
    private final byte[] header;
    private final BTreeLeafEntry[] entries;

    @Getter
    @Setter
    private int leftSibling;
    @Getter
    @Setter
    private int rightSibling;

    public BTreeLeafPage(ByteArray byteAr, PageId pid){
        super(byteAr.readByte(), new PageId(pid.getTableName(), byteAr.readInt()), pid);

        assert category == LEAF;

        this.leftSibling = byteAr.readInt();
        this.rightSibling = byteAr.readInt();

        int numSlots = getMaxNumEntries();

        this.header = new byte[getHeaderSize()];
        this.entries = new BTreeLeafEntry[numSlots];

        for (int i = 0; i < header.length; i++) {
            header[i] = byteAr.readByte();
        }
        for (int i = 0; i < entries.length; i++) {
            try {
                entries[i] = BTreeLeafEntry.parse(byteAr, type, Catalog.getTableNameFromIndexTableName(pid.getTableName()));
                entries[i].setEntryId(new EntryId(pid, i));
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public ByteArray serialized() {
        ByteArray byteAr = ByteArray.allocate(BufferPool.getPageSize());
        byteAr.writeByte(category)
                .writeInt(parent.getPageNumber())
                .writeInt(leftSibling)
                .writeInt(rightSibling);
        for (byte b : header) {
            byteAr.writeByte(b);
        }
        for (BTreeLeafEntry entry : entries) {
            byteAr.writeByteArray(entry.serialized());
        }
        return byteAr;
    }

    @Override
    public int getMaxNumEntries() {
        // key 的长度 + pageNo + recordNo
        int bitsPerTupleIncludingHeader = (type.getLen() + 4 + 4) * 8 + 1;
        // 三个额外指针，一字节类别
        int extraBits = 3 * 4 * 8 + 8;
        return (BufferPool.getPageSize() * 8 - extraBits) / bitsPerTupleIncludingHeader;
    }


    /**
     * 计算 B+ 内部页页眉中的字节数，每个条目占用 entrySize 字节
     *
     * @return 标头中的字节数
     */
    private int getHeaderSize() {
        // 实际需要 getMaxNumEntries() + 1 个槽位
        int slotsPerPage = getMaxNumEntries(), hb = (slotsPerPage / 8);
        if (hb * 8 < slotsPerPage) {
            hb++;
        }
        return hb;
    }

    @Override
    public List<Integer> getEmptySlots() {
        List<Integer> ans = new ArrayList<>();
        for (int i = 0; i < entries.length; i++) {
            if (!isSlotUsed(i, header)) {
                ans.add(i);
            }
        }
        return ans;
    }


    /**
     * 从页面中删除指定的元组；应该更新元组以反映它不再存储在任何页面上。
     *
     * @param t 要删除的条目
     * @throws DbException 如果这个元组不在这个页面上，或者元组插槽已经是空的。
     */
    public void deleteEntry(BTreeLeafEntry t) throws DbException {
        var eid = t.getEntryId();
        if ((eid.getPid().getPageNumber() != pid.getPageNumber()) || (!eid.getPid().getTableName().equals(pid.getTableName())))
            throw new DbException("tried to delete tuple on invalid page or table");
        if (!isSlotUsed(eid.getEntryNumber(), header))
            throw new DbException("tried to delete null tuple.");
        markSlotUsed(eid.getEntryNumber(), false, header);
        t.setEntryId(null);
    }


    /**
     * 将指定的元组添加到页面，以便所有记录保持排序顺序；应该更新元组以反映它现在存储在该页面上。
     *
     * @param t 要添加的元组。
     * @throws DbException 如果页面已满（没有空槽）或 tupledesc 不匹配。
     */
    public void insertEntry(BTreeLeafEntry t) throws DbException {
        if (t.getKey().getType() != type)
            throw new DbException("type mismatch, in addTuple");

        // find the first empty slot
        int emptySlot = -1;
        for (int i = 0; i < entries.length; i++) {
            if (!isSlotUsed(i, header)) {
                emptySlot = i;
                break;
            }
        }

        if (emptySlot == -1)
            throw new DbException("called addBTreeLeafEntry on page with no empty slots.");

        // find the last key less than or equal to the key being inserted
        int lessOrEqKey = -1;
        Field key = t.getKey();
        for (int i = 0; i < entries.length; i++) {
            if (isSlotUsed(i, header)) {
                if (entries[i].getKey().compare(Predicate.Op.LESS_THAN_OR_EQ, key))
                    lessOrEqKey = i;
                else
                    break;
            }
        }


        int goodSlot = -1;
        if (emptySlot < lessOrEqKey) {
            for (int i = emptySlot; i < lessOrEqKey; i++) {
                moveEntry(i + 1, i);
            }
            goodSlot = lessOrEqKey;
        } else {
            for (int i = emptySlot; i > lessOrEqKey + 1; i--) {
                moveEntry(i - 1, i);
            }
            goodSlot = lessOrEqKey + 1;
        }

        // insert new record into the correct spot in sorted order
        markSlotUsed(goodSlot, true, header);
        EntryId id = new EntryId(pid, goodSlot);
        t.setEntryId(id);
        entries[goodSlot] = t;
    }

    /**
     * 将记录从一个槽移动到另一个槽，并更新相应的表头和 EntryId
     */
    private void moveEntry(int from, int to) {
        if (!isSlotUsed(to, header) && isSlotUsed(from, header)) {
            markSlotUsed(to, true, header);
            EntryId id = new EntryId(pid, to);
            entries[to] = entries[from];
            entries[to].setEntryId(id);
            markSlotUsed(from, false, header);
        }
    }


    class BTreeLeafPageIterator implements Iterator<BTreeLeafEntry> {
        int curBTreeLeafEntry = 0;
        BTreeLeafEntry nextToReturn = null;
        final BTreeLeafPage p = BTreeLeafPage.this;

        BTreeLeafEntry getEntry(int i) throws NoSuchElementException {

            if (i >= entries.length)
                throw new NoSuchElementException();

            try {
                if (!isSlotUsed(i, header)) {
                    return null;
                }
                return entries[i];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new NoSuchElementException();
            }
        }


        public boolean hasNext() {
            if (nextToReturn != null)
                return true;

            try {
                while (true) {
                    nextToReturn = getEntry(curBTreeLeafEntry++);
                    if (nextToReturn != null)
                        return true;
                }
            } catch (NoSuchElementException e) {
                return false;
            }
        }

        public BTreeLeafEntry next() {
            BTreeLeafEntry next = nextToReturn;

            if (next == null) {
                if (hasNext()) {
                    next = nextToReturn;
                    nextToReturn = null;
                    return next;
                } else
                    throw new NoSuchElementException();
            } else {
                nextToReturn = null;
                return next;
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }



    /**
     * 获取业内有效 Entry 的迭代器
     */
    public Iterator<BTreeLeafEntry> iterator() {
        return new BTreeLeafPageIterator();
    }

    /**
     * 调用迭代器，将他们转换为数组返回
     *
     * @see #iterator()
     */
    public BTreeLeafEntry[] getEntryAr() {
        List<BTreeLeafEntry> list = new ArrayList<>();
        var iterator = this.iterator();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list.toArray(new BTreeLeafEntry[0]);
    }
}
