package happydb.index;

import happydb.common.ByteArray;
import happydb.common.Debug;
import happydb.exception.DbException;
import happydb.execution.Predicate;
import happydb.storage.BufferPool;
import happydb.storage.Field;
import happydb.storage.PageId;
import happydb.storage.RecordId;

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
public class BTreeInternalPage extends BTreePage {

    private final byte[] header;
    private final Field[] keys;
    private final int[] children;

    public BTreeInternalPage(ByteArray byteAr, PageId pid) {
        super(byteAr.readByte(), new PageId(pid.getTableName(), byteAr.readInt()), pid);

        int numSlots = getMaxNumEntries() + 1;
        header = new byte[getHeaderSize()];
        keys = new Field[numSlots];
        children = new int[numSlots];

        for (int i = 0; i < header.length; i++) {
            header[i] = byteAr.readByte();
        }

        keys[0] = null;
        for (int i = 1; i < keys.length; i++) {
            ByteArray data = byteAr.readByteArray(type.getLen());
            if (isSlotUsed(i, header)) {
                try {
                    keys[i] = type.parse(data);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            } else {
                keys[i] = null;
            }
        }

        for (int i = 0; i < children.length; i++) {
            int pn = byteAr.readInt();
            if (isSlotUsed(i, header)) {
                children[i] = pn;
            }
        }
    }


    @Override
    public ByteArray serialized() {
        ByteArray byteAr = ByteArray.allocate(BufferPool.getPageSize());
        byteAr.writeByte(super.category).writeInt(super.parent.getPageNumber());
        for (byte b : header) {
            byteAr.writeByte(b);
        }
        for (int i = 1; i < keys.length; i++) {
            var key = keys[i];
            if (key != null) {
                byteAr.writeByteArray(key.serialized());
            } else {
                // padding
                byteAr.writeByteArray(ByteArray.allocate(type.getLen()));
            }
        }
        for (int child : children) {
            byteAr.writeInt(child);
        }
        return byteAr;
    }

    /**
     * 此方法返回一页中能存储的最大的键数，如果此方法返回 m，那么页应该有 m + 1 个指针，以及 m + 1 个槽位，第 0 个槽位只会存储指针而不会存储 key
     */
    @Override
    public int getMaxNumEntries() {
        int keySize = super.type.getLen();
        // 转换成 bit，一个 entry 可以放一个 key 和一个指针，然后加上槽位本身 1bit
        int bitsPerEntryIncludingHeader = keySize * 8 + 4 * 8 + 1;
        // 一个父指针 32bit，一个额外的子指针 32bit，一个类别字节 8bit，一个额外的槽位 1bit
        int extraBits = 2 * 4 * 8 + 8 + 1;
        // 总空间减去额外空间 / entry 大小
        return (BufferPool.getPageSize() * 8 - extraBits) / bitsPerEntryIncludingHeader;
    }


    /**
     * 计算 B+ 内部页页眉中的字节数，每个条目占用 entrySize 字节
     *
     * @return 标头中的字节数
     */
    private int getHeaderSize() {
        // 实际需要 getMaxNumEntries() + 1 个槽位
        int slotsPerPage = getMaxNumEntries() + 1, hb = (slotsPerPage / 8);
        if (hb * 8 < slotsPerPage) {
            hb++;
        }
        return hb;
    }

    /**
     * 返回页面的空槽位，从 1 开始
     */
    @Override
    public List<Integer> getEmptySlots() {
        List<Integer> ans = new ArrayList<>();
        for (int i = 1; i < keys.length; i++) {
            if (!isSlotUsed(i, header)) {
                ans.add(i);
            }
        }
        return ans;
    }

    /**
     * 从页面中删除指定条目（键+1个子指针）。 recordId 用于查找指定条目，因此不能为空。删除后，条目的 recordId 应设置为 null，以反映它不再存储在任何页面上。
     *
     * @param e                要删除的条目
     * @param deleteRightChild - 如果为真，则删除右孩子。否则删除左孩子
     * @throws DbException 如果此条目不在此页面上，或者条目槽已经为空。
     */
    public void deleteEntry(BTreeInternalEntry e, boolean deleteRightChild) throws DbException {
        EntryId eid = e.getEntryId();
        if (eid == null)
            throw new DbException("tried to delete entry with null rid");
        if ((eid.getPid().getPageNumber() != pid.getPageNumber()) || (!eid.getPid().getTableName().equals(pid.getTableName())))
            throw new DbException("tried to delete entry on invalid page or table");
        if (!isSlotUsed(eid.getEntryNumber(), header))
            throw new DbException("tried to delete null entry.");
        if (deleteRightChild) {
            markSlotUsed(eid.getEntryNumber(), false, header);
        } else {
            // 页面的左边指针可能已经被删除了
            for (int i = eid.getEntryNumber() - 1; i >= 0; i--) {
                if (isSlotUsed(i, header)) {
                    children[i] = children[eid.getEntryNumber()];
                    markSlotUsed(eid.getEntryNumber(), false, header);
                    break;
                }
            }
        }
        e.setEntryId(null);
    }


    /**
     * 在记录 ID 指定的位置更新条目的键和/或子指针。
     *
     * @param e - 具有更新的键和/或子指针的条目
     * @throws DbException 如果这个条目不在这个页面上，条目槽已经是空的，或者更新这个键会使页面上的条目乱序
     */
    public void updateEntry(BTreeInternalEntry e) throws DbException {
        var eid = e.getEntryId();
        if (eid == null)
            throw new DbException("tried to update entry with null rid");
        if ((eid.getPid().getPageNumber() != pid.getPageNumber()) || (!eid.getPid().getTableName().equals(pid.getTableName())))
            throw new DbException("tried to update entry on invalid page or table");
        if (!isSlotUsed(eid.getEntryNumber(), header))
            throw new DbException("tried to update null entry.");

        for (int i = eid.getEntryNumber() + 1; i < keys.length; i++) {
            if (isSlotUsed(i, header)) {
                if (keys[i].compare(Predicate.Op.LESS_THAN, e.getKey())) {
                    throw new DbException("attempt to update entry with invalid key " + e.getKey() +
                            " HINT: updated key must be less than or equal to keys on the right");
                }
                break;
            }
        }
        for (int i = eid.getEntryNumber() - 1; i >= 0; i--) {
            if (isSlotUsed(i, header)) {
                if (i > 0 && keys[i].compare(Predicate.Op.GREATER_THAN, e.getKey())) {
                    throw new DbException("attempt to update entry with invalid key " + e.getKey() +
                            " HINT: updated key must be greater than or equal to keys on the left");
                }
                children[i] = e.getLeftChild().getPageNumber();
                break;
            }
        }
        children[eid.getEntryNumber()] = e.getRightChild().getPageNumber();
        keys[eid.getEntryNumber()] = e.getKey();
    }


    /**
     * 将指定条目添加到页面；条目的 recordId 应该更新以反映它现在存储在这个页面上。
     *
     * @param e 要添加的条目。
     * @throws DbException 如果页面已满（没有空槽）或关键字段类型、表 ID 或子页面类别不匹配，或者条目无效
     */
    public void insertEntry(BTreeInternalEntry e) throws DbException {
        if (!e.getKey().getType().equals(type))
            throw new DbException("key field type mismatch, in insertEntry");

        if (!e.getLeftChild().getTableName().equals(pid.getTableName()) || !e.getRightChild().getTableName().equals(pid.getTableName()))
            throw new DbException("table id mismatch in insertEntry");

        // if this is the first entry, add it and return
        if (getEmptySlots().size() == getMaxNumEntries()) {
            children[0] = e.getLeftChild().getPageNumber();
            children[1] = e.getRightChild().getPageNumber();
            keys[1] = e.getKey();
            markSlotUsed(0, true, header);
            markSlotUsed(1, true, header);
            e.setEntryId(new EntryId(pid, 1));
            return;
        }

        // find the first empty slot, starting from 1
        int emptySlot = -1;
        for (int i = 1; i < keys.length; i++) {
            if (!isSlotUsed(i, header)) {
                emptySlot = i;
                break;
            }
        }

        if (emptySlot == -1)
            throw new DbException("called insertEntry on page with no empty slots.");

        // find the child pointer matching the left or right child in this entry
        int lessOrEqKey = -1;
        for (int i = 0; i < keys.length; i++) {
            if (isSlotUsed(i, header)) {
                if (children[i] == e.getLeftChild().getPageNumber() || children[i] == e.getRightChild().getPageNumber()) {
                    if (i > 0 && keys[i].compare(Predicate.Op.GREATER_THAN, e.getKey())) {
                        throw new DbException("attempt to insert invalid entry with left child " +
                                e.getLeftChild().getPageNumber() + ", right child " +
                                e.getRightChild().getPageNumber() + " and key " + e.getKey() +
                                " HINT: one of these children must match an existing child on the page" +
                                " and this key must be correctly ordered in between that child's" +
                                " left and right keys");
                    }
                    lessOrEqKey = i;
                    if (children[i] == e.getRightChild().getPageNumber()) {
                        children[i] = e.getLeftChild().getPageNumber();
                    }
                } else if (lessOrEqKey != -1) {
                    // 验证下一个键是否大于或等于我们正在插入的键
                    if (keys[i].compare(Predicate.Op.LESS_THAN, e.getKey())) {
                        throw new DbException("attempt to insert invalid entry with left child " +
                                e.getLeftChild().getPageNumber() + ", right child " +
                                e.getRightChild().getPageNumber() + " and key " + e.getKey() +
                                " HINT: one of these children must match an existing child on the page" +
                                " and this key must be correctly ordered in between that child's" +
                                " left and right keys");
                    }
                    break;
                }
            }
        }

        if (lessOrEqKey == -1) {
            throw new DbException("attempt to insert invalid entry with left child " +
                    e.getLeftChild().getPageNumber() + ", right child " +
                    e.getRightChild().getPageNumber() + " and key " + e.getKey() +
                    " HINT: one of these children must match an existing child on the page" +
                    " and this key must be correctly ordered in between that child's" +
                    " left and right keys");
        }

        // 向后或向前移动条目以填充空槽并为新条目腾出空间，同时保持条目按排序顺序
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

        // insert new entry into the correct spot in sorted order
        markSlotUsed(goodSlot, true, header);
        keys[goodSlot] = e.getKey();
        children[goodSlot] = e.getRightChild().getPageNumber();
        e.setEntryId(new EntryId(pid, goodSlot));
    }

    /**
     * 将条目从一个插槽移动到另一个插槽，并更新相应的标头
     */
    private void moveEntry(int from, int to) {
        if (!isSlotUsed(to, header) && isSlotUsed(from, header)) {
            markSlotUsed(to, true, header);
            keys[to] = keys[from];
            children[to] = children[from];
            markSlotUsed(from, false, header);
        }
    }



    /**
     * 从页面中删除指定条目（键+右子指针）。 recordId 用于查找指定条目，因此不能为空。删除后，条目的 recordId 应设置为 null，以反映它不再存储在任何页面上。
     *
     * @param e 要删除的条目
     * @throws DbException 如果此条目不在此页面上，或者条目槽已经为空。
     */
    public void deleteKeyAndRightChild(BTreeInternalEntry e) throws DbException {
        deleteEntry(e, true);
    }

    /**
     * 从页面中删除指定条目（键+左子指针）。 recordId 用于查找指定条目，因此不能为空。删除后，条目的 recordId 应设置为 null，以反映它不再存储在任何页面上。
     *
     * @param e 要删除的条目
     * @throws DbException 如果此条目不在此页面上，或者条目槽已经为空。
     */
    public void deleteKeyAndLeftChild(BTreeInternalEntry e) throws DbException {
        deleteEntry(e, false);
    }


    /**
     * 迭代器，用以迭代页内 Entry
     */
    private class BTreeInternalPageIterator implements Iterator<BTreeInternalEntry> {
        int curEntry = 1;
        PageId prevChildId = null;
        BTreeInternalEntry nextToReturn = null;

        protected PageId getChildId(int i) throws NoSuchElementException {
            if (i < 0 || i >= children.length)
                throw new NoSuchElementException();

            try {
                if (!isSlotUsed(i, header)) {
                    return null;
                }
                return new PageId(pid.getTableName(), children[i]);
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new NoSuchElementException();
            }
        }

        protected Field getKey(int i) throws NoSuchElementException {
            if (i <= 0 || i >= keys.length)
                throw new NoSuchElementException();

            try {
                if (!isSlotUsed(i, header)) {
                    return null;
                }
                return keys[i];

            } catch (ArrayIndexOutOfBoundsException e) {
                throw new NoSuchElementException();
            }
        }

        public boolean hasNext() {
            if (nextToReturn != null)
                return true;

            try {
                if (prevChildId == null) {
                    prevChildId = getChildId(0);
                    if (prevChildId == null) {
                        return false;
                    }
                }
                while (true) {
                    int entry = curEntry++;
                    Field key = getKey(entry);
                    PageId childId = getChildId(entry);
                    if (key != null && childId != null) {
                        nextToReturn = new BTreeInternalEntry(key, prevChildId, childId);
                        nextToReturn.setEntryId(new EntryId(pid, entry));
                        prevChildId = childId;
                        return true;
                    }
                }
            } catch (NoSuchElementException e) {
                return false;
            }
        }

        public BTreeInternalEntry next() {
            BTreeInternalEntry next = nextToReturn;

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
    public Iterator<BTreeInternalEntry> iterator() {
        return new BTreeInternalPageIterator();
    }

    /**
     * 调用迭代器，将他们转换为数组返回
     *
     * @see #iterator()
     */
    public BTreeInternalEntry[] getEntryAr() {
        List<BTreeInternalEntry> list = new ArrayList<>();
        var iterator = this.iterator();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list.toArray(new BTreeInternalEntry[0]);
    }
}