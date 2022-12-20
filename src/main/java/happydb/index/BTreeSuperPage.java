package happydb.index;

import happydb.common.ByteArray;
import happydb.common.Database;
import happydb.exception.DbException;
import happydb.storage.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author happysnaker
 * @Date 2022/11/21
 * @Email happysnaker@foxmail.com
 */
public class BTreeSuperPage extends AbstractPage {
    @Getter
    @Setter
    private PageId btreeRootPageId;
    @Getter
    private final Type type;
    private final byte[] header;


    private static class EmptyBTreePage extends AbstractPage {

        byte category;

        public EmptyBTreePage(PageId pid, byte category) {
            setPid(pid);
            this.category = category;
        }

        @Override
        public ByteArray serialized() {
            return ByteArray.allocate(BufferPool.getPageSize()).writeByte(category);
        }

        @Override
        public int getMaxNumEntries() {
            return 0;
        }

        @Override
        public List<Integer> getEmptySlots() {
            return null;
        }
    }

    /**
     * 从磁盘中读取特定字节流初始化超级页
     *
     * @param byteAr 字节流
     */
    public BTreeSuperPage(ByteArray byteAr, PageId pid) {
        assert pid.getPageNumber() == 0;
        super.pid = pid;
        this.header = new byte[getHeaderSize()];

        switch (byteAr.readByte()) {
            case 0 -> {
                this.type = Type.INT_TYPE;
            }
            case 1 -> {
                this.type = Type.DOUBLE_TYPE;
            }
            case 2 -> {
                this.type = Type.STRING_TYPE;
            }
            default -> {
                throw new RuntimeException("无法解析的类型");
            }
        }
        this.btreeRootPageId = new PageId(pid.getTableName(), byteAr.readInt());
        for (int i = 0; i < header.length; i++) {
            header[i] = byteAr.readByte();
        }
        markSlotUsed(0, true, header);
    }

    /**
     * 创建一个空的超级页
     *
     * @param type 索引类型
     * @return 返回字节数组
     */
    public static ByteArray createEmptyPageData(Type type) {
        ByteArray byteArray = ByteArray.allocate(BufferPool.getPageSize());
        switch (type) {
            case INT_TYPE -> byteArray.writeByte((byte) 0);
            case DOUBLE_TYPE -> byteArray.writeByte((byte) 1);
            case STRING_TYPE -> byteArray.writeByte((byte) 2);
        }
        return byteArray;
    }


    @Override
    public ByteArray serialized() {
        ByteArray byteArray = ByteArray.allocate(BufferPool.getPageSize());

        switch (type) {
            case INT_TYPE -> byteArray.writeByte((byte) 0);
            case DOUBLE_TYPE -> byteArray.writeByte((byte) 1);
            case STRING_TYPE -> byteArray.writeByte((byte) 2);
        }

        byteArray.writeInt(getBtreeRootPageId().getPageNumber());

        for (byte b : header) {
            byteArray.writeByte(b);
        }
        return byteArray;
    }

    /**
     * 此方法返回页面插槽数目
     *
     * @return 页面插槽数目
     */
    @Override
    public int getMaxNumEntries() {
        return (BufferPool.getPageSize() - 4 - 1) * 8;
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
        for (int i = 0; i < getMaxNumEntries(); i++) {
            if (!isSlotUsed(i, header)) {
                ans.add(i);
            }
        }
        return ans;
    }


    /**
     * 获取一个空页，并且原子的将其标记为已使用，其他线程无法继续获取此空页，除非页被显示的释放
     * <p>此方法会自动调用 {@link happydb.storage.PageManager} 写入一个空页，<strong>以及，将自身槽位的变化写入磁盘</strong>，因此
     * <strong>此方法在调用时无需获取页面上的锁，也无需标记脏页</strong></p>
     *
     * @param category 空页的类别
     * @return 返回新创建的空页 ID，应该通过 {@link BTreePageHolder} 来实际获取页面
     */
    public synchronized PageId malloc(byte category) throws IOException {
        for (int i = 0; i < getMaxNumEntries(); i++) {
            if (!isSlotUsed(i, header)) {
                markSlotUsed(i, true, header);
                PageManager pm = Database.getCatalog().getPageManager(pid.getTableName());
                // 向磁盘中写入空页，仅设置页的类别
                PageId pageId = new PageId(pid.getTableName(), i);
                pm.writePage(new EmptyBTreePage(pageId, (byte) category));
                pm.writePage(this);
                return pageId;
            }
        }
        throw new RuntimeException("空间已耗尽");
    }


    /**
     * 回收一个页面, <strong>语义与 malloc 相反</strong>
     *
     * @param pid 待回收的页面
     */
    public synchronized void free(PageId pid) throws DbException {
        if (!pid.getTableName().equals(super.pid.getTableName())) {
            throw new DbException("模式不匹配");
        }
        markSlotUsed(pid.getPageNumber(), false, header);
        try {
            Database.getCatalog().getPageManager(pid.getTableName()).writePage(this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
