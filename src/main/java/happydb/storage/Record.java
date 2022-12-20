package happydb.storage;

import happydb.common.ByteArray;
import happydb.common.DbSerializable;
import happydb.log.UndoLogId;
import happydb.transaction.TransactionId;
import lombok.Getter;
import lombok.Setter;

import java.text.ParseException;
import java.util.Arrays;

/**
 * 行记录，包含用户记录和隐藏字段，若此记录是一条投影记录，则隐藏字段与记录 ID 无意义
 *
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
public class Record implements DbSerializable, Cloneable {
    /**
     * 元组存储时隐藏字段大小
     */
    public static final int HIDDEN_SIZE = 1 + 8 + 8;
    /**
     * 与此记录关联的模式
     */
    @Getter
    @Setter
    private TableDesc tableDesc;
    /**
     * 记录的 ID
     */
    @Getter
    @Setter
    private RecordId recordId;
    /**
     * 记录的隐藏字段，标识记录是否有效
     */
    @Getter
    @Setter
    private boolean valid;
    /**
     * 指向回滚日志的指针，可用于获取上一个版本，如果没有，则为 {@link UndoLogId#NULL_ID}
     */
    @Getter
    @Setter
    private UndoLogId logPointer;
    /**
     * 上一次修改此记录的事务 ID，如果没有，则为 -1，负编号的事务对所有事务可见
     */
    @Getter
    @Setter
    private TransactionId lastModify;

    /**
     * 元组的值，与 TupleDesc 对应
     */
    @Getter
    @Setter
    private Field[] fieldAr;

    public Record(TableDesc td) {
        this.tableDesc = td;
        this.fieldAr = new Field[td.numFields()];

        this.logPointer = UndoLogId.NULL_ID;
        this.lastModify = new TransactionId(-1);
    }

    private long undoLogIdToLong(UndoLogId id) {
        long x = 0;
        x |= id.pid().getPageNumber();
        x <<= 32;
        x |= id.undoLogNumber();
        return x;
    }

    private UndoLogId longToUndoLogId(long x) {
        if (x == undoLogIdToLong(UndoLogId.NULL_ID)) {
            return UndoLogId.NULL_ID;
        }
        long mask = 0x00000000FFFFFFFF;
        int un = (int) (x & mask);
        int pn = (int) ((x >>> 32) & mask);
        return new UndoLogId(
                new PageId(this.getTableDesc().getTableName() + UndoLogId.UNDO_LOG_TABLE_NAME_SUFFIX, pn),
                un);
    }

    @Override
    public ByteArray serialized() {
        ByteArray byteAr = ByteArray.allocate(tableDesc.getRecordSize() + HIDDEN_SIZE);
        for (Field field : this.fieldAr) {
            byteAr.writeByteArray(field.serialized());
        }
        byteAr.writeByte((byte) (valid ? 1 : 0));
        byteAr.writeLong(undoLogIdToLong(logPointer));
        byteAr.writeLong(lastModify.getXid());
        return byteAr;
    }

    /**
     * 从字节数组中反序列化填充字段值，此方法将顺序推进读取点
     * @param byteAr 包含字段值的字节数组
     * @throws ParseException 解析异常
     */
    public void deserialize(ByteArray byteAr) throws ParseException {
        for (int i = 0; i < tableDesc.numFields(); i++) {
            Field field = tableDesc.getFieldType(i).parse(byteAr);
            this.setField(i, field);
        }
        setValid(byteAr.readByte() == 1);
        setLogPointer(longToUndoLogId(byteAr.readLong()));
        setLastModify(new TransactionId(byteAr.readLong()));
    }


    /**
     * 更改此元组的第 i 个字段的值。
     *
     * @param i 要更改的字段的索引。它必须是一个有效的索引。
     * @param f 新的字段值
     */
    public void setField(int i, Field f) {
        if (tableDesc.getFieldType(i) != f.getType()) {
            throw new IllegalArgumentException("类型不匹配");
        }
        this.fieldAr[i] = f;
    }

    /**
     * @param i 要返回的字段索引。必须是有效索引。
     * @return 第 i 个字段的值，如果尚未设置，则为 null。
     */
    public Field getField(int i) {
        return this.fieldAr[i];
    }

    /**
     * 获取此元组的字段数，不包含隐藏字段
     * @return 此元组的字段数
     */
    public int getNumFields() {
        return fieldAr.length;
    }

    /**
     * 注意，这里没有使用 RecordId 作为标识，因为记录可能作为逻辑记录，不一定包含 ID
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Record record)) return false;

        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(getFieldAr(), record.getFieldAr());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getFieldAr());
    }

    @Override
    public String toString() {
        return "Record{" +
                "tableDesc=" + tableDesc +
                ", fieldAr=" + Arrays.toString(fieldAr) +
                '}';
    }

    @Override
    public Record clone() {
        try {
            Record clone = (Record) super.clone();
            clone.setRecordId(recordId);
            clone.setValid(valid);
            clone.setTableDesc(tableDesc);
            clone.setLogPointer(new UndoLogId(logPointer.pid(), logPointer.undoLogNumber()));
            clone.setLastModify(new TransactionId(lastModify.getXid()));
            clone.setFieldAr(fieldAr.clone());
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }
}
