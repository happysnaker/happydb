package happydb.execution;

import happydb.common.Database;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.index.Index;
import happydb.index.IndexType;
import happydb.log.UndoLog;
import happydb.log.UndoLogId;
import happydb.log.UndoLogPage;
import happydb.parser.InsertParser;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.ReadView;
import happydb.transaction.TransactionId;
import lombok.Getter;
import net.sf.jsqlparser.statement.insert.Insert;

import java.util.Iterator;
import java.util.List;

/**
 * 一切查询运算符的源头之一，BTreeSeqScan 利用 B+ 树索引，选择是否全表扫描还是基于条件的查询，返回元组迭代流，
 * BTreeSeqScan 必须要过滤被逻辑删除的元组以及那些事务看不见的元组（由事务隔离机制决定），并通过版本引用获取对本事务可见的记录。
 * <P>此运算符仅用于查询，此类不具备任何获取锁的逻辑，可以看看 {@link UpdateOperateScan}</P>
 *
 * @Author happysnaker
 * @Date 2022/11/23
 * @Email happysnaker@foxmail.com
 * @see UpdateOperateScan
 * @see happydb.execution.UpdateOperateScan.BTreeSeqScanDisableMvcc
 */
public class BTreeSeqScan extends AbstractOpIterator {
    @Getter
    protected final TransactionId tid;
    @Getter
    protected final String tableName;
    @Getter
    protected final String tableAlias;
    @Getter
    protected final Predicate predicate;

    /**
     * 本次查询使用的读视图
     */
    protected volatile ReadView readView;

    /**
     * 构造 B+ 树查询运算符，支持全表扫描或者基于某谓词扫描
     *
     * @param tid        操作的事务 ID
     * @param tableName  表的本名
     * @param tableAlias 表的别名，如果不存在，他应该等于 tableName
     * @param predicate  谓词，为空则全表扫描，若不为空，情保证此字段上建立了 B+ 树索引
     */
    public BTreeSeqScan(TransactionId tid, String tableName, String tableAlias, Predicate predicate) {
        this.tid = tid;
        this.tableName = tableName;
        this.tableAlias = tableAlias == null ? tableName : tableAlias;
        this.predicate = predicate;
    }


    protected List<RecordId> recordIds;
    protected Iterator<RecordId> iterator;

    @Override
    protected void closeOpIterator() throws DbException {
        iterator = null;
        recordIds = null;
    }

    @Override
    protected void openOpIterator() throws DbException {
        if (predicate != null) {
            Index index = Database.getCatalog().getIndex(tableName, predicate.getField(), IndexType.BTREE);
            recordIds = index.search(tid, predicate.getOp(), predicate.getOperand());
        } else {
            TableDesc tableDesc = Database.getCatalog().getTableDesc(tableName);
            Index index = Database.getCatalog().getIndex(tableName, tableDesc.getPrimaryKeyFieldIndex(), IndexType.BTREE);
            recordIds = index.search(tid, null, null);
        }
        this.iterator = recordIds.iterator();
        this.readView = ReadView.createReadView(tid, Database.ISOLATION_LEVEL);
    }

    @Override
    public void rewind() throws DbException {
        this.iterator = recordIds.iterator();
    }

    @Override
    protected Record fetchNext() throws DbException {
        Record record = null;
        while (iterator.hasNext()) {
            RecordId next = iterator.next();
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, next.getPid(), Permissions.READ_ONLY);

            record = page.readRecord(next);
            if ((record = isVisible(record)) != null && record.isValid()) {
                return record;
            }
        }
        return null;
    }

    /**
     * 判断记录是否可见，如果不可见，则递归获取上一个可见的版本
     *
     * @param record 参数
     */
    protected Record isVisible(Record record) throws DbException {
        if (record == null) {
            return null;
        }
        boolean visible = readView.isVisible(record);
        if (visible) {
            return record;
        } else {
            UndoLogId uid = record.getLogPointer();
            UndoLogPage page = (UndoLogPage) Database.getBufferPool().getPage(tid, uid.pid(), Permissions.READ_ONLY);


            try {
                UndoLog undoLog = page.readUndoLog(uid);
                return isVisible(undoLog.getLastVersion(tid));
            } catch (DbException | NullPointerException e) {
                // FixMissingRecordScan 不会设置 undo log，因此日志可能不存在
                return null;
            }
        }
    }

    /**
     * 返回一个新的表模式，其中字段名为 tableAlias.fieldName，在查询中，我们总是使用 a.b 来表述一个字段
     * <P>最终会通过一个额外的投影运算符将这些字段名转为用户所需的字段名</P>
     * <P>此方法仅会修改原纪录模式的字段名，表名、字段类型和索引类型将原封不动</P>
     */
    @Override
    public TableDesc getTableDesc() {
        var td = Database.getCatalog().getTableDesc(this.tableName);
        Type[] typeAr = new Type[td.numFields()];
        String[] fieldAr = new String[td.numFields()];
        int[] indexAr = new int[td.numFields()];
        String alias = this.tableAlias == null ? getTableName() : this.tableAlias;
        for (int i = 0; i < td.numFields(); i++) {
            typeAr[i] = td.getFieldType(i);
            indexAr[i] = td.getIndexType(i);
            fieldAr[i] = String.format("%s.%s", alias, td.getFieldName(i));
        }
        return new TableDesc(td.getTableName(), fieldAr, typeAr, indexAr);
    }
}
