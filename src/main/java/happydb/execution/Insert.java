package happydb.execution;

import happydb.common.Database;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.index.Index;
import happydb.index.IndexType;
import happydb.log.UndoLog;
import happydb.optimizer.TableStateView;
import happydb.storage.*;
import happydb.storage.Record;
import happydb.transaction.ReadView;
import happydb.transaction.TransactionId;
import lombok.NonNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Insert 接受一些元组，并准备将他们插入到页中，更确切的说，Insert 将做如下事情：
 * <ol>
 *     <li>完备这些元组，这包括设置有效位为真，设置最后修改的事务 ID 为当前事务</li>
 *     <li>
 *         获取表锁，判断待插入唯一索引键是否重复，如果重复则抛出异常，请注意，<strong>已经插入的记录不会取消</strong>，
 *         由于存在被逻辑删除的记录以及其他事务并发，具体判断规则如下：
 *          <ul>
 *              <li>使用索引搜索等值条件的记录 ID 列表，如果存在任意一个行记录被其他事务锁定，则认为已有记录，抛出异常</li>
 *              <li>否则，遍历行记录，如果存在记录，其事务 ID 对本事务不可见(使用当前读)，则认为已有记录，抛出异常</li>
 *              <li>否则，如果存在记录有效位为真，则认为已有记录，抛出异常</li>
 *              <li>否则，可以安全的插入</li>
 *          </ul>
 *     </li>
 *     <li>向 {@link happydb.storage.HeapPageManager} 申请一个 {@link happydb.storage.RecordId}，对行记录 ID 上锁，锁绝不会堵塞</li>
 *     <li>产生 {@link happydb.log.UndoLog}，并设置记录的上一个版本</li>
 *     <li>向页面插入元组</li>
 *     <li>调用 {@link happydb.log.LogBuffer} 创建 {@link happydb.log.InsertRedoLog}，将页面弄脏并放入 flushLst 中</li>
 *     <li>为记录每一个包含索引的字段建立索引</li>
 *     <li>动态维护直方图，直方图允许一定的误差，不管是否回滚，都执行插入</li>
 *     <li>释放表锁</li>
 *     <li><strong>Insert 将在 {@link #open()} 时完成一切操作</strong>，在 {@link #next()} 时，仅返回一行记录影响的行数，多次调用将返回 null </li>
 * </ol>
 *
 * @Author happysnaker
 * @Date 2022/12/4
 * @Email happysnaker@foxmail.com
 */
public class Insert extends AbstractOpIterator {

    final static Map<String, ReentrantLock> tableLock = new ConcurrentHashMap<>();

    /**
     * 获取锁，由于一次操作将释放锁，因此这里是基于线程的而不是基于事务的锁，一次操作总是单线程的
     *
     * @param tableName
     * @return
     */
    protected static ReentrantLock getLock(String tableName) {
        if (!tableLock.containsKey(tableName)) {
            synchronized (tableLock) {
                tableLock.putIfAbsent(tableName, new ReentrantLock());
            }
        }
        return tableLock.get(tableName);
    }

    OpIterator child;

    TransactionId tid;

    int rowsAffected;

    ReadView readView;

    public Insert(@NonNull OpIterator child, @NonNull TransactionId tid) {
        this.child = child;
        this.tid = tid;
        this.readView = ReadView.createReadView(tid, ReadView.READ_COMMIT);
    }

    private void assertUnique(TableDesc td, Record record) throws DbException, IOException {
        for (int i = 0; i < td.numFields(); i++) {
            Set<IndexType> set = IndexType.intToIndexSet(td.getIndexType(i));
            for (IndexType indexType : set) {
                if (indexType != IndexType.BTREE && indexType != IndexType.HASH) {
                    continue;
                }
                Index index = indexType == IndexType.BTREE ? Database.getCatalog().getIndex(td.getTableName(), i, IndexType.BTREE)
                        : Database.getCatalog().getIndex(td.getTableName(), i, IndexType.HASH);

                boolean unique = (indexType == IndexType.BTREE && set.contains(IndexType.BTREE_UNIQUE)) ||
                        (indexType == IndexType.HASH && set.contains(IndexType.HASH_UNIQUE));
                if (unique) {
                    List<RecordId> search = index.search(tid, Predicate.Op.EQUALS, record.getField(i));

                    boolean hasRecord = false;
                    for (RecordId recordId : search) {
                        TransactionId holdLock = Database.getLockTable().holdLock(recordId);
                        if (holdLock != null && !holdLock.equals(tid)) {
                            hasRecord = true;
                            break;
                        }

                        Record r = ((HeapPage) Database.getBufferPool()
                                .getPage(tid, recordId.getPid(), Permissions.READ_ONLY)).readRecord(recordId);
                        if (!readView.isVisible(r)) {
                            hasRecord = true;
                            break;
                        }

                        if (r.isValid()) {
                            hasRecord = true;
                            break;
                        }
                    }

                    if (hasRecord) {
                        throw new DbException("Duplicated insert val " + record.getField(i));
                    }
                }
            }
        }
    }

    private void doCreateIndex(TableDesc td, Record record) throws DbException, IOException {
        for (int i = 0; i < td.numFields(); i++) {
            Set<IndexType> set = IndexType.intToIndexSet(td.getIndexType(i));
            for (IndexType indexType : set) {
                if (indexType != IndexType.BTREE && indexType != IndexType.HASH) {
                    continue;
                }
                Index index = indexType == IndexType.BTREE ? Database.getCatalog().getIndex(td.getTableName(), i, IndexType.BTREE)
                        : Database.getCatalog().getIndex(td.getTableName(), i, IndexType.HASH);

                index.insert(tid, record.getField(i), record.getRecordId());
            }
        }
    }

    private void doInsert(Record record) throws IOException, DbException {
        // step1
        record.setValid(true);
        record.setLastModify(tid);

        // step2
        getLock(child.getTableDesc().getTableName()).lock();
        try {
            assertUnique(child.getTableDesc(), record);

            // step3
            HeapPageManager pm = (HeapPageManager) Database.getCatalog().getPageManager(child.getTableDesc().getTableName());
            RecordId malloc = pm.malloc();
            record.setRecordId(malloc);
            Database.getLockTable().lock(tid, malloc);

            // step4
            UndoLog undoLog = Database.getLogBuffer().createInsertUndoLog(tid, record);
            record.setLogPointer(undoLog.getId());

            // step5
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, malloc.getPid(), Permissions.READ_ONLY);
            page.insertRecord(malloc, record);

            // step6
            assert record.getRecordId() != null;
            Database.getLogBuffer().createInsertRedoLog(tid, record);

            // step7
            doCreateIndex(child.getTableDesc(), record);

            // step8
            TableStateView.getInstance().insertRecord(child.getTableDesc().getTableName(), record);
        } finally {
            // step9
            getLock(child.getTableDesc().getTableName()).unlock();
        }
    }


    @Override
    protected void openOpIterator() throws DbException {
        child.open();

        for (Record record : child.getRecordAr()) {
            try {
                doInsert(record);
            } catch (DbException | IOException e) {
                throw new DbException(e);
            }
            rowsAffected++;
        }
        assert rowsAffected > 0; // 不允许插入空行
    }

    @Override
    protected void closeOpIterator() throws DbException {
        child.close();
    }

    @Override
    protected Record fetchNext() throws DbException {
        if (rowsAffected < 0) {
            return null;
        }
        Record record = new Record(getTableDesc());
        record.setField(0, new IntField(rowsAffected));
        rowsAffected = -rowsAffected;
        return record;
    }

    @Override
    public void rewind() throws DbException {
        child.rewind();
        rowsAffected = Math.abs(rowsAffected);
    }

    @Override
    public TableDesc getTableDesc() {
        return new TableDesc(new String[]{"rowsAffected"}, new Type[]{Type.INT_TYPE});
    }
}
