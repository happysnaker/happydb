package happydb.execution;

import happydb.common.Database;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.index.IndexType;
import happydb.optimizer.LogicalFilterNode;
import happydb.optimizer.LogicalPlan;
import happydb.optimizer.TableState;
import happydb.optimizer.TableStateView;
import happydb.storage.HeapPage;
import happydb.storage.Record;
import happydb.storage.RecordId;
import happydb.storage.TableDesc;
import happydb.transaction.ReadView;
import happydb.transaction.TransactionId;

import java.util.*;

import static happydb.optimizer.LogicalPlan.sortFilters;

/**
 * 此类针对更新操作（Update 和 Delete）进行记录扫描
 * <P>此类接受更新语句的 WHERE 过滤器，以最佳方式获取行记录，此类将对这些行记录加锁(意味着可能会堵塞)，然后作为运算符输出这些行记录</P>
 * <P>此类与 {@link BTreeSeqScan} 和 {@link LogicalPlan#decideBestSubPlan(Map, Map, String, List, TransactionId)}
 * 具备类似的逻辑，可以复用大多数逻辑，但是有一些细微的差别</P>
 * <P>此类绝不会使用 MVCC，当一个事务正在对一个行记录进行更新操作时，
 * 此类不应该对其上一个版本操作，而是应该堵塞在行锁上，等待其他事务操作完毕</P>
 * <p>此类将过滤那些可见的，并且已经被删除的元组。此类将使用 {@link ReadView#READ_COMMIT} 判断记录是否真正删除</p>
 * <P>由于堵塞，当其他事务提交后，本事务等待的行记录可能不再符合过滤条件，因此获取锁后，此类将过滤那些不再符合条件的记录，并释放锁</P>
 * <P>当调用 {@link #open()} 时，此类会开始获取锁的逻辑，一旦检测到死锁，则会抛出 {@link happydb.exception.DeadLockException}</P>
 * <P>此类会过滤掉任何被逻辑删除的记录，一旦过滤操作发生，此类将立即释放记录上的锁</P>
 * <P>过此类产生的记录流指向页面中的记录，对迭代产生的记录修改将会反应到对于页面中</P>
 *
 * @Author happysnaker
 * @Date 2022/11/23
 * @Email happysnaker@foxmail.com
 */
public class UpdateOperateScan extends AbstractOpIterator {

    TransactionId tid;

    LogicalPlan lp;

    TableDesc td;


    List<Record> records = new ArrayList<>();

    Iterator<Record> iterator;

    public UpdateOperateScan(TransactionId tid, LogicalPlan lp) {
        this.tid = tid;
        this.lp = lp;

        if (lp.getTableMap() == null || lp.getTableMap().size() != 1) {
            throw new IllegalArgumentException("Update statement only support one table.");
        }
        this.td = Database.getCatalog().getTableDesc(lp.getTableMap().values().iterator().next());
    }

    @Override
    protected Record fetchNext() throws DbException {
        return iterator.hasNext() ? iterator.next() : null;
    }

    @Override
    protected void closeOpIterator() throws DbException {
        tid = null;
        lp = null;
    }

    @Override
    protected void openOpIterator() throws DbException {
        List<LogicalFilterNode> filters = lp.getFilters();
        OpIterator it = decideBestSubPlan(lp.getTableMap().keySet().iterator().next(), filters, tid);

        it.open();
        while (it.hasNext()) {
            Record next = it.next();
            boolean prevHoldLock = Database.getLockTable().holdLock(tid, next.getRecordId());
            Database.getLockTable().lock(tid, next.getRecordId());
            // 加锁之后重新读一遍，双重验证，因为记录可能被修改额了
            next = ((HeapPage) Database.getBufferPool()
                    .getPage(tid, next.getRecordId().getPid(), Permissions.READ_ONLY)).readRecord(next.getRecordId());
            if (next.isValid() && filter(next, filters)) {
                records.add(next);
            } else {
                // 必须要小心，如果事务之前就持有锁，那么不能释放它们！！！
                if (!prevHoldLock) {
                    Database.getLockTable().unsafeUnLock(tid, next.getRecordId());
                }
            }
        }

        this.iterator = records.iterator();
    }

    /**
     * 判断记录是否符合过滤条件
     * @param record 记录
     * @param filters 过滤条件
     * @return 真则符合
     */
    private boolean filter(Record record, List<LogicalFilterNode> filters) {
        if (filters == null) {
            return true;
        }
        boolean ans = true;
        for (LogicalFilterNode filter : filters) {
            int i = td.fieldNameToIndex(filter.getFieldPureName());
            Predicate predicate = new Predicate(i, filter.getOp(), filter.parseConstant(td));
            boolean valid = predicate.filter(record);

            if (filter.isAnd() && !valid) {
                ans = false;
            }

            // or 条件只要满足一个直接返回
            if (!filter.isAnd() && valid) {
                return true;
            }
        }
        return ans;
    }

    /**
     * 根据 Predicate 给出最优的执行计划
     * @return
     */
    public OpIterator getBestFilterOpIterator(
            LogicalPlan lp, Set<IndexType> indexTypes, Predicate predicate, TransactionId tid, String tableAlias) {
        if (predicate.getOp() == Predicate.Op.EQUALS && indexTypes.contains(IndexType.HASH)) {
            // using hash
            throw new RuntimeException("Hash index haven`t implement");
        } else if (indexTypes.contains(IndexType.BTREE)) {
            return new BTreeSeqScanDisableMvcc(tid, lp.getTableName(tableAlias), tableAlias, predicate);
        } else {
            return new Filter(predicate, new BTreeSeqScanDisableMvcc(tid, lp.getTableName(tableAlias), tableAlias, null));
        }
    }


    /**
     * 根据给定的过滤条件，决定出最佳的顺序将其组合.
     * <p>
     * 所谓的最佳顺序指：
     *     <ul>
     *         <li>
     *             <strong>对于 AND，有索引的优先考虑，如果都具备索引则按照基数从小到大执行</strong>
     *         </li>
     *         <li>
     *             对于 OR，将按照顺序进行匹配，有索引则走索引
     *         </li>
     *     </ul>
     *
     * </P>
     *  @param tableAlias 表别名
     *
     * @param filters 关于此表的过滤节点
     */
    public OpIterator decideBestSubPlan(String tableAlias, List<LogicalFilterNode> filters, TransactionId tid) throws DbException {
        if (filters == null || filters.isEmpty()) {
            return new BTreeSeqScanDisableMvcc(tid, lp.getTableName(tableAlias), tableAlias, null);
        }
        OpIterator iterator = null;
        TableState state = TableStateView.getInstance().getTableState(lp.getTableName(tableAlias));
        sortFilters(filters, state, td);

        for (LogicalFilterNode filter : filters) {
            int field = td.fieldNameToIndex(filter.getFieldPureName());
            Predicate predicate = new Predicate(field, filter.getOp(), filter.parseConstant(td));
            Set<IndexType> indexTypes = IndexType.intToIndexSet(td.getIndexType(field));

            if (iterator == null) {
                iterator = getBestFilterOpIterator(lp, indexTypes, predicate, tid, tableAlias);
            } else {
                if (filter.isAnd()) {
                    iterator = new Filter(predicate, iterator);
                } else {
                    iterator = new UnionOnOr(iterator, getBestFilterOpIterator(lp, indexTypes, predicate, tid, tableAlias));
                }
            }
        }
        return iterator;
    }


    @Override
    public void rewind() throws DbException {
        iterator = records.iterator();
    }

    @Override
    public TableDesc getTableDesc() {
        return td;
    }

    /**
     * 一个辅助类，继承至 {@link BTreeSeqScan}，禁用 MVCC 功能，由此类返回的元组指向页中的元组<P>
     * 此类将显示过滤那些对事务可见并且非有效的记录，这将通过 {@link ReadView#READ_COMMIT} 级别判断
     */
    static class BTreeSeqScanDisableMvcc extends BTreeSeqScan {

        public BTreeSeqScanDisableMvcc(TransactionId tid, String tableName, String tableAlias, Predicate predicate) {
            super(tid, tableName, tableAlias, predicate);
        }

        @Override
        protected void openOpIterator() throws DbException {
            super.openOpIterator();
            super.readView = ReadView.createReadView(tid, ReadView.READ_COMMIT); // 当前读
        }

        @Override
        protected Record fetchNext() throws DbException {
            Record record = null;
            while (iterator.hasNext()) {
                RecordId next = iterator.next();
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, next.getPid(), Permissions.READ_ONLY);
                record = page.readRecord(next);
                // 如果记录可见并且已经被删除，则过滤
                if (super.readView.isVisible(record) && !record.isValid()) {
                    continue;
                }
                // 其他情况下直接返回
                return record;
            }
            return null;
        }
    }

    // TODO 继承哈希索引，禁用 MVCC 功能
    static class HashSeqScanDisableMvcc {

    }
}
