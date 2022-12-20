package happydb.index;

import happydb.exception.DbException;
import happydb.execution.Predicate;
import happydb.storage.Field;
import happydb.storage.RecordId;
import happydb.transaction.TransactionId;

import java.io.IOException;
import java.util.List;

/**
 * @Author happysnaker
 * @Date 2022/11/21
 * @Email happysnaker@foxmail.com
 */
public interface Index {
    /**
     * 向索引中插入一个条目
     *
     * @param tid
     * @param key      键
     * @param recordId 指向行记录的 ID
     */
    void insert(TransactionId tid, Field key, RecordId recordId) throws DbException, IOException;

    /**
     * 删除指定的条目
     *
     * @param tid
     * @param key      键
     * @param recordId 指定的条目
     */
    void delete(TransactionId tid, Field key, RecordId recordId) throws DbException;

    /**
     * 查找符合条件的条目
     *
     * @param tid
     * @param op      操作符，允许为空，为空则全表扫描
     * @param operand 操作数，允许为空，为空则全表扫描
     * @return 返回指定的条目
     */
    List<RecordId> search(TransactionId tid, Predicate.Op op, Field operand) throws DbException;
}
