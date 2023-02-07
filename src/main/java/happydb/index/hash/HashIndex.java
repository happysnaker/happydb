package happydb.index.hash;

import happydb.common.Catalog;
import happydb.common.Database;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.execution.Predicate;
import happydb.index.EntryId;
import happydb.index.Index;
import happydb.index.IndexType;
import happydb.storage.*;
import happydb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;

/**
 * @Author happysnaker
 * @Date 2023/1/4
 * @Email happysnaker@foxmail.com
 */
public class HashIndex implements Index {

    String tableName;
    HashPageManager pm;
    Type type;
    /**
     * 此哈希索引是否为唯一索引
     */
    boolean unique;

    public HashIndex(String indexTableName) {
        this.tableName = indexTableName;
        this.pm = (HashPageManager) Database.getCatalog().getPageManager(tableName);
        this.type = Catalog.getFieldTypeFromIndexTableName(tableName);
        TableDesc td = Database.getCatalog().getTableDesc(Catalog.getTableNameFromIndexTableName(tableName));
        Set<IndexType> set = IndexType.intToIndexSet(
                td.getIndexType(Catalog.getFieldIndexFromIndexTableName(tableName)));
        this.unique = set.contains(IndexType.HASH_UNIQUE);
    }

    /**
     * 给定一个哈希值，此方法将返回从哈希值映射的位置开始到此映射值前一个位置结束的所有 EntryId 的迭代器
     *
     * @param hash 给定哈希值
     * @return 迭代器
     * @throws IOException
     */
    private Iterator<EntryId> iterator(int hash) throws IOException {
        int m = pm.malloc(0), n = (int) Math.floor((BufferPool.getPageSize() * 8f) / ((type.getLen() + 8) * 8f + 1));
        if (m == 0) {
            return new ArrayList<EntryId>().iterator();
        }
        int x = (hash % (n * m)) / n, y = (hash % (n * m)) % n;
        int i = x, j = y;
        List<EntryId> it = new ArrayList<>();
        do {
            EntryId id = new EntryId(new PageId(tableName, i), j);
            it.add(id);
            j++;
            if (j == n) {
                i = (i + 1) % m;
                j = 0;
            }
        } while (i != x || j != y);
        return it.iterator();
    }

    /**
     * 为了避免死锁，哈希索引每一次只会锁定一个页，此方法能够保证获取某个页面时能够安全的释放其他页面上的锁
     *
     * @throws DbException
     */
    private HashPage safelyGetPage(PageId pid, HashPageHolder holder, Permissions perm) throws DbException {
        HashPage page = holder.getHashPage(pid, perm);
        for (Page holderPage : new HashSet<>(holder.getPages())) {
            if (!page.equals(holderPage)) {
                holder.releasePageIfHolder(holderPage, perm);
            }
        }
        return page;
    }

    @Override
    public void insert(TransactionId tid, Field key, RecordId recordId) throws DbException, IOException {
        HashPageHolder holder = new HashPageHolder(tid, tableName);
        holder.init();
        try {
            doInsert(new HashEntry(key, recordId), holder);
        } finally {
            holder.end();
        }
    }

    /**
     * 插入一个 Entry，当哈希页已满时开始扩容并重试方法
     *
     * @param entry  待插入的 Entry
     * @param holder 本次操作的 holder
     */
    private void doInsert(HashEntry entry, HashPageHolder holder) throws DbException, IOException {
        int hash = entry.getKey().getObject().hashCode();
        Iterator<EntryId> it = null;
        try {
            it = iterator(hash);
            while (it.hasNext()) {
                EntryId next = it.next();
                HashPage page = safelyGetPage(next.getPid(), holder, Permissions.READ_WRITE);

                if (page.putIfAbsent(next.getEntryNumber(), entry)) {
                    page.markDirty(true);
                    return;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        resize(holder);
        doInsert(entry, holder);
    }

    /**
     * 哈希表扩容，创建两倍的新页，并将所有的 Entry 重新计算哈希值插入
     * <p>
     * 如果其他线程抢先扩容，则本次扩容将静默返回
     * <p>
     * 开始扩容时，此线程持有扩容锁，操作将是绝对并发安全的
     *
     * @param holder
     * @throws IOException
     */
    private void resize(HashPageHolder holder) throws DbException, IOException {
        // 页已满，未插入成功，开始扩容
        int identificationStamp = pm.malloc(0);
        holder.resizeLock();
        try {
            // 如果别的线程已经扩容了，那么这一次就先不扩容，直接重试
            if (pm.malloc(0) > identificationStamp) {
                return;
            }

            // 开始扩容
            pm.malloc(identificationStamp == 0 ? 1 : identificationStamp);

            List<HashEntry> entries = new ArrayList<>();
            Iterator<HashPage> pageIterator = pm.iterator();
            while (pageIterator.hasNext()) {
                HashPage page = pageIterator.next();
                Iterator<HashEntry> entryIterator = page.iterator();
                while (entryIterator.hasNext()) {
                    entries.add(entryIterator.next());
                }
                page.clear();
                page.markDirty(true);
            }

            // 重新计算哈希值
            for (HashEntry entry : entries) {
                doInsert(entry, holder);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            holder.resizeUnLock();
        }
    }

    @Override
    public void delete(TransactionId tid, Field key, RecordId recordId) throws DbException {
        throw new RuntimeException("unimplemented method");
    }

    @Override
    public List<RecordId> search(TransactionId tid, Predicate.Op op, Field operand) throws DbException {
        if (op != Predicate.Op.EQUALS) {
            throw new DbException("Hash index only support equals op, but not " + op);
        }
        HashPageHolder holder = new HashPageHolder(tid, tableName);
        holder.init();
        try {
            return doSearch(operand, holder);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                holder.end();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private List<RecordId> doSearch(Field key, HashPageHolder holder) throws IOException, DbException {
        List<RecordId> ret = new ArrayList<>();
        Iterator<EntryId> iterator = iterator(key.getObject().hashCode());
        while (iterator.hasNext()) {
            EntryId entryId = iterator.next();
            HashPage page = safelyGetPage(entryId.getPid(), holder, Permissions.READ_ONLY);

            HashEntry entry = page.readEntry(entryId.getEntryNumber());
            if (entry == null) {
                return ret;
            }

            if (entry.getKey().compare(Predicate.Op.EQUALS, key)) {
                ret.add(entry.getRecordId());
                if (unique) {
                    return ret;
                }
            }
        }
        return ret;
    }
}
