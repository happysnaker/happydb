package happydb.index;

import happydb.common.Database;
import happydb.common.Permissions;
import happydb.exception.DbException;
import happydb.execution.Predicate;
import happydb.storage.Field;
import happydb.storage.Page;
import happydb.storage.PageId;
import happydb.storage.RecordId;
import happydb.transaction.TransactionId;
import lombok.Getter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * B+ 树索引，<b>在字符串字段上建立索引存在 BUG，暂不清楚原因</b>
 * @Author happysnaker
 * @Date 2022/11/21
 * @Email happysnaker@foxmail.com
 */
public class BTreeIndex implements Index {
    @Getter
    private final String indexTableName;
    @Getter
    private final BTreeSuperPage superPage;

    public BTreeIndex(String tableName) {
        this.indexTableName = tableName;
        try {
            this.superPage = (BTreeSuperPage) Database.getCatalog()
                    .getPageManager(tableName)
                    .readPage(new PageId(tableName, 0));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * 递归搜索<strong>可能</strong>包含字段 field 的最左侧的页面，对沿途的页面以 perm 权限锁定，并严格参照蟹行协议释放父页面的锁定
     * <P>如果父页面是 null，那么它必须要释放 {@link #superPage} 上的锁定</P>
     *
     * @param holder  本次操作的页面 holder
     * @param currPid 当前递归搜索到的 PID
     * @param parent  父节点页面，在适当的情况释放它身上的锁定，如果为 null 则为根节点页面
     * @param f       搜索字段，允许 null，null 表示返回最左侧的页面
     * @param perm    权限，如果是只读权限，则应该立即释放父节点的锁，否则，需要根据 delete 字段判断是否是插入寻找还是删除寻找，从而运行蟹行协议
     * @param insert  指示本次查找是为了插入做准备还是删除 —— 仅在 perm 为读写权限时生效
     * @return 返回可能包含字段 field 的页面
     */
    private BTreeLeafPage findLeafPage(BTreePageHolder holder, PageId currPid, Page parent, Field f,
                                       Permissions perm, boolean insert) throws DbException {
        BTreePage p = holder.getBTreePage(currPid, perm);

        // 蟹行协议，尝试释放锁
        if (perm == Permissions.READ_WRITE) {
            if (insert && p.getEmptySlots().size() > 0) {
                // 如果页面非满，则此页不会分裂，因此可以释放父节点
                if (parent == null) {
                    holder.releasePageIfHolder(superPage, perm);
                } else {
                    holder.releasePageIfHolder(parent, perm);
                }
                // 请思考，蟹行协议是否有不足之处？
                // 是的，我们没能做到递归向上解锁，这是一个针对蟹行协议的优化点
            }
        }

        if (p.getCategory() == BTreePage.LEAF) {
            return (BTreeLeafPage) p;
        }
        BTreeInternalPage page = (BTreeInternalPage) p;
        BTreeInternalEntry[] entryAr = page.getEntryAr();

        if (entryAr.length == 0)
            throw new DbException("内部节点为空");

        for (BTreeInternalEntry it : entryAr) {
            // 因为要找到最左节点，所以相等也一律递归左孩子，即 f <= next.key
            if (f == null || f.compare(Predicate.Op.LESS_THAN_OR_EQ, it.getKey())) {
                return findLeafPage(holder, it.getLeftChild(), page, f, perm, insert);
            }
        }
        // f 大于全部 entry 的 key，则返沪最右的指针
        return findLeafPage(holder, entryAr[entryAr.length - 1].getRightChild(), page, f, perm, insert);
    }

    /**
     * 拆分叶子节点使得他们可以容纳更多条目，请注意，为了避免死锁，总是将拆分的新节点作为右兄弟
     *
     * @param holder 本次操作的页面 holder
     * @param page   待拆分的叶子页面
     * @param field  待插入的节点
     * @return 返回页面或拆分的新页面，此页面可以插入字段 field
     * @throws DbException
     */
    private BTreeLeafPage splitLeafPage(BTreePageHolder holder, BTreeLeafPage page, Field field) throws DbException, IOException {
        // 创建并锁定新页面
        BTreeLeafPage newPage = (BTreeLeafPage) holder.getBTreePage(superPage.malloc(BTreePage.LEAF), Permissions.READ_WRITE);
        // 将 page 中的一半移到新页去
        // 更正式的说，如果有 N 个元组，那么 [0, N/2) 作为此页的元组，[N/2，N) 作为新页的元组，N/2 下标处的元组将被向上移动
        BTreeLeafEntry[] entryAr = page.getEntryAr();
        int n = entryAr.length;
        if (n != page.getMaxNumEntries())
            throw new DbException("错误的调用，页面非满");

        for (int i = 0; i < n; i++) {
            var next = entryAr[i];
            if (i >= n / 2) {
                page.deleteEntry(next);
                newPage.insertEntry(next);
            }
        }

        int oldRightId = page.getRightSibling();
        if (oldRightId != 0) {
            BTreeLeafPage right = (BTreeLeafPage) holder.getBTreePage(new PageId(page.getPageId().getTableName(), oldRightId), Permissions.READ_WRITE);
            // 双重验证，避免与 delete 冲突死锁
            if (page.getRightSibling() == oldRightId) {
                newPage.setRightSibling(oldRightId);
                right.setLeftSibling(newPage.getPageId().getPageNumber());
            }
        }
        page.setRightSibling(newPage.getPageId().getPageNumber());
        newPage.setLeftSibling(page.getPageId().getPageNumber());

        Field midField = newPage.getEntryAr()[0].getKey();
        BTreeInternalPage parent = getParentWithEmptySlot(holder, page.getParent(), midField);
        parent.insertEntry(new BTreeInternalEntry(midField, page.getPageId(), newPage.getPageId()));

        safeUpdateParentPointer(holder, parent.getPageId(), page.getPageId());
        safeUpdateParentPointer(holder, parent.getPageId(), newPage.getPageId());


        page.markDirty(true);
        newPage.markDirty(true);
        parent.markDirty(true);

        // 实际要插入哪个页面？如果页面只能容纳下一个元组，那么左页面是空的，否则需要与中间节点比较
        if (n == 1)
            return page;

        return midField.compare(Predicate.Op.GREATER_THAN_OR_EQ, field) ? page : newPage;
    }


    /**
     * 拆分内部节点使得他们可以容纳更多条目，请注意，为了避免死锁，总是将拆分的新节点作为右兄弟
     * <P>拆分动作需要将中间节点向上推，可能会涉及到递归的拆分父节点</P>
     *
     * @param holder 本次操作的页面 holder
     * @param page   待拆分的内部页面
     * @param field  待插入的节点
     * @return 返回页面或拆分的新页面，此页面可以插入字段 field
     * @throws DbException
     * @throws IOException
     */
    public BTreeInternalPage splitInternalPage(BTreePageHolder holder, BTreeInternalPage page, Field field)
            throws DbException, IOException {
        // 锁定新页
        BTreeInternalPage newPage = (BTreeInternalPage) holder.getBTreePage(superPage.malloc(BTreePage.INTERNAL), Permissions.READ_WRITE);

        // 与 Leaf 分裂有一点小差距，新页不会包含中间元组，中间元组需要向上提
        // 如果有 N 个元组，那么 [0, N/2) 作为此页的元组，(N/2，N) 作为新页的元组，N/2 下标处的元组将被向上移动
        BTreeInternalEntry midEntry = null;
        BTreeInternalEntry[] entryAr = page.getEntryAr();
        int n = entryAr.length;

        if (n != page.getMaxNumEntries())
            throw new DbException("错误的调用，页面非满");

        for (int i = 0; i < n; i++) {
            var next = entryAr[i];
            if (i >= n / 2) {
                page.deleteKeyAndRightChild(next);
                if (i == n / 2) {
                    midEntry = next;
                } else {
                    newPage.insertEntry(next);
                }
            }
        }


        assert midEntry != null;

        // 中间的节点向上提，左指针为 page，右指针为 newPage
        midEntry.setLeftChild(page.getPageId());
        midEntry.setRightChild(newPage.getParent());

        // 递归向上插入
        Field midField = midEntry.getKey();
        BTreeInternalPage parent = getParentWithEmptySlot(holder, page.getParent(), midField);
        parent.insertEntry(midEntry);

        // 新页的孩子应该修改他们的 parent 指向
        safeUpdateParentPointers(holder, newPage);

        // 最后，别忘记了标记脏页，以便它们能落盘
        page.markDirty(true);
        newPage.markDirty(true);
        parent.markDirty(true);
        return midField.compare(Predicate.Op.GREATER_THAN_OR_EQ, field) ? page : newPage;
    }


    /**
     * 向上搜索一个带有空槽的父节点，如果父节点已满，则会递归的进行拆分操作
     * <P>任何操作应当严格遵循蟹行协议，当需要向父节点插入字段时，父节点应该提前被事务锁定，因此此方法不会释放写锁</P>
     * <P>如果此操作创建了新的根节点，则会以写模式锁定新的根节点</P>
     *
     * @param holder   本次操作的页面 holder
     * @param parentId 递归到的页面 ID
     * @param field    待插入的键
     * @return 返回可以带有空槽的父节点
     * @throws DbException 一些异常或者 parentId 上不存在写锁
     */
    private BTreeInternalPage getParentWithEmptySlot(BTreePageHolder holder, PageId parentId, Field field) throws DbException, IOException {

        BTreeInternalPage parent = null;

        if (!holder.isHoldLock(parentId, Permissions.READ_WRITE)) {
            throw new DbException("未能正确实现蟹行协议规范");
        }

        // 没有父节点了，需要创建父节点，并作为根节点，同时更新指针指向
        if (parentId.getPageNumber() == 0) {
            parent = (BTreeInternalPage) holder.getBTreePage(superPage.malloc(BTreePage.INTERNAL), Permissions.READ_WRITE);

            // 更新根指针
            PageId prevRootId = superPage.getBtreeRootPageId();
            superPage.setBtreeRootPageId(parent.getPageId());
            superPage.markDirty(true);

            // 更新以前的根现在指向这个新的根
            BTreePage prevRoot = holder.getBTreePage(prevRootId, Permissions.READ_WRITE);
            prevRoot.setParent(parent.getPageId());
            prevRoot.markDirty(true);
        } else {
            // 否则，存在父节点
            parent = (BTreeInternalPage) holder.getBTreePage(parentId, Permissions.READ_WRITE);
        }

        // 递归向上拆分
        if (parent.getEmptySlots().size() == 0) {
            parent = splitInternalPage(holder, parent, field);
        }
        return parent;
    }


    /**
     * 用于更新节点的父指针的辅助函数。
     *
     * <P>此方法会首先利用读者锁判断是否有必要更新</P>
     * <p>
     * 如果有必要，此方法会以写者模式获取子页面，如果子页面原先持有锁，则此方法会安全的恢复到原先的状态
     * <P><strong>此方法会标记子节点为脏，如果需要释放写锁，此方法会调用
     * {@link BTreePageHolder#releasePageIfHolder(Page, Permissions)} 方法尝试将其落盘</strong></P>
     * <P>之所以不严格锁定子页面是因为我们仅仅只是为了修改子页面的 parent 指向，不会影响其本身的条目，
     * 没有必要等到操作结束后统一释放，提前释放以加大并发性，除非这些页面上本身就被当前事务锁定</P>
     *
     * @param holder 页面 holder
     * @param parent 父节点的 ID
     * @param child  子节点的 ID，将更新其父节点
     * @throws DbException
     */
    private void safeUpdateParentPointer(BTreePageHolder holder, PageId parent, PageId child)
            throws DbException {
        boolean hasWriteLock = holder.isHoldLock(child, Permissions.READ_WRITE);
        boolean hasReadLock = holder.isHoldLock(child, Permissions.READ_ONLY);

        BTreePage page = holder.getBTreePage(child, Permissions.READ_ONLY);
        if (page.getPageId().equals(parent)) {
            if (!hasReadLock) {
                holder.releasePageIfHolder(page, Permissions.READ_ONLY);
            }
            return;
        }

        page = holder.getBTreePage(child, Permissions.READ_WRITE);
        page.setParent(parent);
        page.markDirty(true);
        if (hasReadLock) {
            // 锁降级
            holder.getBTreePage(child, Permissions.READ_ONLY);
            holder.releasePageIfHolder(page, Permissions.READ_WRITE);
        } else if (!hasWriteLock) {
            holder.releasePageIfHolder(page, Permissions.READ_WRITE);
        }
    }

    /**
     * 修改页面的所有的子项，使得他们正确的指向页面，如果严格遵循蟹行协议，其余事务无法绕过父节点锁定子节点，因此子节点应该是安全的
     * <P>但也有例外，例如搜索例程遍历叶子节点，他可能会以读者模式锁定叶子节点，但是搜索例程只会搜索叶子节点，
     * 而此方法可能也会锁定叶子节点，但是，搜索例程是从左向右搜索，此方法也是如此，破坏了循环等待条件，<strong>因此不会产生死锁</strong>
     * </P>
     * <P>{@link #safeUpdateParentPointer(BTreePageHolder, PageId, PageId)} 提供了安全的调用方式，因此本方法也是安全的</P>
     *
     * @param holder 页面 holder
     * @param page   页面，它必须已经被锁定
     * @throws DbException
     */
    private void safeUpdateParentPointers(BTreePageHolder holder, BTreeInternalPage page)
            throws DbException {
        var it = page.iterator();
        var pid = page.getPageId();

        if (!holder.isHoldLock(pid, Permissions.READ_WRITE))
            throw new DbException("页面未能被正确锁定");

        BTreeInternalEntry e = null;
        while (it.hasNext()) {
            e = it.next();
            safeUpdateParentPointer(holder, pid, e.getLeftChild());
        }
        if (e != null) {
            safeUpdateParentPointer(holder, pid, e.getRightChild());
        }
    }

    @Override
    public void insert(TransactionId tid, Field key, RecordId recordId) throws DbException, IOException {
        BTreePageHolder holder = new BTreePageHolder(superPage, tid);
        // 先锁定超级页，然后读取根节点
        BTreeSuperPage sp = holder.getSuperPage(Permissions.READ_WRITE);
        PageId rootPid = sp.getBtreeRootPageId();
        if (rootPid.getPageNumber() == 0) {
            // 没有根节点，创建根节点，此时根节点是叶子节点
            try {
                rootPid = sp.malloc(BTreePage.LEAF);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            sp.setBtreeRootPageId(rootPid);
            sp.markDirty(true);
        }

        BTreeLeafPage leafPage = findLeafPage(holder, rootPid, null, key, Permissions.READ_WRITE, true);

        if (leafPage.getEmptySlots().size() == 0) {
            leafPage = splitLeafPage(holder, leafPage, key);
        }

        leafPage.insertEntry(new BTreeLeafEntry(key, recordId));
        leafPage.markDirty(true);
        try {
            holder.releaseAllPages();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(TransactionId tid, Field key, RecordId recordId) {
        throw new RuntimeException("没实现这个功能");
    }


    @Override
    public List<RecordId> search(TransactionId tid, Predicate.Op op, Field operand) throws DbException {
        BTreePageHolder holder = new BTreePageHolder(superPage, tid);
        List<RecordId> ans = new ArrayList<>();
        try {
            // 先锁定超级页，然后读取根节点，防止根节点被改变
            BTreeSuperPage sp = holder.getSuperPage(Permissions.READ_ONLY);

            // 树还没被建立
            if (sp.getBtreeRootPageId().getPageNumber() == 0) {
                return ans;
            }
            // 某些谓词需要从头遍历
            BTreeLeafPage leafPage = shouldFindFromLeft(op) ?
                    findLeafPage(holder, sp.getBtreeRootPageId(), null, null, Permissions.READ_ONLY, true) :
                    findLeafPage(holder, sp.getBtreeRootPageId(), null, operand, Permissions.READ_ONLY, true);

            Iterator<BTreeLeafEntry> iterator = new LeafPageScan(leafPage, holder);

            out:
            while (iterator.hasNext()) {
                BTreeLeafEntry next = iterator.next();
                // 全表扫描
                if (op == null) {
                    ans.add(next.getRecordId());
                    continue;
                }
                switch (op) {
                    case NOT_EQUALS, GREATER_THAN, GREATER_THAN_OR_EQ -> {
                        // 这种必须从扫描项遍历到末尾
                        // 其中 NOT_EQUALS 从头遍历，GREATER_THAN 是从 operand 开始遍历的
                        if (next.getKey().compare(op, operand)) {
                            ans.add(next.getRecordId());
                        }
                    }
                    case EQUALS, LIKE -> {
                        // 这种是从 operand 最左页面扫描，碰到第一个大于 operand 的时候就不需要继续遍历了，后面肯定都是大于的
                        if (next.getKey().compare(op, operand)) {
                            ans.add(next.getRecordId());
                        }
                        if (next.getKey().compare(Predicate.Op.GREATER_THAN, operand)) {
                            break out;
                        }
                    }
                    case LESS_THAN, LESS_THAN_OR_EQ -> {
                        // 这种是从头扫描的，因为是小于或小于等于，符合的都是前面的，碰到第一个不符合的记录就可以退出了
                        if (next.getKey().compare(op, operand)) {
                            ans.add(next.getRecordId());
                        } else {
                            break out;
                        }
                    }
                }
            }
        } finally {
            try {
                holder.releaseAllPages();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return ans;
    }

    public boolean shouldFindFromLeft(Predicate.Op op) {
        return op == Predicate.Op.NOT_EQUALS ||
                op == Predicate.Op.LESS_THAN ||
                op == Predicate.Op.LESS_THAN_OR_EQ;

    }

    private static class LeafPageScan implements Iterator<BTreeLeafEntry> {

        private BTreeLeafPage currPage;
        private final BTreePageHolder holder;

        public LeafPageScan(BTreeLeafPage leafPage, BTreePageHolder holder) {
            this.currPage = leafPage;
            this.holder = holder;
            this.iterator = leafPage.iterator();
        }

        BTreeLeafEntry nextToReturn;
        Iterator<BTreeLeafEntry> iterator = null;

        @Override
        public boolean hasNext() {
            if (nextToReturn != null) {
                return true;
            }
            while (!iterator.hasNext()) {
                if (currPage.getRightSibling() == 0) {
                    return false;
                }
                try {
                    currPage = (BTreeLeafPage) holder.getBTreePage(new PageId(
                            currPage.getPageId().getTableName(), currPage.getRightSibling()), Permissions.READ_ONLY);
                    iterator = currPage.iterator();
                } catch (DbException e) {
                    throw new RuntimeException(e);
                }
            }
            nextToReturn = iterator.next();
            return true;
        }

        @Override
        public BTreeLeafEntry next() {
            if (nextToReturn == null && !hasNext()) {
                throw new NoSuchElementException();
            }
            var ret = nextToReturn;
            nextToReturn = null;
            return ret;
        }
    }
}
