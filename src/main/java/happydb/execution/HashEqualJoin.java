package happydb.execution;

import happydb.exception.DbException;
import happydb.storage.Field;
import happydb.storage.Record;
import happydb.storage.TableDesc;
import lombok.Getter;

import java.util.*;

/**
 * 使用哈希连接法在等值条件下的连接
 *
 * @Author happysnaker
 * @Date 2022/11/23
 * @Email happysnaker@foxmail.com
 */
public class HashEqualJoin extends AbstractOpIterator {
    @Getter
    private final JoinPredicate joinPredicate;
    @Getter
    private final OpIterator child1;
    @Getter
    private final OpIterator child2;
    private final TableDesc comboTD;
    private Record record = null;

    public HashEqualJoin(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.joinPredicate = p;
        this.child1 = child1;
        this.child2 = child2;
        comboTD = TableDesc.merge(child1.getTableDesc(), child2.getTableDesc());
    }


    public TableDesc getTableDesc() {
        return comboTD;
    }


    final Map<Field, List<Record>> map = new HashMap<>();


    private void initMap() throws DbException {
        while (child1.hasNext()) {
            var next = child1.next();
            Field field = next.getField(joinPredicate.getField1());
            map.putIfAbsent(field, new ArrayList<>());
            map.get(field).add(next);
        }
    }

    public void openOpIterator() throws DbException, NoSuchElementException {
        child1.open();
        child2.open();
        initMap();
    }

    public void closeOpIterator() throws DbException {
        child2.close();
        child1.close();
        this.record = null;
        this.matchListIt = null;
        this.map.clear();
    }

    public void rewind() throws DbException {
        child1.rewind();
        child2.rewind();
    }

    Iterator<Record> matchListIt = null;


    /**
     * 返回连接生成的下一个元组，如果没有更多元组则返回 null。从逻辑上讲，这是 r1 交叉 r2 中满足连接谓词的下一个元组。此类实现的是嵌套循环连接。
     * <p>
     * 请注意，从 Join 的这个特定实现返回的元组只是从左右关系连接元组的串联。因此，如果使用相等谓词，结果中将有两个 join 属性副本。
     * （如果需要，可以使用额外的投影运算符删除此类重复列。）
     * <p>
     * 例如，如果一个元组是 {1,2,3}，另一个元组是 {1,5,6}，根据第一列的相等性连接，则返回 {1,2,3,1,5,6 }.
     *
     * @return 下一个匹配的元组。
     * @see JoinPredicate#filter
     */
    protected Record fetchNext() throws DbException {
        // map 中还存在元素与 record 匹配
        if (matchListIt != null && matchListIt.hasNext()) {
            return NormalJoin.mergeRecord(matchListIt.next(), record);
        }
        // child2 的 record 匹配完了，继续匹配 child2 的下一个元素
        while (child2.hasNext()) {
            record = child2.next();
            List<Record> match = map.get(record.getField(joinPredicate.getField2()));
            // 没有匹配的，跳过这个
            if (match == null) {
                continue;
            }
            matchListIt = match.iterator();
            return NormalJoin.mergeRecord(matchListIt.next(), record);
        }
        return null;
    }
}
