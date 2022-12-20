package happydb.index;

import java.util.HashSet;
import java.util.Set;

/**
 * 索引类型，请注意，<b>UNIQUE 仅仅只是个逻辑标识符，并不存在对应的索引文件，实际的索引文件只有 BTREE 和 HASH 两种</b>，
 * HappyDb 不允许同一种索引同时存在普通索引和唯一索引
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
public enum IndexType {
    /**
     * 是否为主键索引，主键索引一定是唯一 B+ 树索引
     */
    PRIMARY_KEY,
    /**
     * B+ 树索引
     */
    BTREE,
    /**
     * 哈希索引
     */
    HASH,
    /**
     * 唯一索引，如果一个字段是 BTREE_UNIQUE，那么它一定也是 BTREE
     */
    BTREE_UNIQUE,
    /**
     * 唯一索引，如果一个字段是 HASH_UNIQUE，那么它一定也是 HASH
     */
    HASH_UNIQUE;

    /**
     * 将索引集合转换为一个整数, byte 虽然节省空间，但只能表示八种类型，比较局限
     * @param indexTypeSet 索引集合
     * @return 一个包含了索引类型集合信息的整数
     */
    public static int indexSetToInt(Set<IndexType> indexTypeSet) {
        IndexType[] values = values();
        int mask = 0;
        for (int i = 0; i < values.length; i++) {
            if (indexTypeSet.contains(values[i])) {
                mask |= (1 << i);
            }
        }
        return mask;
    }

    /**
     * 将包含索引类型集合的整数反解析为索引类型集合
     * @param indexType 一个整肃
     * @return 索引类型集合
     */
    public static Set<IndexType> intToIndexSet(int indexType) {
        Set<IndexType> ans = new HashSet<>();
        IndexType[] values = values();
        for (int i = 0; i < values.length; i++) {
            if ((indexType & (1 << i)) != 0) {
                ans.add(values[i]);
            }
        }
        return ans;
    }
}
