package happydb.common;

import happydb.storage.PageId;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Pair<K, V> {
    public K key;
    public V val;

    public static<K, V> Pair<K, V> create(K k, V v) {
        return new Pair<>(k, v);
    }
}
