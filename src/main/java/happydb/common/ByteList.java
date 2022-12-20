package happydb.common;

import happydb.common.ByteArray;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * <p>此类提供变长的 ByteArray 实现，此类支持调用 <strong>write</strong> 方法动态的添加数据，数组的长度会自动扩容</p>
 * <br>
 * 不过，这有一些限制：
 * <ul>
 *     <li>调用 <strong>update</strong> 方法在数组长度外更新数据将会导致错误，而不会引起扩容</li>
 *     <li>调用 {@link #getByteArray()} 的结果是未定义的，数组可能或由于扩容而发生变化，这意味着共享数组可能并不适用</li>
 * </ul>
 * 此类的用途在于，<strong>在序列号写入磁盘时，它可能非常方便，因为无需提前计算数组的大小</strong>
 * @author Happysnaker
 * @Date 2022/5/12
 * @Email happysnaker@foxmail.com
 */
public class ByteList extends ByteArray {
    private int capacity;

    public ByteList(int capacity) {
        super(new byte[capacity], 0, 0);
        this.capacity = capacity;
    }

    public ByteList() {
        this(16);
    }

    @Override
    public ByteArray writeByteArray(ByteArray array) {
        // 顺序写入，实际的长度增加
        super.length += array.length();
        // 需要扩容
        if (super.length > this.capacity) {
            int newCap = Math.max(this.capacity << 1, super.length);
            super.byteArray = Arrays.copyOf(super.byteArray, newCap);
            this.capacity = newCap;
        }
        return super.writeByteArray(array);
    }
}
