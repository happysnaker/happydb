package happydb.common;


import happydb.storage.StringField;
import lombok.val;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * <p>在 HappyDB 中，任何存储都是基于 byte 字节数组的，为此我们提供了一组方便操作字节数组的类，此类具备如下功能：</P>
 * <ul>
 *     <li>此类提供了丰富的构造函数，可以便捷的将大部分对象转换为 {@link ByteArray} 对象</li>
 *     <li>就像 {@link ByteBuffer#slice()} 方法一样，此类的某些构造函数以及 {@link #subArray(int, int)} 方法同样支持共享子数组</li>
 *     <li>此类结合了 {@link java.io.DataInputStream} 和 {@link java.io.DataOutputStream} 的优点，能够基于读取点和写入点顺序处理数据</li>
 *     <li>此类同样支持从指定偏移读取或写入数据</li>
 * </ul>
 * 当索引越界时，此类会抛出 {@link IndexOutOfBoundsException} 或 {@link NoSuchElementException}<br>
 * <strong>此类所有方法不提供线程安全保证</strong>
 * @Author Happysnaker
 * @Date 2022/11/15
 * @Email happysnaker@foxmail.com
 */
public class ByteArray implements Serializable, Cloneable {
    /**
     * 源数组
     */
    protected byte[] byteArray;
    /**
     * 起始点
     */
    protected int startPoint;
    /**
     * 读取点
     */
    protected int readPos;
    /**
     * 写入点
     */
    protected int writePos;
    /**
     * 实际可用的长度
     */
    protected int length;


    /**
     * 获取长度
     *
     * @return 返回字节数组的实际长度
     */
    public int length() {
        return length;
    }


    /**
     * 分配一个大小为 capacity 的字节数组
     *
     * @param capacity 初始化大小
     * @return 返回一个固定容量的 ByteArray
     */
    public static ByteArray allocate(int capacity) {
        return new ByteArray(new byte[capacity]);
    }



    /**
     * 截取 [start, end) 范围构造字共享节数组，如果 start == end，这将构建一个空的字节数组
     *
     * @param array 数组
     * @param start 起始位置包含在内
     * @param end   终止位置不包含在内
     */
    public ByteArray(byte[] array, int start, int end) {
        this.byteArray = array;
        this.startPoint = start;
        this.readPos = start;
        this.writePos = start;
        this.length = end - start;
    }

    /**
     * 截取 [start, array.length) 范围构造字共享节数组
     *
     * @param array 数组
     * @param start 起始位置包含在内
     */
    public ByteArray(byte[] array, int start) {
        this(array, start, array.length);
    }


    /**
     * 以整个源数组构造字共享节数组
     *
     * @param array 数组
     */
    public ByteArray(byte[] array) {
        this(array, 0);
    }


    /**
     * 构造一个仅包含单个字节的字节数组
     *
     * @param val
     */
    public ByteArray(byte val) {
        this(new byte[]{val}, 0);
    }


    /**
     * 构造一个仅包含单个 short 的字节数组
     *
     * @param val
     */
    public ByteArray(short val) {
        this(ByteBuffer.allocate(Short.BYTES).putShort(val).array(), 0);
    }


    /**
     * 构造一个仅包含单个 int 的字节数组
     *
     * @param val
     */
    public ByteArray(int val) {
        this(ByteBuffer.allocate(Integer.BYTES).putInt(val).array(), 0);
    }


    /**
     * 构造一个仅包含单个 long 的字节数组
     *
     * @param val
     */
    public ByteArray(long val) {
        this(ByteBuffer.allocate(Long.BYTES).putLong(val).array(), 0);
    }

    /**
     * 构造一个仅包含单个 double 的字节数组
     *
     * @param val
     */
    public ByteArray(double val) {
        this(ByteBuffer.allocate(Double.BYTES).putDouble(val).array(), 0);
    }

    /**
     * 以缓冲数组构造共享字节数组
     */
    public ByteArray(ByteBuffer buffer) {
        this(buffer.array(), 0);
    }


    /**
     * 以多个字节数组拼接构造共享字节数组，<strong>注意，这并不会进行逻辑上的组合，而是会进行深拷贝</strong>
     */
    public ByteArray(ByteArray... arrays) {
        int size = 0, idx = 0;
        for (ByteArray array : arrays) {
            size += array.length();
        }
        this.length = size;
        this.byteArray = new byte[size];
        this.startPoint = 0;
        for (ByteArray array : arrays) {
            for (int i = 0; i < array.length(); i++) {
                this.byteArray[idx++] = array.get(i);
            }
        }
    }


    /**
     * 以字符串构造字节数组，此构造函数将调用 {@link String#getBytes(String)} 并指定 {@link StandardCharsets#UTF_8} 编码方式获取字节数组
     *
     * @param str
     */
    public ByteArray(String str) {
        this(str.getBytes(StandardCharsets.UTF_8));
    }


    /**
     * 按照给定整型数组顺序写入并构造字节数组
     */
    public ByteArray(int... val) {
        int n = val.length;
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES * n);
        for (int x : val) {
            buffer.putInt(x);
        }
        byteArray = buffer.array();
        startPoint = 0;
        length = byteArray.length;
    }

    /**
     * 以字节链表构造共享字节数组
     */
    public ByteArray(List<Byte> bytes) {
        int n = bytes.size();
        this.byteArray = new byte[n];
        this.length = n;
        this.startPoint = 0;
        for (int i = 0; i < n; i++) {
            byteArray[i] = bytes.get(i);
        }
    }

    /**
     * 在共享数组下标 index 处设置值
     *
     * @param index 从零开始的下标
     * @param val   待设置的值
     * @throws IndexOutOfBoundsException 如果下标大于或等于字节数组的长度或小于零
     */
    public void set(int index, byte val) throws IndexOutOfBoundsException {
        if (index < 0 || index >= length())
            throw new IndexOutOfBoundsException(String.format("index %d out og bound [0, %d)", index, length()));
        byteArray[startPoint + index] = val;
    }

    /**
     * 获取共享数组下标 index 的值
     *
     * @param index 下标
     * @throws IndexOutOfBoundsException 如果下标大于或等于字节数组的长度或小于零
     */
    public byte get(int index) throws IndexOutOfBoundsException {
        if (index < 0 || index >= length())
            throw new IndexOutOfBoundsException(String.format("index %d out og bound [0, %d)", index, length()));
        return byteArray[startPoint + index];
    }



    /**
     * 将此字节数组从 start 位置向后 array.length 个字节更新为参数 array，<strong>update 方法不会导致写入点的变化</strong>
     *
     * @param start 更新起始位置包含在内
     * @param array 更新的数据
     * @throws IndexOutOfBoundsException 如果下标大于或等于字节数组的长度或小于零
     */
    public void update(int start, ByteArray array) throws IndexOutOfBoundsException {
        for (int i = 0; i < array.length(); i++) {
            this.set(start + i, array.get(i));
        }
    }


    /**
     * 将此数组从 start 位置向后 array.length 个字节更新为参数 array，<strong>update 方法不会导致写入点的变化</strong>
     *
     * @param start 更新起始位置包含在内
     * @param array 更新的数据
     * @throws IndexOutOfBoundsException 如果下标大于或等于字节数组的长度或小于零
     */
    public void update(int start, ByteBuffer array) {
        update(start, new ByteArray(array));
    }


    /**
     * 将此数组从 start 位置向后 array.length 个字节更新为参数 array，<strong>update 方法不会导致写入点的变化</strong>
     *
     * @param start 更新起始位置包含在内
     * @param val   更新的数据
     * @throws IndexOutOfBoundsException 如果下标大于或等于字节数组的长度或小于零
     */
    public void update(int start, short val) {
        update(start, new ByteArray(val));
    }

    /**
     * 将此数组从 start 位置向后 array.length 个字节更新为参数 array，<strong>update 方法不会导致写入点的变化</strong>
     *
     * @param start 更新起始位置包含在内
     * @param val   更新的数据
     * @throws IndexOutOfBoundsException 如果下标大于或等于字节数组的长度或小于零
     */
    public void update(int start, int val) {
        update(start, new ByteArray(val));
    }

    /**
     * 将此数组从 start 位置向后 array.length 个字节更新为参数 array，<strong>update 方法不会导致写入点的变化</strong>
     *
     * @param start 更新起始位置包含在内
     * @param val   更新的数据
     * @throws IndexOutOfBoundsException 如果下标大于或等于字节数组的长度或小于零
     */
    public void update(int start, long val) {
        update(start, new ByteArray(val));
    }

    /**
     * 将此数组从 start 位置向后 array.length 个字节更新为参数 array，<strong>update 方法不会导致写入点的变化</strong>
     *
     * @param start 更新起始位置包含在内
     * @param string   更新的数据
     * @throws IndexOutOfBoundsException 如果下标大于或等于字节数组的长度或小于零
     */
    public void update(int start, String string) {
        update(start, new ByteArray(string));
    }

    /**
     * 此方法以引用的方式会返回源数组，<strong>此方法会返回存储在此 ByteArray 中的源数组</strong><br>
     *
     * <p>例如，如果通过 {@link #ByteArray(byte[], int, int)} 方法初始化的对象，此方法会返回参数中的 byte 源数组，而不是被截取后的数组</p>
     *
     * @return 返回源数组
     * @see #getByteArray()
     */
    public byte[] getRawByteArray() {
        return byteArray;
    }


    /**
     * 此方法以深拷贝的方式会返回被截断的源数组
     * <p>例如，如果通过 {@link #ByteArray(byte[], int, int)} 方法初始化的对象，此方法会返回参数中的 byte 源数组 [startPos, endPos) 截取的数组</p>
     *
     * @return 由于需要截断，返回以深拷贝形式进行
     * @see #subArray(int, int)
     */
    public byte[] getByteArray() {

        byte[] ans = new byte[length()];
        System.arraycopy(byteArray, startPoint, ans, 0, length());
        return ans;
    }


    /**
     * 设置当前读取点为对应下标
     *
     * @return 返回自身对象，支持流式编程
     */
    public ByteArray setReadPos(int index) {
        this.readPos = startPoint + index;
        return this;
    }

    /**
     * 获取当前读取点
     *
     * @return 当前读取点
     */
    public int getReadPos() {
        return this.readPos - startPoint;
    }

    /**
     * 重置当前读取点为起始下标 0
     *
     * @return 返回自身对象，支持流式编程
     */
    public ByteArray rewindReadPos() {
        return setReadPos(0);
    }

    /**
     * 计算读取点（包括当前位置）之后还剩多少字节
     *
     * @return 返回剩余字节数
     */
    public int nextBytes() {
        return startPoint + length() - readPos;
    }


    /**
     * 当前读取点（包括当前位置）之后是否还存在一个字节
     *
     * @see #rewindReadPos()
     */
    public boolean hasNextByte() {
        return nextBytes() >= Byte.BYTES;
    }


    /**
     * 当前读取点（包括当前位置）之后是否还存在一个 short
     *
     * @see #rewindReadPos()
     */
    public boolean hasNextShort() {
        return nextBytes() >= Short.BYTES;
    }


    /**
     * 当前读取点（包括当前位置）之后是否还存在一个 int
     *
     * @see #rewindReadPos()
     */
    public boolean hasNextInt() {
        return nextBytes() >= Integer.BYTES;
    }


    /**
     * 当前读取点（包括当前位置）之后是否还存在一个 long
     *
     * @see #rewindReadPos()
     */
    public boolean hasNextLong() {
        return nextBytes() >= Long.BYTES;
    }

    /**
     * 当前读取点（包括当前位置）之后是否还存在一个 double
     *
     * @see #rewindReadPos()
     */
    private boolean hasNextDouble() {
        return nextBytes() >= Double.BYTES;
    }


    /**
     * 读取下一个 byte，此方法会将读取点向后移动
     */
    public byte readByte() {
        if (!hasNextByte())
            throw new NoSuchElementException();
        return byteArray[readPos++];
    }


    /**
     * 读取下一个 short，此方法会将读取点向后移动
     */
    public short readShort() {
        if (!hasNextShort())
            throw new NoSuchElementException();
        var ans = ByteBuffer.wrap(byteArray, readPos, Short.BYTES).getShort();
        readPos += Short.BYTES;
        return ans;
    }


    /**
     * 读取下一个 int，此方法会将读取点向后移动
     */
    public int readInt() {
        if (!hasNextInt())
            throw new NoSuchElementException();
        var ans = ByteBuffer.wrap(byteArray, readPos, Integer.BYTES).getInt();
        readPos += Integer.BYTES;
        return ans;
    }

    /**
     * 读取下一个 double，此方法会将读取点向后移动
     */
    public double readDouble() {
        if (!hasNextDouble())
            throw new NoSuchElementException();
        var ans = ByteBuffer.wrap(byteArray, readPos, Double.BYTES).getDouble();
        readPos += Double.BYTES;
        return ans;
    }

    /**
     * 读取固定长度的字节，并封装为字节数组，此方法会将读取点向后移动
     */
    public ByteArray readByteArray(int len) {
        if (nextBytes() < len)
            throw new NoSuchElementException();
        readPos += len;
        return new ByteArray(byteArray, readPos - len, readPos);
    }

    /**
     * 读取下一个 String，此方法会将读取点向后移动
     *  <br>由于字符串在 HelloDb 总是定长存储的，因此此方法将废弃，字符串读取应该总是调用
     *  {@link happydb.storage.Type#parse(ByteArray)} 读取
     * @param length 待读取的字节长度
     * @return 返回以读取出的字节数组构造的字符串，指定以 {@link StandardCharsets#UTF_8} 编码
     *
     *
     */
    public String readString(int length) {
        if (nextBytes() < length)
            throw new NoSuchElementException();
        byte[] bytes = new byte[length];
        int idx = 0;
        while (length-- != 0) {
            bytes[idx++] = byteArray[readPos++];
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }


    /**
     * 读取下一个 long，此方法会将读取点向后移动
     */
    public long readLong() {
        if (!hasNextLong())
            throw new NoSuchElementException();
        var ans = ByteBuffer.wrap(byteArray, readPos, Long.BYTES).getLong();
        readPos += Long.BYTES;
        return ans;
    }

    /**
     * 从 start 位置读取一个 short，此方法不会导致读取点移动
     *
     * @param start 读取的起始偏移
     * @return 读取的值
     */
    public short readShort(int start) {
        if (start + Short.BYTES > length())
            throw new NoSuchElementException();
        return ByteBuffer.wrap(byteArray, start + startPoint, Short.BYTES).getShort();
    }


    /**
     * 从 start 位置读取一个 int，此方法不会导致读取点移动
     *
     * @param start 读取的起始偏移
     * @return 读取的值
     */
    public int readInt(int start) {
        if (start + Integer.BYTES > length())
            throw new NoSuchElementException();
        return ByteBuffer.wrap(byteArray, start + startPoint, Integer.BYTES).getInt();
    }


    /**
     * 从 start 位置读取一个 long，此方法不会导致读取点移动
     *
     * @param start 读取的起始偏移
     * @return 读取的值
     */
    public long readLong(int start) {
        if (start + Long.BYTES > length())
            throw new NoSuchElementException();
        return ByteBuffer.wrap(byteArray, start + startPoint, Long.BYTES).getLong();
    }

    /**
     * 从 start 位置读取一个 byte，此方法不会导致读取点移动
     *
     * @param start 读取的起始偏移
     * @return 读取的值
     */
    public byte readByte(int start) {
        if (start + 1 > length())
            throw new NoSuchElementException();
        return byteArray[start];
    }


    /**
     * 从 start 位置读取一个 long，此方法不会导致读取点移动
     *
     * @param start 读取的起始偏移
     * @return 读取的值
     */
    public double readDouble(int start) {
        if (start + Double.BYTES > length())
            throw new NoSuchElementException();
        return ByteBuffer.wrap(byteArray, start + startPoint, Double.BYTES).getDouble();
    }

    /**
     * 从 start 处读取下一个 String，此方法不会道中读取点向后移动
     *
     * @param start  读取起始偏移
     * @param length 待读取的字节长度
     * @return 返回以读取出的字节数组构造的字符串，指定以 {@link StandardCharsets#UTF_8} 编码
     */
    public String readString(int start, int length) {
        if (start + length > length())
            throw new NoSuchElementException();
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = get(start + i);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * 从此字节数组中截取部分作为共享数组
     *
     * @param fromIndex 截取起始位置包含在内
     * @param toIndex   截取的终止位置不包含在内
     * @return 返回截取的共享字节数组，对返回数组的改变将会反应到此字节数组中，反之亦然
     */
    public ByteArray subArray(int fromIndex, int toIndex) {
        return new ByteArray(byteArray, startPoint + fromIndex, startPoint + toIndex);
    }


    /**
     * 从此字节数组中截取部分作为共享数组
     *
     * @param fromIndex 截取起始位置包含在内
     * @return 返回截取的共享字节数组，对返回数组的改变将会反应到此字节数组中，反之亦然
     */
    public ByteArray subArray(int fromIndex) {
        return new ByteArray(byteArray, startPoint + fromIndex, startPoint + length());
    }




    /**
     * 比较两个字节数组是否相等，这将一一比较数组元素
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ByteArray array)) {
            return false;
        }
        if (length() != array.length()) {
            return false;
        }
        for (int i = 0; i < length(); i++) {
            if (this.get(i) != array.get(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 比较两个字节数组是否相等，这将一一比较数组元素
     */
    public boolean equals(byte[] bytes) {
        return equals(new ByteArray(byteArray));
    }


    /**
     * 设置写入指针位置
     *
     * @param pos 写入位置
     * @return 返回自身，支持流式编程
     */
    public ByteArray setWritePos(int pos) {
        this.writePos = pos + startPoint;
        return this;
    }

    /**
     * 设置写入指针位置
     *
     * @return 返回自身，支持流式编程
     */
    public ByteArray rewindWritePos() {
        return setWritePos(0);
    }

    /**
     * 向当前写入点写入数据，并推动写入点，<strong>如果写入下标存在值，写入新数据将直接覆盖</strong>
     *
     * @param array 写入的字节数组
     * @return 返回自身，支持流式编程
     */
    public ByteArray writeByteArray(ByteArray array) {
        update(writePos, array);
        writePos += array.length();
        return this;
    }

    /**
     * 向当前写入点写入数据，并推动写入点，<strong>如果写入下标存在值，写入新数据将直接覆盖</strong>
     *
     * @param val 写入的数据
     * @return 返回自身，支持流式编程
     */
    public ByteArray writeByte(byte val) {
        return writeByteArray(new ByteArray(val));
    }

    /**
     * 向当前写入点写入数据，并推动写入点，<strong>如果写入下标存在值，写入新数据将直接覆盖</strong>
     *
     * @param val 写入的字节数组
     * @return 返回自身，支持流式编程
     */
    public ByteArray writeShort(short val) {
        return writeByteArray(new ByteArray(val));
    }

    /**
     * 向当前写入点写入数据，并推动写入点，<strong>如果写入下标存在值，写入新数据将直接覆盖</strong>
     *
     * @param val 写入的字节数组
     * @return 返回自身，支持流式编程
     */
    public ByteArray writeDouble(double val) {
        return writeByteArray(new ByteArray(val));
    }

    /**
     * 向当前写入点写入数据，并推动写入点，<strong>如果写入下标存在值，写入新数据将直接覆盖</strong>
     *
     * @param val 写入的字节数组
     * @return 返回自身，支持流式编程
     */
    public ByteArray writeInt(int val) {
        return writeByteArray(new ByteArray(val));
    }


    /**
     * 向当前写入点写入数据，并推动写入点，<strong>如果写入下标存在值，写入新数据将直接覆盖</strong>
     *
     * @param val 写入的字节数组
     * @return 返回自身，支持流式编程
     */
    public ByteArray writeLong(long val) {
        return writeByteArray(new ByteArray(val));
    }


    /**
     * 向当前写入点写入数据，并推动写入点，<strong>如果写入下标存在值，写入新数据将直接覆盖</strong><br>
     * 由于字符串在 HelloDb 总是定长存储的，字符串需要由 {@link StringField#serialized()} 写入而不能直接写入
     * @param val 写入的字节数组
     * @return 返回自身，支持流式编程
     */
    public ByteArray writeString(String val) {
        return writeByteArray(new ByteArray(val));
    }


    @Override
    public ByteArray clone() throws CloneNotSupportedException {
        ByteArray clone = (ByteArray) super.clone();
        return new ByteArray(clone);
    }


    public int getWritePos() {
        return writePos - startPoint;
    }
}
