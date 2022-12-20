package happydb.common;

import lombok.Data;
import lombok.Getter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/**
 * 在 HelloDb 中，与磁盘打交道是再正常不过的事情，此类封装了一些方法来简化操作，<strong>此类的任何方法都是严格并发安全的</strong>
 * @Author happysnaker
 * @Date 2022/11/15
 * @Email happysnaker@foxmail.com
 */
public class DbFile {
    @Getter
    private final File file;

    public DbFile(File file) throws FileNotFoundException {
        this.file = file;
    }

    public DbFile(String file) throws FileNotFoundException {
        this(new File(file));
    }


    /**
     * 向文件中指定偏移写入数据，此方法默认强制刷新缓冲
     * @param offset 偏移
     * @param data 数据
     * @throws IOException
     */
    public synchronized void write(long offset, ByteArray data) throws IOException {
        write(offset, data, true);
    }

    /**
     * 向文件中指定偏移写入数据
     * @param offset 偏移
     * @param data 数据
     * @param flush 指示是否要强制刷新
     * @throws IOException
     */
    public synchronized void write(long offset, ByteArray data, boolean flush) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(offset);
            for (int i = 0; i < data.length(); i++) {
                raf.write(data.get(i));
            }
//            raf.write(data.getByteArray());

            if (flush) {
                raf.getFD().sync();
                raf.getChannel().force(true);
            }
        }
    }

    /**
     * 向文件末尾追加数据
     * @param data 数据
     * @param flush 指示是否要强制刷新
     * @throws IOException
     */
    public synchronized void append(ByteArray data, boolean flush) throws IOException {
        write(file.length(), data, flush);
    }

    /**
     * 从文件中指定偏移读取固定大小的数据，并放入字节数组中
     * @param offset 偏移
     * @param buffer 字节数组
     * @throws IOException
     * @throws NoSuchElementException 如果内容不足以填满字节数组
     */
    public synchronized void read(long offset, ByteArray buffer) throws IOException, NoSuchElementException {
        byte[] bytes = new byte[buffer.length()];
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(offset);
            int read = raf.read(bytes);
            if (read != bytes.length) {
                throw new NoSuchElementException("读取长度与缓冲区长度不相等");
            }
        }
        buffer.update(0, ByteBuffer.wrap(bytes));
    }

    /**
     * 从文件中指定偏移读取固定大小的数据，并返回字节数组
     * @param offset 指定偏移
     * @param len 要读取的长度
     * @return 固定大小的字节数组
     * @throws IOException
     * @throws NoSuchElementException 如果文件内容从指定偏移开始不足 len 字节抛出
     */
    public synchronized ByteArray read(long offset, int len) throws IOException, NoSuchElementException {
        ByteArray array = ByteArray.allocate(len);
        read(offset, array);
        return array;
    }

    /**
     *设置此文件的长度。 <p> 如果 {@code length} 方法返回的文件的当前长度大于 {@code newLength} 参数，
     * 那么文件将被截断。在这种情况下，如果 {@code getFilePointer} 方法返回的文件偏移量大于 {@code newLength}，
     * 那么在此方法返回后偏移量将等于 {@code newLength}。 <p> 如果 {@code length} 方法返回的文件的当前长度小于 {@code newLength} 参数，
     * 那么文件将被扩展。在这种情况下，文件扩展部分的内容未定义。
     *
     * @param      newLength    所需的文件长度
     * @throws     IOException  If an I/O error occurs
     */
    public synchronized void setLength(long newLength) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(newLength);
        }
    }

    /**
     * 获取文件长度
     */
    public long getLength() throws IOException {
        return getFile().length();
    }

    /**
     * 关闭文件句柄
     */
    public void close() {

    }
}
