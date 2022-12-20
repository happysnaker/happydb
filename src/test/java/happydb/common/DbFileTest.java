package happydb.common;

import happydb.TestBase;
import happydb.TestUtil;
import happydb.transaction.TransactionId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * @Author happysnaker
 * @Date 2022/11/15
 * @Email happysnaker@foxmail.com
 */
public class DbFileTest extends TestBase {

    DbFile df;
    ByteArray byteAr;

    @Before
    public void setUp() throws IOException {
        ByteArrayTest byteArrayTest = new ByteArrayTest();
        byteArrayTest.setUp();
        byteAr = byteArrayTest.byteAr;

        File f = File.createTempFile("hello_db_file_test", ".tmp", new File(TEST_TEMP_DIR));
        df = new DbFile(f);
    }

    @Test
    public void testReadWrite() throws IOException {
        int length = byteAr.length();
        df.setLength(length);
        df.write(0, byteAr);
        Assert.assertEquals(byteAr, df.read(0, byteAr.length));

        df.setLength(length + 1024);
        df.write(1024, byteAr);
        Assert.assertEquals(byteAr, df.read(1024, byteAr.length));
        Assert.assertEquals(1024 + length, df.getLength());

        df.setLength(1024 + length / 2);
        Assert.assertEquals(byteAr.subArray(0, length / 2), df.read(1024L, length / 2));
    }

    /**
     * 多个线程读取不同偏移位置应该互不干扰，此测试可能运行时间较长
     */
    @Test
    public void testManyThreadReadWrite() throws IOException, TimeoutException {
        int n = 512;
        df.setLength(n * 2);
        Assert.assertEquals(n * 2, df.getLength());
        List<TestUtil.TestRunnable> tasks = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            int finalI = i;
            tasks.add(new TestUtil.TestRunnable() {
                @Override
                public void run() {
                    try {
                        long offset = finalI * 2;
                        for (int j = 0; j < 64; j++) {
                            df.write(offset, new ByteArray((short) j), true);
                            Assert.assertEquals(j, df.read(offset, 2).readShort());
                        }
                        Debug.log("%s done.", Thread.currentThread().getName());
                        setDone(true);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        TestUtil.runManyThread(tasks, 1000 * 60 * 3);
    }


    @After
    public void close() {
        df.close();
        df.getFile().deleteOnExit();
    }
}
