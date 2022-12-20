package happydb;

import happydb.common.Database;
import happydb.log.CheckPoint;
import happydb.storage.BufferPool;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

/**
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
public class TestBase {
    public static final String TEST_TEMP_DIR = "test_tmp_dir";

    @Before
    public void setUpOnBase() throws Exception {
        File file = new File(TEST_TEMP_DIR);
        if (!file.exists()) {
            boolean b = file.mkdir();
        }
        BufferPool.DEFAULT_PAGES = 50;
        CheckPoint.RATE = Integer.MAX_VALUE;
        Database.REPOSITORY_DIR = TEST_TEMP_DIR + "/" + UUID.randomUUID();
        Database.run();
    }

    @After
    public void clearDirOnBase() throws InterruptedException {
        Database.shutDown();
        deleteFile(new File(TEST_TEMP_DIR));
    }

    public void deleteFile(File file) {
        if (file.exists() && file.isFile()) {
            if (!file.delete()) {
                file.deleteOnExit();
            }
        }

        if (file.exists() && file.isDirectory()) {
            for (File listFile : Objects.requireNonNull(file.listFiles())) {
                deleteFile(listFile);
            }
            if (!file.delete()) {
                file.deleteOnExit();
            }
        }
    }
}
