package happydb.common;

import happydb.log.CheckPoint;
import happydb.log.LogBuffer;
import happydb.log.Recovery;
import happydb.replication.RaftLogManager;
import happydb.storage.BufferPool;
import happydb.storage.PageId;
import happydb.transaction.LockTable;
import happydb.transaction.ReadView;
import happydb.transaction.TransactionManager;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
public class Database {
    /**
     * 传输时使用的魔数
     */
    public static final short PHANTOM_NUMBER = 250;

    /**
     * 数据库的版本
     */
    public static final byte VERSION = 0x1b;

    /**
     * 指示数据库是否开启
     */
    public static boolean open = false;

    /**
     * 是否开启主从复制
     */
    public static boolean enableReplication = false;

    public static int ISOLATION_LEVEL = ReadView.READ_REPEAT;

    private static AtomicReference<Database> _instance;

    public static String REPOSITORY_DIR;

    public final Catalog catalog;

    public final BufferPool bufferPool;

    public final LogBuffer logBuffer;

    public final TransactionManager transactionManager;

    public final CheckPoint checkPoint;

    public final LockTable lockTable;

    public RaftLogManager raftLogManager;

    public static volatile ScheduledExecutorService service = null;

    /**
     * 运行数据库实例
     */
    public static void run() {
        try {
            open = true;
            if (REPOSITORY_DIR == null) {
                REPOSITORY_DIR = "repo";
            }
            _instance = new AtomicReference<>(new Database());

            getCatalog().loadCatalog();

            Recovery.recovery();

//            演示没有激烈检查点刷盘时恢复例程的正确性，需要注释掉此行代码
            Runtime.getRuntime().addShutdownHook(new Thread(Database::shutDown));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    public static File getDbFile(String fileName) {
        if (!new File(REPOSITORY_DIR).exists()) {
            boolean b = new File(REPOSITORY_DIR).mkdir();
        }
        return new File(REPOSITORY_DIR + "/" + fileName);
    }

    private Database() throws Exception {
        lockTable = new LockTable();

        File catalogFile = getDbFile("catalog");
        if (!catalogFile.exists()) {
            boolean b = catalogFile.createNewFile();
        }
        catalog = new Catalog(new DbFile(catalogFile));

        File transactionManagerFile = getDbFile("db.tx");
        if (!transactionManagerFile.exists()) {
            boolean b = transactionManagerFile.createNewFile();
        }

        transactionManager = new TransactionManager(new DbFile(transactionManagerFile));
        bufferPool = new BufferPool(BufferPool.DEFAULT_PAGES);

        File logBufferFile = getDbFile("db.redo.log");
        if (!logBufferFile.exists()) {
            boolean b = logBufferFile.createNewFile();
        }
        logBuffer = new LogBuffer(new DbFile(logBufferFile));
        checkPoint = new CheckPoint();


        if (enableReplication) {
            File binLogFile = getDbFile("db.bin.log");
            if (!binLogFile.exists()) {
                boolean b = binLogFile.createNewFile();
            }
            raftLogManager = new RaftLogManager(new DbFile(binLogFile));
        }

        if (service == null) {
            service = Executors.newScheduledThreadPool(2);
            service.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    if (Database.open) {
                        Debug.log("后台线程运行，执行模糊检查点");
                        Database.getCheckPoint().fuzzleCheckPoint();
                    }
                }
            }, CheckPoint.RATE, CheckPoint.RATE, TimeUnit.SECONDS);
        }
    }

    /**
     * 返回静态数据库实例的 raftLogManager
     */
    public static RaftLogManager getRaftLogManager() {
        return _instance.get().raftLogManager;
    }

    /**
     * 返回静态数据库实例的 lockTable
     */
    public static LockTable getLockTable() {
        return _instance.get().lockTable;
    }

    /**
     * 返回静态数据库实例的 TransactionManager
     */
    public static TransactionManager getTransactionManager() {
        return _instance.get().transactionManager;
    }

    /**
     * 返回静态数据库实例的检查点实例
     */
    public static CheckPoint getCheckPoint() {
        return _instance.get().checkPoint;
    }

    /**
     * 返回静态数据库实例的日志管理
     */
    public static LogBuffer getLogBuffer() {
        return _instance.get().logBuffer;
    }


    /**
     * 返回静态数据库实例的缓冲池
     */
    public static BufferPool getBufferPool() {
        return _instance.get().bufferPool;
    }

    /**
     * 返回静态数据库实例的目录
     */
    public static Catalog getCatalog() {
        return _instance.get().catalog;
    }

    /**
     * 用于测试的方法——创建缓冲池的新实例并返回它
     */
    public static BufferPool resetBufferPool(int pages) {
        java.lang.reflect.Field bufferPoolF = null;
        try {
            bufferPoolF = Database.class.getDeclaredField("bufferPool");
            bufferPoolF.setAccessible(true);
            bufferPoolF.set(_instance.get(), new BufferPool(pages));
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }
        return _instance.get().bufferPool;
    }

    /**
     * 重置数据库，仅用于单元测试。
     */
    public static void reset() throws Exception {
        shutDown();
        run();
    }

    public static void shutDown() {
        getCheckPoint().sharkCheckPoint();
        open = false;
        getCatalog().close();
    }
}
