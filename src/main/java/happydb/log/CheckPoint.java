package happydb.log;

import happydb.common.Database;
import happydb.common.Debug;
import happydb.common.Pair;
import happydb.exception.DbException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

/**
 * 检查点机制，推进脏页，并记录检查点，<strong>检查点为刷盘后内存中脏页最小的 LSN</strong>
 * <P>检查点允许因为某些原因设置的较小（例如 {@link happydb.storage.BufferPool#evictPage(long, boolean, boolean, boolean)}），
 * 但它不能不能跳过某些脏页设置的较大，这会导致恢复出错</P>
 *
 * @Author happysnaker
 * @Date 2022/11/30
 * @Email happysnaker@foxmail.com
 */
public class CheckPoint {
    /**
     * 后台线程运行间隔（秒）
     */
    public static int RATE = 30;
    Random random = new Random();


    /**
     * 模糊检查点，将随机推进检查点，并尝试驱逐一些页面（但它绝不保证）
     */
    public synchronized void fuzzleCheckPoint() {
        Iterator<Pair<Long, DataPage>> it = Database.getLogBuffer().getFlushList();
        long ckp = -1;
        while (it.hasNext()) {
            Pair<Long, DataPage> next = it.next();
            try {
                Database.getBufferPool().unsafeFlushPage(next.getVal().getPageId());
                // 如果失败，无法继续走下去
                ckp = next.getKey();
                if (random.nextInt(2) == 0) {
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // ckp 是比最后落盘页面 LSN 大的下一个重做日志的 LSN
        // 但是，由于日志可能已经刷盘，我们不知道下一个更大的日志的 LSN
        // 因此检查点就设置为最后落盘页面 LSN，他只落后一个版本，恢复起来也很快

        if (ckp != -1) {
            try {
                Database.getLogBuffer().pushCheckPoint(ckp);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            try {
                Database.getBufferPool().evictPage(1, true, true, false);
            } catch (DbException | IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 激烈检查点，尝试刷新所有页面落盘，<B>仅数据库关闭时使用，调用此方法时，应保证数据库不会继续产生脏页</B>
     */
    public synchronized void sharkCheckPoint() {
        Debug.log("shark checkpoint started");
        long ckp = Database.getLogBuffer().getCurrentLsn();

        try {
            Database.getBufferPool().flushAllDirtyPages();
            // 如果失败了走不下去
            // 如果成功了，那么脏页被全部刷回
            if (ckp != -1) {
                Debug.log("new checkpoint is " + ckp);
                Database.getLogBuffer().pushCheckPoint(ckp);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
