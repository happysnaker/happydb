package happydb.storage;

import happydb.common.DbFile;
import lombok.Getter;

import java.io.File;
import java.io.IOException;

/**
 * @Author happysnaker
 * @Date 2022/11/17
 * @Email happysnaker@foxmail.com
 */
public interface PageManager {
    /**
     * 从磁盘中读取一页
     * @param pid 页 ID
     * @return 页
     */
    Page readPage(PageId pid) throws IOException;

    /**
     * 向磁盘中写入页
     * @param page 待写入的页
     */
    void writePage(Page page) throws IOException;

    /**
     * 获取与此类页关联的表
     */
    String getTableName();

    /**
     * 关闭文件通道
     */
    void close();
}
