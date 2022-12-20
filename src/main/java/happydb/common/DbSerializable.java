package happydb.common;

import java.io.Serializable;

/**
 * @Author happysnaker
 * @Date 2022/11/16
 * @Email happysnaker@foxmail.com
 */
public interface DbSerializable extends Serializable {
    /**
     * 将自身转换为在文件中存储的二进制字节数组格式
     * @return 定长的二进制数组
     */
    ByteArray serialized();
}
