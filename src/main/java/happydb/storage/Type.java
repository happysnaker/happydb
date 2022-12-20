package happydb.storage;

import happydb.common.ByteArray;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;

/**
 * 表示 HelloDB 中类型的类。类型是此类定义的静态对象，任何一种类型都是定长的
 *
 * @Author happysnaker
 * @Date 2022/11/16
 * @Email happysnaker@foxmail.com
 */

public enum Type implements Serializable {
    INT_TYPE() {
        @Override
        public int getLen() {
            return 4;
        }

        @Override
        public Field parse(ByteArray byteAr) throws ParseException {
            return new IntField(byteAr.readInt());
        }
    }, DOUBLE_TYPE {
        @Override
        public int getLen() {
            return 8;
        }

        @Override
        public Field parse(ByteArray byteAr) throws ParseException {
            return new DoubleField(byteAr.readDouble());
        }
    }, STRING_TYPE() {
        @Override
        public int getLen() {
            return STRING_LEN + 4;
        }

        @Override
        public Field parse(ByteArray byteAr) throws ParseException {
            int len = 0;
            ByteArray data = byteAr.readByteArray(getLen());
            try {
                len = data.readInt();
            } catch (Exception e) {
                e.printStackTrace();
            }
            assert len <= STRING_LEN;
            return new StringField(data.readString(len));
        }
    };

    public static final int STRING_LEN = 256;


    /**
     * @return 存储这种类型的字段所需的字节数。
     */
    public abstract int getLen();

    /**
     * 从字节数组当前读取点解析字段，这会导致读取点向后移动 {@link #getLen()} 个长度
     *
     * @param byteAr 字节数组
     * @return 解析出的字段
     * @throws ParseException
     */
    public abstract Field parse(ByteArray byteAr) throws ParseException;
}
