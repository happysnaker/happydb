package happydb.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.List;

import static happydb.common.Database.PHANTOM_NUMBER;

/**
 * @author Happysnaker
 * @description
 * @date 2022/3/1
 * @email happysnaker@foxmail.com
 */
public class Decoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        synchronized (Decoder.class) {
            // 幻数 2 字节， 版本 1 字节，长度 4 字节，校验和 1 字节，首部 8 字节
            if (in.readableBytes() > 8) {
                short p = in.readShort();
                // 如果幻数相等则进一步读取，如果不等说明这是垃圾信息，直接丢弃
                if (p == PHANTOM_NUMBER) {
                    // 版本
                    byte version = in.readByte();
                    int len = in.readInt();

                    // 1 字节校验和
                    byte checkSum = in.readByte();
                    checkSum = InverseAddition.inverseSummation(checkSum, PHANTOM_NUMBER);
                    checkSum = InverseAddition.inverseSummation(checkSum, version);
                    checkSum = InverseAddition.inverseSummation(checkSum, len);
                    checkSum = (byte) ~checkSum;

                    // 验证通过
                    if (checkSum == 0) {

                        // 防止拆包导致数量不够
                        if (in.readableBytes() < len) {
                            in.resetReaderIndex();
                            return;
                        }

                        byte[] bytes = new byte[len];
                        // 读取 len 个字节，避免粘包问题
                        for (int i = 0; i < len; i++) {
                            bytes[i] = in.readByte();
                        }
                        // Java 默认序列化
                        // 这里可以基于策略模式采用其他序列化方式
                        ObjectInputStream inputStream =
                                new ObjectInputStream(new ByteArrayInputStream(bytes));
                        Object o = inputStream.readObject();
                        out.add(o);
                    }
                }
            }
        }
    }
}
