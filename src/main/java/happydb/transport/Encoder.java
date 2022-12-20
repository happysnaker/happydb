package happydb.transport;


import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import static happydb.common.Database.PHANTOM_NUMBER;
import static happydb.common.Database.VERSION;


/**
 * @author Happysnaker
 * @description
 * @date 2022/3/1
 * @email happysnaker@foxmail.com
 */
public class Encoder extends MessageToByteEncoder {


    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        // Java 默认序列化方式
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream stream = new ObjectOutputStream(byteArrayOutputStream);
        stream.writeObject(msg);
        byte[] bytes = byteArrayOutputStream.toByteArray();
        int len = bytes.length;

        // 幻数 2 字节
        out.writeShort(PHANTOM_NUMBER);

        // 版本 1 字节
        out.writeByte(VERSION);

        // 消息体长度 4 字节
        out.writeInt(len);

        // 一字节反码加法校验和
        byte checkSum = 0;
        checkSum = InverseAddition.inverseSummation(checkSum, PHANTOM_NUMBER);
        checkSum = InverseAddition.inverseSummation(checkSum, VERSION);
        checkSum = InverseAddition.inverseSummation(checkSum, len);
        checkSum = (byte) ~checkSum;
        out.writeByte(checkSum);

        // 序列化后的消息
        out.writeBytes(bytes);
    }
}
