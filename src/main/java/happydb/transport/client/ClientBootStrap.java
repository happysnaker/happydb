package happydb.transport.client;


import happydb.transport.Decoder;
import happydb.transport.Encoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * 客户端启动类
 * @author Happysnaker
 * @description
 * @date 2022/3/3
 * @email happysnaker@foxmail.com
 */
public class ClientBootStrap {


    public static void start(int port) throws Exception {
        start("127.0.0.1", port);
    }

    /**
     * 启动数据库客户端，此方法不会堵塞用户进程
     * @throws Exception
     */
    public static void start(String ip, int port) throws Exception {
        NioEventLoopGroup group = new NioEventLoopGroup();
        SocketAddress socketAddress = new InetSocketAddress(ip, port);
        Bootstrap bootstrap = new Bootstrap();
        SimpleDbClientHandler handler = SimpleDbClientHandler.getInstance();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress(socketAddress)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(new Decoder())
                                .addLast(new Encoder())
                                .addLast(handler);
                    }
                });

        bootstrap.connect().sync();

        new Shell().run();
    }
}
