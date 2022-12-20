package happydb.transport.server;

import happydb.transport.Decoder;
import happydb.transport.Encoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * @author Happysnaker
 * @description
 * @date 2022/5/16
 * @email happysnaker@foxmail.com
 */
public class ServerBootStrap {

    /**
     * 启动服务，此方法会堵塞运行
     */
    public static void start(String address, int port) throws InterruptedException {
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        SocketAddress socketAddress = new InetSocketAddress(address, port);
        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(socketAddress)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline()
                                    .addLast(new Decoder())
                                    .addLast(new Encoder())
                                    .addLast(new SimpleDbServerHandler());
                        }
                    });

            ChannelFuture f = bootstrap.bind().sync();
            System.out.println("server start in " + f.channel().localAddress());
            f.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully().sync();
        }
    }
}
