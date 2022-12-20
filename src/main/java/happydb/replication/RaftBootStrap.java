package happydb.replication;

import happydb.common.Database;
import happydb.common.Debug;
import happydb.transport.Decoder;
import happydb.transport.Encoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * @Author happysnaker
 * @Date 2022/12/11
 * @Email happysnaker@foxmail.com
 */
public class RaftBootStrap {

    public static void start(int port) throws InterruptedException {
        start("127.0.0.1", port);
    }

    public static void start(String host, int port) throws InterruptedException {
        startRpcClient();
        startRpcServer(host, port);
    }


    /**
     * 作为 RPC 服务提供方开始监听客户端的连接，此方法不会堵塞主线程
     *
     * @throws InterruptedException
     */
    private static void startRpcServer(String ip, int port) throws InterruptedException {
        Database.service.schedule(new Runnable() {
            @Override
            public void run() {
                NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
                NioEventLoopGroup workerGroup = new NioEventLoopGroup();
                SocketAddress socketAddress = new InetSocketAddress(ip, port);

                ServerBootstrap bootstrap = new ServerBootstrap();
                bootstrap.group(bossGroup, workerGroup)
                        .channel(NioServerSocketChannel.class)
                        .localAddress(socketAddress)
                        .childHandler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            public void initChannel(SocketChannel ch) throws Exception {
                                ch.pipeline()
                                        .addLast(new IdleStateHandler(5, 0, 0, TimeUnit.SECONDS))
                                        .addLast(new Decoder())
                                        .addLast(new Encoder())
                                        .addLast(new RpcServerHandler());
                            }
                        });

                ChannelFuture sync = null;
                try {
                    sync = bootstrap.bind().sync();
                    Debug.log("Raft rpc server start on " + socketAddress);
                    sync.channel().closeFuture().sync();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } finally {
                    Debug.log("Raft server stop.");
                    workerGroup.shutdownGracefully();
                    bossGroup.shutdownGracefully();
                }
            }
        }, 0, TimeUnit.MILLISECONDS);
    }


    /**
     * 创建 Netty 客户端，负责与 RPC 服务提供方通信
     */
    private static void startRpcClient() {
        Bootstrap b = new Bootstrap();
        EventLoopGroup group = new NioEventLoopGroup();
        RpcClientHandler handler = RpcClientHandler.getInstance(b, group);
        b.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(new Decoder())
                                .addLast(new Encoder())
                                .addLast(handler);
                    }
                });
    }
}
