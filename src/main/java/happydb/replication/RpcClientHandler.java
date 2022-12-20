package happydb.replication;

import happydb.common.Debug;
import happydb.common.Pair;
import happydb.exception.DbException;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@ChannelHandler.Sharable
public class RpcClientHandler extends SimpleChannelInboundHandler<RpcMessage> {

    private final Bootstrap b;
    private final EventLoopGroup group;
    private static final AtomicLong counter = new AtomicLong(0);

    private static volatile RpcClientHandler handler;

    /**
     * 存储信道复用
     */
    private final static Map<Pair<String, Integer>, Channel> CHANNEL_MAP = new ConcurrentHashMap<>();

    /**
     * 存储已发送的消息，并正在等待回复的未来
     */
    private final static Map<Long, CompletableFuture<RpcMessage>> FUTURE_MAP = new ConcurrentHashMap<>();

    /**
     * 单例类，获取单例实例
     *
     * @param b     引导程序
     * @param group 通道
     * @return 此类
     */
    public static RpcClientHandler getInstance(Bootstrap b, EventLoopGroup group) {
        if (handler == null) {
            synchronized (RpcClientHandler.class) {
                if (handler == null) {
                    handler = new RpcClientHandler(b, group);
                    return handler;
                }
            }
        }
        return handler;
    }

    public static RpcClientHandler getRpcClientHandler() {
        return handler;
    }

    private RpcClientHandler(Bootstrap b, EventLoopGroup group) {
        this.b = b;
        this.group = group;
    }

    /**
     * 断开一切连接，关闭通道
     */
    public void stop() {
        if (group != null) {
            group.shutdownGracefully();
        }
        if (CHANNEL_MAP != null) {
            for (Channel value : CHANNEL_MAP.values()) {
                if (value != null && value.isOpen()) {
                    try {
                        Channel channel = value.closeFuture().sync().channel();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcMessage msg) throws Exception {
        Debug.log("Rpc client receive msg " + msg + " from " + ctx.channel().remoteAddress());
        if (FUTURE_MAP.containsKey(msg.getId())) {
            FUTURE_MAP.get(msg.getId()).complete(msg);
            FUTURE_MAP.remove(msg.getId());
        }
    }


    private ChannelFuture connect(String host, int port) {
        return b.connect(host, port);
    }

    /**
     * 连接服务器并发送消息，此方法会自动保存与服务器的 Channel 并复用
     *
     * @param host 域名
     * @param port 断开
     * @param body 消息主体
     * @param type 消息类型
     * @return 返回一个未来，客户端可在此未来上等待返回值
     * @throws DbException 如果通道断开连接
     */
    public CompletableFuture<RpcMessage> connectAndWrite(String host, int port, Object body, int type) throws InterruptedException, DbException {
        var pair = Pair.create(host, port);
        Channel channel;
        if ((channel = CHANNEL_MAP.getOrDefault(pair, null)) != null) {
            if (channel.isActive() && channel.isOpen()) {
                RpcMessage message = RpcMessage.builder()
                        .messageType(type)
                        .body(body)
                        .id(counter.getAndIncrement())
                        .build();
                FUTURE_MAP.put(message.getId(), new CompletableFuture<>());
                try {
                    Debug.log("Rpc client write msg " + message + " to " + channel.remoteAddress());
                    channel.writeAndFlush(message).sync();
                } catch (InterruptedException ignore) {
                    // 忽略掉线程被中断的事实，他必须做完已经决定的事情
                }
                return FUTURE_MAP.get(message.getId());
            } else {
                CHANNEL_MAP.remove(pair);
            }
        }
        ChannelFuture connect = connect(host, port);
        try {
            connect.sync(); // 同步等待建立连接
        } catch (InterruptedException ignore) {
            // 忽略掉线程被中断的事实，他必须做完已经决定的事情
        } catch (Exception e) {
            connect.cancel(true);
            throw new RuntimeException(e);
        }

        CHANNEL_MAP.put(pair, connect.channel());
        return connectAndWrite(host, port, body, type);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
    }
}
