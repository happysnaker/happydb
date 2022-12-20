package happydb.transport.client;



import happydb.SqlMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * 此处理程序用以处理与注册中心通信
 * @author Happysnaker
 * @description
 * @date 2022/3/2
 * @email happysnaker@foxmail.com
 */
@ChannelHandler.Sharable
public class SimpleDbClientHandler extends SimpleChannelInboundHandler<SqlMessage> {
    private SimpleDbClientHandler(){
    };

    static CompletableFuture<SqlMessage> response;

    /**
     * 单例类
     * @return
     */
    public static SimpleDbClientHandler getInstance() {
        return INSTANCE;
    }

    static SimpleDbClientHandler INSTANCE = new SimpleDbClientHandler();

    private ChannelHandlerContext ctx;


    public Channel getChannel() {
        return ctx.channel();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    /**
     * 复用通道发送消息，客户端与数据库的通信总是复用此连接
     * <p>当连接断开时，此方法会自动尝试重新连接</p>
     * @param message
     * @throws InterruptedException
     */
    public SqlMessage sendMessage(SqlMessage message) throws InterruptedException, ExecutionException {
        if (!ctx.channel().isOpen() || !ctx.channel().isActive()) {
            SocketAddress address = new InetSocketAddress(2048);
            ctx.channel().connect(address).sync();
        }
        // 同一个客户端总是会串行发起通信，这里用一个 CompletableFuture 同步接受消息
        response = new CompletableFuture<>();
        ctx.channel().writeAndFlush(message).sync();
        return response.get();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SqlMessage msg) {
        response.complete(msg);
    }


    public void close()  {
        try {
            ctx.channel().close().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
