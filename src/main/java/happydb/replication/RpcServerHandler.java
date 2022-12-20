package happydb.replication;


import happydb.common.Debug;
import happydb.common.Pair;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author happysnaker
 * @Date 2022/12/10
 * @Email happysnaker@foxmail.com
 */

@ChannelHandler.Sharable
public class RpcServerHandler extends SimpleChannelInboundHandler<RpcMessage> {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        super.exceptionCaught(ctx, cause);
    }


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcMessage msg) throws Exception {
        Debug.log("Rpc server receive " + msg);


        RpcMessage.RpcMessageBuilder builder = RpcMessage.builder();
        builder.id(msg.getId());
        builder.messageType(msg.getMessageType());
        switch (msg.getMessageType()) {
            case RpcMessage.APPEND_LOG_MESSAGE -> {
                builder.body(RaftConfig.self.appendLogEntries((AppendEntriesRequest) msg.getBody()));
            }
            case RpcMessage.REQUEST_VOTE_MESSAGE -> {
                builder.body(RaftConfig.self.requestVote((VoteRequest) msg.getBody()));
            }
            case RpcMessage.REQUEST_COMMIT_MESSAGE -> {
                builder.body(RaftConfig.self.requestCommit((CommitRequest) msg.getBody()));
            }
            default -> {
                ctx.channel().writeAndFlush("pong");
            }
        }
        Debug.log("Rpc server give the response " + builder.build());
        ctx.channel().writeAndFlush(builder.build());
    }
}
