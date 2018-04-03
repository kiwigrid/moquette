package io.moquette.server.netty.metrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * @author Shalbanov, Kostiantyn (kostiantyn.shalbanov@intecsoft.de)
 */
public class MicrometerBytesHandler extends ChannelDuplexHandler {
    private DistributionSummary readMsgs;
    private DistributionSummary writtenMsgs;

    public MicrometerBytesHandler(final String... tags) {
        this.readMsgs = Metrics.summary("mqtt.messages.bytes.read", tags);
        this.writtenMsgs = Metrics.summary("mqtt.messages.bytes.wrote", tags);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        readMsgs.record(((ByteBuf) msg).readableBytes());
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        writtenMsgs.record(((ByteBuf) msg).readableBytes());
        ctx.write(msg, promise);
    }
}
