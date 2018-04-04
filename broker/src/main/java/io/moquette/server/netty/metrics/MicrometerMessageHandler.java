package io.moquette.server.netty.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * @author Shalbanov, Kostiantyn (kostiantyn.shalbanov@intecsoft.de)
 */
public class MicrometerMessageHandler extends ChannelDuplexHandler {

    private Counter readMsgs;
    private Counter writtenMsgs;

    public MicrometerMessageHandler(final String... tags) {
        this.readMsgs = Metrics.counter("mqtt.messages.read", tags);
        this.writtenMsgs = Metrics.counter("mqtt.messages.wrote", tags);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        readMsgs.increment();
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        writtenMsgs.increment();
        ctx.write(msg, promise);
    }
}
