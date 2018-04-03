package io.moquette.server.netty.metrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.moquette.server.netty.NettyUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Shalbanov, Kostiantyn (kostiantyn.shalbanov@intecsoft.de)
 */
public class MicrometerMessageHandler extends ChannelDuplexHandler {

    private String[] tags;

    public MicrometerMessageHandler(final String... tags) {
        this.tags = tags;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        String clientID = NettyUtils.clientID(ctx.channel());
        String userName = NettyUtils.userName(ctx.channel());
        if (userName != null && clientID != null) {
            List<String> listTags = new ArrayList<>(Arrays.asList(tags));
            listTags.add("clientId");
            listTags.add(clientID);
            listTags.add("userName");
            listTags.add(userName);
            Metrics.counter("mqtt.messages.read", listTags.toArray(new String[listTags.size()])).increment();
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        String clientID = NettyUtils.clientID(ctx.channel());
        String userName = NettyUtils.userName(ctx.channel());
        List<String> listTags = new ArrayList<>(Arrays.asList(tags));
        listTags.add("clientId");
        listTags.add(clientID);
        listTags.add("userName");
        listTags.add(userName);
        Metrics.counter("mqtt.messages.wrote", listTags.toArray(new String[listTags.size()])).increment();
        ctx.write(msg, promise);
    }
}
