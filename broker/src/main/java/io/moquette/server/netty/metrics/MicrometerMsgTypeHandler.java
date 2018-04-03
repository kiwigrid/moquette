package io.moquette.server.netty.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.moquette.server.netty.NettyUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Shalbanov, Kostiantyn (kostiantyn.shalbanov@intecsoft.de)
 */
public class MicrometerMsgTypeHandler extends ChannelInboundHandlerAdapter {

    private AtomicLong connectedClients;
    private String[] tags;

    public MicrometerMsgTypeHandler(String... tags) {
        connectedClients = Metrics.gauge("mqtt.clients.connections", Tags.of(tags), new AtomicLong());
        this.tags = tags;
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object message) throws Exception {
        MqttMessage msg = (MqttMessage) message;
        MqttMessageType messageType = msg.fixedHeader().messageType();
        switch (messageType) {
            case PUBLISH:
                getPublishCounter(ctx).increment();
                break;
            case SUBSCRIBE:
                getSubscribeCounter(ctx).increment();
                break;
            case CONNECT:
                connectedClients.incrementAndGet();
                break;
            default:
                break;
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        String clientID = NettyUtils.clientID(ctx.channel());
        if (clientID != null && !clientID.isEmpty()) {
            this.connectedClients.decrementAndGet();
        }
        ctx.fireChannelInactive();
    }

    private Counter getSubscribeCounter(ChannelHandlerContext ctx) {
        String clientID = NettyUtils.clientID(ctx.channel());
        String userName = NettyUtils.userName(ctx.channel());
        List<String> listTags = new ArrayList<>(Arrays.asList(tags));
        listTags.add("clientId");
        listTags.add(clientID);
        listTags.add("userName");
        listTags.add(userName);
        return Metrics.counter("mqtt.messages.subscribes", listTags.toArray(new String[listTags.size()]));
    }

    private Counter getPublishCounter(ChannelHandlerContext ctx) {
        String clientID = NettyUtils.clientID(ctx.channel());
        String userName = NettyUtils.userName(ctx.channel());
        List<String> listTags = new ArrayList<>(Arrays.asList(tags));
        listTags.add("clientId");
        listTags.add(clientID);
        listTags.add("userName");
        listTags.add(userName);
        return Metrics.counter("mqtt.messages.publishes", listTags.toArray(new String[listTags.size()]));
    }
}
