package io.moquette.server.netty.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.moquette.server.netty.NettyUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageType;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Shalbanov, Kostiantyn (kostiantyn.shalbanov@intecsoft.de)
 */
public class MicrometerMsgTypeHandler extends ChannelInboundHandlerAdapter {
    private Counter subscribes;
    private Counter publishes;
    private AtomicLong connectedClients;

    public MicrometerMsgTypeHandler(String... tags) {
        subscribes = Metrics.counter("mqtt.messages.subscribes", Tags.of(tags));
        publishes = Metrics.counter("mqtt.messages.publishes", Tags.of(tags));
        connectedClients = Metrics.gauge("mqtt.clients.connections", Tags.of(tags), new AtomicLong());
    }


    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object message) throws Exception {
        MqttMessage msg = (MqttMessage) message;
        MqttMessageType messageType = msg.fixedHeader().messageType();
        switch (messageType) {
            case PUBLISH:
                publishes.increment();
                break;
            case SUBSCRIBE:
                subscribes.increment();
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
}
