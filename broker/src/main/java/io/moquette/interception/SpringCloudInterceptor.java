package io.moquette.interception;

import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.server.InternalPublisher;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;

import java.nio.charset.Charset;
import java.util.Map;

/**
 * @author Shalbanov, Kostiantyn (kostiantyn.shalbanov@intecsoft.de)
 */
@EnableBinding(Processor.class)
public class SpringCloudInterceptor extends AbstractInterceptHandler {

    private static Logger logger = LoggerFactory.getLogger(SpringCloudMsg.class);

    private MessageChannel output;
    private InternalPublisher publisher;

    @Override
    public String getID() {
        return this.getClass().getName();
    }

    @Autowired
    public SpringCloudInterceptor (MessageChannel output) {
        this.output = output;
    }

    @Override
    public void onPublish(final InterceptPublishMessage msg) {
        final ByteBuf payload = msg.getPayload();
        if (null != payload) {
            logger.debug("Publishing following message in the cloud: {}", payload.toString(Charset.defaultCharset()));
        } else {
            logger.debug("Publishing 'null' message in the cloud.");
        }
        final Message<SpringCloudMsg> externalMsg = MessageBuilder
            .withPayload(new SpringCloudMsg(msg))
            .build();
        output.send(externalMsg);
    }

    @StreamListener(Processor.INPUT)
    public void receive(@Payload SpringCloudMsg msg, @Headers Map<String, String> headers) {

        final String msgText = new String(msg.getPayload());
        logger.debug("Following message is received from the cloud: {}", msgText);
        if (null == publisher) {
            logger.error("No Publisher defined. Message can't be sent.");
            return;
        }

        MqttQoS qos = MqttQoS.valueOf(msg.getQos());
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, false, 0);
        MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader(msg.getTopic(), 0);
        final ByteBuf payload = Unpooled.wrappedBuffer(msg.getPayload());
        MqttPublishMessage publishMessage = new MqttPublishMessage(fixedHeader, varHeader, payload);

        logger.debug("From the cloud received message is going to be published: {}", msgText);
        publisher.publish(publishMessage, msg.getClientId());
    }

    public void setPublisher(final InternalPublisher publisher) {
        this.publisher = publisher;
    }
}
