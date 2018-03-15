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
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.Charset;

@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class SpringIntegrationInterceptor extends AbstractInterceptHandler
                                            implements MessageHandler {

    private static Logger logger = LoggerFactory.getLogger(SpringIntegrationInterceptor.class);

    private InternalPublisher publisher;

    private final SubscribableChannel integrationInputChannel;

    private final MessageChannel integrationOutputChannel;

    @PostConstruct
    public void init(){
        integrationInputChannel.subscribe(this);
    }

    @Override
    public String getID() {
        return this.getClass().getName();
    }

    @Override
    public void onPublish(final InterceptPublishMessage msg) {
        final ByteBuf payload = msg.getPayload();
        if (null != payload) {
            logger.info("Publishing following message in the cloud: {}", payload.toString(Charset.defaultCharset()));
        } else {
            logger.info("Publishing 'null' message in the cloud.");
        }

        final Message<String> externalMsg = MessageBuilder
            .withPayload(msg.getPayload().toString(Charset.forName("UTF8")))
            .setHeader("userName",msg.getUsername())
            .build();

        integrationOutputChannel.send(externalMsg);
    }

    public void setPublisher(final InternalPublisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public void handleMessage(Message<?> message) throws MessagingException {
        logger.debug("Message arrived! Payload: " + message.getPayload());
        MqttQoS qos = MqttQoS.valueOf(1);
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, qos, false, 0);
        //TODO: /out vllt. konfigurierbar machen, oder anpassen
        MqttPublishVariableHeader varHeader = new MqttPublishVariableHeader("/out", 0);
        final ByteBuf payload = Unpooled.wrappedBuffer(message.getPayload().toString().getBytes());
        MqttPublishMessage publishMessage = new MqttPublishMessage(fixedHeader, varHeader, payload);

        logger.info("From the cloud received message is going to be published: {}", "");
        //TODO: test ändern. Vllt. konfigurierbar machen.
        publisher.publish(publishMessage, "test");
    }
}
