package io.moquette.interception;

import io.moquette.interception.messages.InterceptPublishMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.support.MessageBuilder;

import java.nio.charset.Charset;

/**
 * @author Shalbanov, Kostiantyn (kostiantyn.shalbanov@intecsoft.de)
 */
@EnableBinding(Processor.class)
public class SpringCloudInterceptor extends AbstractInterceptHandler {

    private MessageChannel output;

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
        final String msgTxt = msg.getPayload().toString(Charset.defaultCharset());
        System.out.println(msgTxt);
        final Message<String> amqpMsg = MessageBuilder.withPayload(msgTxt).build();
        output.send(amqpMsg);
    }

    @StreamListener(Processor.INPUT)
    public void receive(String msg) {
        System.out.println("Received message is: " + msg);
    }
}
