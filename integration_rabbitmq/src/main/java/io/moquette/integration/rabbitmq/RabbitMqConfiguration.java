package io.moquette.integration.rabbitmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.inbound.AmqpInboundGateway;
import org.springframework.integration.amqp.outbound.AmqpOutboundEndpoint;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

@Configuration
public class RabbitMqConfiguration {

    private static Logger logger = LoggerFactory.getLogger(RabbitMqConfiguration.class);

    @Bean
    public AmqpInboundGateway inbound(SimpleMessageListenerContainer listenerContainer,
                                      @Qualifier("integrationInputChannel") MessageChannel channel) {
        AmqpInboundGateway gateway = new AmqpInboundGateway(listenerContainer);
        gateway.setRequestChannel(channel);
        // TODO: "foo" in der Datei konfigurierbar machen. Soll immer eine Queue und 1 Exchange verwendet werden
        gateway.setDefaultReplyTo("foo");
        return gateway;
    }

    @Bean
    public SimpleMessageListenerContainer container(ConnectionFactory connectionFactory) {
        SimpleMessageListenerContainer container =
            new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames("foo");
        // container.setConcurrentConsumers(2);
        // ...
        return container;
    }

    @Bean
    @ServiceActivator(inputChannel = "integrationInputChannel")
    public MessageHandler handler() {
        return new AbstractReplyProducingMessageHandler() {

            @Override
            protected Object handleRequestMessage(Message<?> requestMessage) {
                return "reply to " + requestMessage.getPayload();
            }

        };
    }

    @Bean
    @ServiceActivator(inputChannel = "integrationOutputChannel")
    public AmqpOutboundEndpoint amqpOutbound(AmqpTemplate amqpTemplate) {
        AmqpOutboundEndpoint outbound = new AmqpOutboundEndpoint(amqpTemplate);
        outbound.setRoutingKey("foo"); // default exchange - route to queue 'foo'
        return outbound;
    }
}
