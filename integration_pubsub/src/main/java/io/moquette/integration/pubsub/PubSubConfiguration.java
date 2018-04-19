package io.moquette.integration.pubsub;

import javax.annotation.PreDestroy;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.gcp.pubsub.PubSubAdmin;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.cloud.gcp.pubsub.integration.AckMode;
import org.springframework.cloud.gcp.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import org.springframework.cloud.gcp.pubsub.integration.outbound.PubSubMessageHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;

@Configuration
@EnableConfigurationProperties(GooglePubsubProperties.class)
public class PubSubConfiguration {

    private static Logger logger = LoggerFactory.getLogger(PubSubConfiguration.class);

    private PubSubAdmin pubSubAdmin;

    private GooglePubsubProperties googlePubsubProperties;

    public PubSubConfiguration(PubSubAdmin pubSubAdmin,
                               GooglePubsubProperties googlePubsubProperties,
                               @Value("${spring.application.name:dummy}") String applicationName) {
        this.pubSubAdmin = pubSubAdmin;
        this.googlePubsubProperties = googlePubsubProperties;
        subName = String.format("%s-%s", applicationName, UUID.randomUUID());
        kiwiConnectSubName = String.format("%s-%s", applicationName, UUID.randomUUID());
    }

    private String subName;
    private String kiwiConnectSubName;

    @PreDestroy
    public void cleanUp() throws Exception {
        logger.info("remove subscription with name {}", subName);
        pubSubAdmin.deleteSubscription(subName);
        logger.info("remove subscription with name {}", kiwiConnectSubName);
        pubSubAdmin.deleteSubscription(kiwiConnectSubName);
    }

    @Bean("messageChannelAdapter")
    public PubSubInboundChannelAdapter messageChannelAdapter(@Qualifier("integrationInputChannel") SubscribableChannel integrationInputChannel,
                                                             PubSubTemplate pubSubTemplate) {
        logger.info("create Subscription {} {}", subName, googlePubsubProperties.getFromPubsubCloudTopic());
        pubSubAdmin.createSubscription(subName, googlePubsubProperties.getFromPubsubCloudTopic());
        PubSubInboundChannelAdapter adapter = new PubSubInboundChannelAdapter(pubSubTemplate, subName);
        adapter.setOutputChannel(integrationInputChannel);
        adapter.setAckMode(AckMode.AUTO);
        return adapter;
    }

    @Bean("kiwiConnectMessageChannelAdapter")
    public PubSubInboundChannelAdapter kiwiConnectMessageChannelAdapter(@Qualifier("kiwiConnectCloudInputChannel") SubscribableChannel kiwiConnectCloudInputChannel,
                                                                        PubSubTemplate pubSubTemplate) {
        logger.info("create Subscription {} {}", kiwiConnectSubName, googlePubsubProperties.getMqttTopic());
        pubSubAdmin.createSubscription(kiwiConnectSubName, googlePubsubProperties.getMqttTopic());
        PubSubInboundChannelAdapter adapter = new PubSubInboundChannelAdapter(pubSubTemplate, kiwiConnectSubName);
        adapter.setOutputChannel(kiwiConnectCloudInputChannel);
        adapter.setAckMode(AckMode.AUTO);
        return adapter;
    }

/*    @Bean
    @ServiceActivator(inputChannel = "integrationInputChannel")
    public MessageHandler handler() {
        return message -> {
            AckReplyConsumer consumer =
                (AckReplyConsumer) message.getHeaders().get(GcpHeaders.ACKNOWLEDGEMENT);
            consumer.ack();
        };
    }*/

    @Bean
    @ServiceActivator(inputChannel = "integrationOutputChannel")
    public MessageHandler messageSender(PubSubTemplate pubsubTemplate) {
        logger.info("integrationOutputChannel");
        PubSubMessageHandler pubSubMessageHandler = new PubSubMessageHandler(pubsubTemplate, googlePubsubProperties.getToPubsubCloudTopic());
        return pubSubMessageHandler;
    }

    @Bean
    @ServiceActivator(inputChannel = "emLoggerOutputChannel")
    public MessageHandler loggerMessageSender(PubSubTemplate pubsubTemplate) {
        logger.info("emLoggerOutputChannel");
        PubSubMessageHandler pubSubMessageHandler = new PubSubMessageHandler(pubsubTemplate, googlePubsubProperties.getEmLoggerTopic());
        return pubSubMessageHandler;
    }

    @Bean
    @ServiceActivator(inputChannel = "kiwiConnectOutputChannel")
    public MessageHandler kiwiConnectMessageSender(PubSubTemplate pubsubTemplate) {
        PubSubMessageHandler pubSubMessageHandler = new PubSubMessageHandler(pubsubTemplate, googlePubsubProperties.getEmLoggerTopic());
        return pubSubMessageHandler;
    }

}
