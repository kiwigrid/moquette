package io.moquette.integration.pubsub;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.gcp.pubsub.PubSubAdmin;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.cloud.gcp.pubsub.integration.AckMode;
import org.springframework.cloud.gcp.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import org.springframework.cloud.gcp.pubsub.integration.outbound.PubSubMessageHandler;
import org.springframework.cloud.gcp.pubsub.support.GcpHeaders;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.annotation.PreDestroy;
import java.util.UUID;

@ConditionalOnBean(name = "pubSub")
@EnableAutoConfiguration
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
    }

    private String subName;

    @PreDestroy
    public void cleanUp() throws Exception {
        logger.info("remove subscription with name {}", subName);
        pubSubAdmin.deleteSubscription(subName);
    }

    @Bean
    public PubSubInboundChannelAdapter messageChannelAdapter(SubscribableChannel integrationInputChannel,
                                                             PubSubTemplate pubSubTemplate) {
        pubSubAdmin.createSubscription(subName, googlePubsubProperties.getFromPubsubCloudTopic());
        PubSubInboundChannelAdapter adapter = new PubSubInboundChannelAdapter(pubSubTemplate, subName);
        adapter.setOutputChannel(integrationInputChannel);
        adapter.setAckMode(AckMode.MANUAL);
        return adapter;
    }

    @Bean
    @ServiceActivator(inputChannel = "integrationInputChannel")
    public MessageHandler handler() {
        return message -> {
            AckReplyConsumer consumer =
                (AckReplyConsumer) message.getHeaders().get(GcpHeaders.ACKNOWLEDGEMENT);
            consumer.ack();
        };
    }

    @Bean
    @ServiceActivator(inputChannel = "integrationOutputChannel")
    public MessageHandler messageSender(PubSubTemplate pubsubTemplate) {
        PubSubMessageHandler pubSubMessageHandler = new PubSubMessageHandler(pubsubTemplate, googlePubsubProperties.getToPubsubCloudTopic());
        return pubSubMessageHandler;
    }

    @Bean
    @ServiceActivator(inputChannel = "emLoggerOutputChannel")
    public MessageHandler loggerMessageSender(PubSubTemplate pubsubTemplate) {
        PubSubMessageHandler pubSubMessageHandler = new PubSubMessageHandler(pubsubTemplate, googlePubsubProperties.getEmLoggerTopic());
        return pubSubMessageHandler;
    }
}
