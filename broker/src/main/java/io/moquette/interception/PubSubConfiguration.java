package io.moquette.interception;

import javax.annotation.PreDestroy;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.SubscriptionName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.gcp.pubsub.PubSubAdmin;
import org.springframework.cloud.gcp.pubsub.core.PubSubOperations;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.cloud.gcp.pubsub.integration.AckMode;
import org.springframework.cloud.gcp.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import org.springframework.cloud.gcp.pubsub.integration.outbound.PubSubMessageHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.concurrent.ListenableFutureCallback;

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
    }

    @Bean
    public SubscribableChannel integrationInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel integrationOutputChannel() {
        return new DirectChannel();
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
    @ServiceActivator(inputChannel = "integrationOutputChannel")
    public MessageHandler messageSender(PubSubTemplate pubsubTemplate) {
        PubSubMessageHandler pubSubMessageHandler = new PubSubMessageHandler(pubsubTemplate, googlePubsubProperties.getToPubsubCloudTopic());
        pubSubMessageHandler.setPublishCallback(new ListenableFutureCallback<String>() {
            @Override
            public void onFailure(Throwable ex) {
                logger.info("There was an error sending the message.");
            }

            @Override
            public void onSuccess(String result) {
                logger.info("Message was sent successfully.");
            }
        });
        return pubSubMessageHandler;
    }
}
