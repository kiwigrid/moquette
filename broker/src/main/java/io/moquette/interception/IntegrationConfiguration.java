package io.moquette.interception;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * @author Shalbanov, Kostiantyn (kostiantyn.shalbanov@intecsoft.de)
 */
@Configuration
public class IntegrationConfiguration {

    @Bean
    public SubscribableChannel integrationInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MessageChannel integrationOutputChannel() {
        return new DirectChannel();
    }

}
