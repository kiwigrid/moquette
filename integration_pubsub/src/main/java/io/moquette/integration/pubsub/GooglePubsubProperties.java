package io.moquette.integration.pubsub;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties("google.pubsub")
public class GooglePubsubProperties {
    private String toPubsubCloudTopic = "legacy-kiwibus-to-cloud";
    private String fromPubsubCloudTopic = "legacy-kiwibus-from-cloud";
    private String emLoggerTopic = "em-log";

}
