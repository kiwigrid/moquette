package io.moquette.server.netty.metrics;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author Shalbanov, Kostiantyn (kostiantyn.shalbanov@intecsoft.de)
 */
@Configuration
@ConfigurationProperties(prefix = "micrometer")
public class MicrometerConfig {

    private boolean enable = false;

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(final boolean enable) {
        this.enable = enable;
    }
}
