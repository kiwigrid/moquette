package io.moquette.server.config;

import io.moquette.server.netty.metrics.MicrometerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component
@EnableConfigurationProperties({MicrometerConfig.class})
public class SpringConfig extends IConfig {

    private static Logger logger = LoggerFactory.getLogger(SpringConfig.class);

    private Environment environment;

    public SpringConfig(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void setProperty(String name, String value) {
    }

    @Override
    public String getProperty(String name) {
        logger.info("getProperty {}", name);
        return environment.getProperty(name);
    }

    @Override
    public String getProperty(String name, String defaultValue) {
        logger.info("getProperty {} {}", name, defaultValue);
        return environment.getProperty(name, defaultValue);
    }

    @Override
    public IResourceLoader getResourceLoader() {
        return null;
    }
}
