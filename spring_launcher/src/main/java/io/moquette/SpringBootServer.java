package io.moquette;

import io.moquette.interception.InterceptHandler;
import io.moquette.interception.SpringIntegrationInterceptor;
import io.moquette.server.SpringMqttServer;
import io.moquette.server.config.SpringConfig;
import io.moquette.server.netty.SpringNettyAcceptor;
import io.moquette.spi.impl.ProtocolProcessorBootstrapper;
import io.moquette.spi.security.IAuthenticator;
import io.moquette.spi.security.IAuthorizator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Shalbanov, Kostiantyn (kostiantyn.shalbanov@intecsoft.de)
 */
// @Component("ConnectToPubSub")
// @Component("ConnectToRabitMQ")
// @ComponentScan({"io.moquette.integration.rabbitmq", "io.moquette"})
@ComponentScan({"io.moquette.integration.pubsub", "io.moquette"})
@SpringBootApplication
public class SpringBootServer {

    @Autowired
    private SpringIntegrationInterceptor interceptor;

    @Autowired
    private SpringNettyAcceptor acceptor;

    public static void main(String[] args) throws IOException {
        SpringApplication.run(SpringBootServer.class, args);
    }

    @Bean
    CommandLineRunner commandLineRunner(SpringConfig config, IAuthenticator authenticator, IAuthorizator authorizator) {
        return args -> {
            final SpringMqttServer server = new SpringMqttServer(new ProtocolProcessorBootstrapper(), acceptor);
            List<InterceptHandler> handlers = new ArrayList<>();
            interceptor.setPublisher(server::internalPublish);
            handlers.add(interceptor);
            server.startServer(config, handlers, null, authenticator, authorizator);
            System.out.println("Server started, version 0.11-SNAPSHOT");
            //Bind  a shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(server::stopServer));
        };
    }
}
