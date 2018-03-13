package io.moquette;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.moquette.interception.InterceptHandler;
import io.moquette.interception.SpringCloudInterceptor;
import io.moquette.server.SpringMqttServer;
import io.moquette.server.config.SpringConfig;
import io.moquette.server.netty.NettyAcceptor;
import io.moquette.spi.impl.ProtocolProcessorBootstrapper;
import io.moquette.spi.security.IAuthenticator;
import io.moquette.spi.security.IAuthorizator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SpringBootServer {

    public static void main(String[] args) throws IOException {

        SpringApplication.run(SpringBootServer.class, args);
    }
    @Autowired
    private SpringCloudInterceptor interceptor;

    @Bean
    CommandLineRunner commandLineRunner(SpringConfig config, IAuthenticator authenticator, IAuthorizator authorizator) {
        return args -> {
            ProtocolProcessorBootstrapper protocolProcessorBootstrapper = new ProtocolProcessorBootstrapper();
            NettyAcceptor acceptor = new NettyAcceptor();
            final SpringMqttServer server = new SpringMqttServer(protocolProcessorBootstrapper, acceptor);
            List<InterceptHandler> handlers = new ArrayList<>();
            interceptor.setPublisher(server::internalPublish);
            handlers.add(interceptor);
            server.startServer(config, handlers, null, authenticator, authorizator);
            System.out.println("Server started, version 0.11-SNAPSHOT");
            //Bind  a shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(server::stopServer));
        };
    }

    public void setInterceptor(final SpringCloudInterceptor interceptor) {
        this.interceptor = interceptor;
    }
}
