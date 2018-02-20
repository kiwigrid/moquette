package io.moquette.server;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ITopic;
import io.moquette.BrokerConstants;
import io.moquette.connections.IConnectionsManager;
import io.moquette.interception.HazelcastInterceptHandler;
import io.moquette.interception.HazelcastMsg;
import io.moquette.interception.InterceptHandler;
import io.moquette.server.config.IConfig;
import io.moquette.spi.impl.ProtocolProcessor;
import io.moquette.spi.impl.ProtocolProcessorBootstrapper;
import io.moquette.spi.impl.subscriptions.Subscription;
import io.moquette.spi.security.IAuthenticator;
import io.moquette.spi.security.IAuthorizator;
import io.moquette.spi.security.ISslContextCreator;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.moquette.logging.LoggingUtils.getInterceptorIds;

public class SpringMqttServer implements IServer {
    private static final Logger LOG = LoggerFactory.getLogger(SpringMqttServer.class);

    private static final String HZ_INTERCEPT_HANDLER = HazelcastInterceptHandler.class.getCanonicalName();

    private ServerAcceptor serverAcceptor;

    private volatile boolean m_initialized;

    private ProtocolProcessor protocolProcessor;

    private HazelcastInstance hazelcastInstance;

    private ProtocolProcessorBootstrapper protocolProcessorBootstrapper;

    private ScheduledExecutorService scheduler;

    public SpringMqttServer(ProtocolProcessorBootstrapper protocolProcessorBootstrapper, ServerAcceptor serverAcceptor){
        this.protocolProcessorBootstrapper = protocolProcessorBootstrapper;
        this.serverAcceptor = serverAcceptor;
    }

    public void startServer(IConfig config, List<? extends InterceptHandler> handlers, ISslContextCreator sslCtxCreator,
                            IAuthenticator authenticator, IAuthorizator authorizator) throws IOException {
        if (handlers == null) {
            handlers = Collections.emptyList();
        }
        LOG.info("Starting Moquette Server. MQTT message interceptors={}", getInterceptorIds(handlers));

        scheduler = Executors.newScheduledThreadPool(1);

        final String handlerProp = System.getProperty(BrokerConstants.INTERCEPT_HANDLER_PROPERTY_NAME);
        if (handlerProp != null) {
            config.setProperty(BrokerConstants.INTERCEPT_HANDLER_PROPERTY_NAME, handlerProp);
        }
        configureCluster(config);
        final String persistencePath = config.getProperty(BrokerConstants.PERSISTENT_STORE_PROPERTY_NAME);
        LOG.info("Configuring Using persistent store file, path={}", persistencePath);

        final ProtocolProcessor processor = protocolProcessorBootstrapper.init(config, handlers, authenticator, authorizator,
                                                                               this);
        LOG.info("Initialized MQTT protocol processor");
        if (sslCtxCreator == null) {
            LOG.warn("Using default SSL context creator");
            sslCtxCreator = new DefaultMoquetteSslContextCreator(config);
        }

        LOG.info("Binding server to the configured ports");

        serverAcceptor.initialize(processor, config, sslCtxCreator);
        protocolProcessor = processor;

        LOG.info("Moquette server has been initialized successfully");
        m_initialized = true;
    }

    private void configureCluster(IConfig config) throws FileNotFoundException {
        LOG.info("Configuring embedded Hazelcast instance");
        String interceptHandlerClassname = config.getProperty(BrokerConstants.INTERCEPT_HANDLER_PROPERTY_NAME);
        if (interceptHandlerClassname == null || !HZ_INTERCEPT_HANDLER.equals(interceptHandlerClassname)) {
            LOG.info("There are no Hazelcast intercept handlers. The server won't start a Hazelcast instance.");
            return;
        }
        String hzConfigPath = config.getProperty(BrokerConstants.HAZELCAST_CONFIGURATION);
        if (hzConfigPath != null) {
            boolean isHzConfigOnClasspath = this.getClass().getClassLoader().getResource(hzConfigPath) != null;
            Config hzconfig = isHzConfigOnClasspath
                ? new ClasspathXmlConfig(hzConfigPath)
                : new FileSystemXmlConfig(hzConfigPath);
            LOG.info("Starting Hazelcast instance. ConfigurationFile={}", hzconfig);
            hazelcastInstance = Hazelcast.newHazelcastInstance(hzconfig);
        } else {
            LOG.info("Starting Hazelcast instance with default configuration");
            hazelcastInstance = Hazelcast.newHazelcastInstance();
        }
        listenOnHazelCastMsg();
    }

    private void listenOnHazelCastMsg() {

        //TODO
        LOG.info("Subscribing to Hazelcast topic. TopicName={}", "moquette");
        HazelcastInstance hz = getHazelcastInstance();
        ITopic<HazelcastMsg> topic = hz.getTopic("moquette");
        //topic.addMessageListener(new HazelcastListener(this));
    }

    @Override
    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    /**
     * Use the broker to publish a message. It's intended for embedding applications. It can be used
     * only after the server is correctly started with startServer.
     *
     * @param msg
     *            the message to forward.
     * @param clientId
     *            the id of the sending server.
     * @throws IllegalStateException
     *             if the server is not yet started
     */
    public void internalPublish(MqttPublishMessage msg, final String clientId) {
        final int messageID = msg.variableHeader().messageId();
        if (!m_initialized) {
            LOG.error("Moquette is not started, internal message cannot be published. CId={}, messageId={}", clientId,
                      messageID);
            throw new IllegalStateException("Can't publish on a server is not yet started");
        }
        LOG.debug("Publishing message. CId={}, messageId={}", clientId, messageID);
        protocolProcessor.internalPublish(msg, clientId);
    }

    public void stopServer() {
        LOG.info("Unbinding server from the configured ports");
        serverAcceptor.close();
        LOG.trace("Stopping MQTT protocol processor");
        protocolProcessorBootstrapper.shutdown();
        m_initialized = false;
        if (hazelcastInstance != null) {
            LOG.trace("Stopping embedded Hazelcast instance");
            try {
                hazelcastInstance.shutdown();
            } catch (HazelcastInstanceNotActiveException e) {
                LOG.warn("embedded Hazelcast instance is already shut down.");
            }
        }

        scheduler.shutdown();

        LOG.info("Moquette server has been stopped.");
    }

    /**
     * SPI method used by Broker embedded applications to get list of subscribers. Returns null if
     * the broker is not started.
     *
     * @return list of subscriptions.
     */
    public List<Subscription> getSubscriptions() {
        if (protocolProcessorBootstrapper == null) {
            return null;
        }
        return protocolProcessorBootstrapper.getSubscriptions();
    }

    /**
     * SPI method used by Broker embedded applications to add intercept handlers.
     *
     * @param interceptHandler
     *            the handler to add.
     */
    public void addInterceptHandler(InterceptHandler interceptHandler) {
        if (!m_initialized) {
            LOG.error("Moquette is not started, MQTT message interceptor cannot be added. InterceptorId={}",
                      interceptHandler.getID());
            throw new IllegalStateException("Can't register interceptors on a server that is not yet started");
        }
        LOG.info("Adding MQTT message interceptor. InterceptorId={}", interceptHandler.getID());
        protocolProcessor.addInterceptHandler(interceptHandler);
    }

    /**
     * SPI method used by Broker embedded applications to remove intercept handlers.
     *
     * @param interceptHandler
     *            the handler to remove.
     */
    public void removeInterceptHandler(InterceptHandler interceptHandler) {
        if (!m_initialized) {
            LOG.error("Moquette is not started, MQTT message interceptor cannot be removed. InterceptorId={}",
                      interceptHandler.getID());
            throw new IllegalStateException("Can't deregister interceptors from a server that is not yet started");
        }
        LOG.info("Removing MQTT message interceptor. InterceptorId={}", interceptHandler.getID());
        protocolProcessor.removeInterceptHandler(interceptHandler);
    }

    /**
     * Returns the connections manager of this broker.
     *
     * @return IConnectionsManager the instance used bt the broker.
     */
    public IConnectionsManager getConnectionsManager() {
        return protocolProcessorBootstrapper.getConnectionDescriptors();
    }

    @Override
    public ProtocolProcessor getProcessor() {
        return protocolProcessor;
    }

    @Override
    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }
}
