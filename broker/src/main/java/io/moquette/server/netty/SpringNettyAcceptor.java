package io.moquette.server.netty;

import io.moquette.BrokerConstants;
import io.moquette.server.ServerAcceptor;
import io.moquette.server.config.IConfig;
import io.moquette.server.netty.metrics.BytesMetrics;
import io.moquette.server.netty.metrics.BytesMetricsCollector;
import io.moquette.server.netty.metrics.BytesMetricsHandler;
import io.moquette.server.netty.metrics.DropWizardMetricsHandler;
import io.moquette.server.netty.metrics.MQTTMessageLogger;
import io.moquette.server.netty.metrics.MessageMetrics;
import io.moquette.server.netty.metrics.MessageMetricsCollector;
import io.moquette.server.netty.metrics.MessageMetricsHandler;
import io.moquette.server.netty.metrics.MicrometerBytesHandler;
import io.moquette.server.netty.metrics.MicrometerConfig;
import io.moquette.server.netty.metrics.MicrometerMessageHandler;
import io.moquette.server.netty.metrics.MicrometerMsgTypeHandler;
import io.moquette.spi.impl.ProtocolProcessor;
import io.moquette.spi.security.ISslContextCreator;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.moquette.BrokerConstants.BUGSNAG_ENABLE_PROPERTY_NAME;
import static io.moquette.BrokerConstants.DISABLED_PORT_BIND;
import static io.moquette.BrokerConstants.METRICS_ENABLE_PROPERTY_NAME;
import static io.moquette.BrokerConstants.PORT_PROPERTY_NAME;
import static io.moquette.BrokerConstants.SSL_PORT_PROPERTY_NAME;
import static io.moquette.BrokerConstants.WEB_SOCKET_PORT_PROPERTY_NAME;
import static io.moquette.BrokerConstants.WSS_PORT_PROPERTY_NAME;

/**
 * @author Shalbanov, Kostiantyn (kostiantyn.shalbanov@intecsoft.de)
 */
@Component
public class SpringNettyAcceptor implements ServerAcceptor {

    private static final String MQTT_SUBPROTOCOL_CSV_LIST = "mqtt, mqttv3.1, mqttv3.1.1";

    @Autowired
    private MicrometerConfig micrometerConfig;

    abstract class PipelineInitializer {

        abstract void init(ChannelPipeline pipeline) throws Exception;
    }

    private static final Logger LOG = LoggerFactory.getLogger(NettyAcceptor.class);

    EventLoopGroup m_bossGroup;
    EventLoopGroup m_workerGroup;
    BytesMetricsCollector m_bytesMetricsCollector = new BytesMetricsCollector();
    MessageMetricsCollector m_metricsCollector = new MessageMetricsCollector();
    private Optional<? extends ChannelInboundHandler> metrics;
    private Optional<? extends ChannelInboundHandler> errorsCather;

    private int nettySoBacklog;
    private boolean nettySoReuseaddr;
    private boolean nettyTcpNodelay;
    private boolean nettySoKeepalive;
    private int nettyChannelTimeoutSeconds;
    private int maxBytesInMessage;

    private Class<? extends ServerSocketChannel> channelClass;

    @Override
    public void initialize(ProtocolProcessor processor, IConfig props, ISslContextCreator sslCtxCreator)
        throws IOException {
        LOG.info("Initializing Netty acceptor...");

        nettySoBacklog = Integer.parseInt(props.getProperty(BrokerConstants.NETTY_SO_BACKLOG_PROPERTY_NAME, "128"));
        nettySoReuseaddr = Boolean
            .parseBoolean(props.getProperty(BrokerConstants.NETTY_SO_REUSEADDR_PROPERTY_NAME, "true"));
        nettyTcpNodelay = Boolean
            .parseBoolean(props.getProperty(BrokerConstants.NETTY_TCP_NODELAY_PROPERTY_NAME, "true"));
        nettySoKeepalive = Boolean
            .parseBoolean(props.getProperty(BrokerConstants.NETTY_SO_KEEPALIVE_PROPERTY_NAME, "true"));
        nettyChannelTimeoutSeconds = Integer
            .parseInt(props.getProperty(BrokerConstants.NETTY_CHANNEL_TIMEOUT_SECONDS_PROPERTY_NAME, "10"));
        maxBytesInMessage = Integer
            .parseInt(props.getProperty(BrokerConstants.NETTY_MAX_BYTES_PROPERTY_NAME,
                String.valueOf(BrokerConstants.DEFAULT_NETTY_MAX_BYTES_IN_MESSAGE)));

        boolean epoll = Boolean.parseBoolean(props.getProperty(BrokerConstants.NETTY_EPOLL_PROPERTY_NAME, "false"));
        if (epoll) {
            LOG.info("Netty is using Epoll");
            m_bossGroup = new EpollEventLoopGroup();
            m_workerGroup = new EpollEventLoopGroup();
            channelClass = EpollServerSocketChannel.class;
        } else {
            LOG.info("Netty is using NIO");
            m_bossGroup = new NioEventLoopGroup();
            m_workerGroup = new NioEventLoopGroup();
            channelClass = NioServerSocketChannel.class;
        }

        final NettyMQTTHandler mqttHandler = new NettyMQTTHandler(processor);

        final boolean useFineMetrics = Boolean.parseBoolean(props.getProperty(METRICS_ENABLE_PROPERTY_NAME, "true"));
        if (useFineMetrics) {
            DropWizardMetricsHandler metricsHandler = new DropWizardMetricsHandler();
            metricsHandler.init(props);
            this.metrics = Optional.of(metricsHandler);
        } else {
            this.metrics = Optional.empty();
        }

        final boolean useBugSnag = Boolean.parseBoolean(props.getProperty(BUGSNAG_ENABLE_PROPERTY_NAME, "false"));
        if (useBugSnag) {
            BugSnagErrorsHandler bugSnagHandler = new BugSnagErrorsHandler();
            bugSnagHandler.init(props);
            this.errorsCather = Optional.of(bugSnagHandler);
        } else {
            this.errorsCather = Optional.empty();
        }
        initializePlainTCPTransport(mqttHandler, props);
        initializeWebSocketTransport(mqttHandler, props);
        String sslTcpPortProp = props.getProperty(BrokerConstants.SSL_PORT_PROPERTY_NAME);
        String wssPortProp = props.getProperty(BrokerConstants.WSS_PORT_PROPERTY_NAME);
        if (sslTcpPortProp != null || wssPortProp != null) {
            SSLContext sslContext = sslCtxCreator.initSSLContext();
            if (sslContext == null) {
                LOG.error("Can't initialize SSLHandler layer! Exiting, check your configuration of jks");
                return;
            }
            initializeSSLTCPTransport(mqttHandler, props, sslContext);
            initializeWSSTransport(mqttHandler, props, sslContext);
        }
    }

    private void initFactory(String host, int port, String protocol, final SpringNettyAcceptor.PipelineInitializer pipeliner) {
        LOG.info("Initializing server. Protocol={}", protocol);
        ServerBootstrap b = new ServerBootstrap();
        b.group(m_bossGroup, m_workerGroup).channel(channelClass)
            .childHandler(new ChannelInitializer<SocketChannel>() {

                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();
                    try {
                        pipeliner.init(pipeline);
                    } catch (Throwable th) {
                        LOG.error("Severe error during pipeline creation", th);
                        throw th;
                    }
                }
            }).option(ChannelOption.SO_BACKLOG, nettySoBacklog).option(ChannelOption.SO_REUSEADDR, nettySoReuseaddr)
            .option(ChannelOption.TCP_NODELAY, nettyTcpNodelay)
            .childOption(ChannelOption.SO_KEEPALIVE, nettySoKeepalive);
        try {
            LOG.info("Binding server. host={}, port={}", host, port);
            // Bind and start to accept incoming connections.
            ChannelFuture f = b.bind(host, port);
            LOG.info("Server has been bound. host={}, port={}", host, port);
            f.sync();
        } catch (InterruptedException ex) {
            LOG.error("An interruptedException was caught while initializing server. Protocol={}", protocol, ex);
        }
    }

    private void initializePlainTCPTransport(final NettyMQTTHandler handler,
                                             IConfig props) throws IOException {
        LOG.info("Configuring TCP MQTT transport");
        final MoquetteIdleTimeoutHandler timeoutHandler = new MoquetteIdleTimeoutHandler();
        String host = props.getProperty(BrokerConstants.HOST_PROPERTY_NAME);
        String tcpPortProp = props.getProperty(PORT_PROPERTY_NAME, DISABLED_PORT_BIND);
        if (DISABLED_PORT_BIND.equals(tcpPortProp)) {
            LOG.info("Property {} has been set to {}. TCP MQTT will be disabled", BrokerConstants.PORT_PROPERTY_NAME,
                DISABLED_PORT_BIND);
            return;
        }
        int port = Integer.parseInt(tcpPortProp);
        initFactory(host, port, "TCP MQTT", new SpringNettyAcceptor.PipelineInitializer() {

            @Override
            void init(ChannelPipeline pipeline) {
                pipeline.addFirst("idleStateHandler", new IdleStateHandler(nettyChannelTimeoutSeconds, 0, 0));
                pipeline.addAfter("idleStateHandler", "idleEventHandler", timeoutHandler);
                // pipeline.addLast("logger", new LoggingHandler("Netty", LogLevel.ERROR));
                if (errorsCather.isPresent()) {
                    pipeline.addLast("bugsnagCatcher", errorsCather.get());
                }
                pipeline.addFirst("bytemetrics", new BytesMetricsHandler(m_bytesMetricsCollector));
                pipeline.addLast("decoder", new MqttDecoder(maxBytesInMessage));
                pipeline.addLast("encoder", MqttEncoder.INSTANCE);
                pipeline.addLast("metrics", new MessageMetricsHandler(m_metricsCollector));
                pipeline.addLast("messageLogger", new MQTTMessageLogger());
                if (metrics.isPresent()) {
                    pipeline.addLast("wizardMetrics", metrics.get());
                }
                addMicrometerHandlers(pipeline, "tcp");

                pipeline.addLast("handler", handler);
            }
        });
    }

    private void initializeWebSocketTransport(final NettyMQTTHandler handler, IConfig props) throws IOException {
        LOG.info("Configuring Websocket MQTT transport");
        String webSocketPortProp = props.getProperty(WEB_SOCKET_PORT_PROPERTY_NAME, DISABLED_PORT_BIND);
        if (DISABLED_PORT_BIND.equals(webSocketPortProp)) {
            // Do nothing no WebSocket configured
            LOG.info("Property {} has been setted to {}. Websocket MQTT will be disabled",
                BrokerConstants.WEB_SOCKET_PORT_PROPERTY_NAME, DISABLED_PORT_BIND);
            return;
        }
        int port = Integer.parseInt(webSocketPortProp);

        final MoquetteIdleTimeoutHandler timeoutHandler = new MoquetteIdleTimeoutHandler();

        String host = props.getProperty(BrokerConstants.HOST_PROPERTY_NAME);
        initFactory(host, port, "Websocket MQTT", new SpringNettyAcceptor.PipelineInitializer() {

            @Override
            void init(ChannelPipeline pipeline) {

                pipeline.addLast(new HttpServerCodec());
                pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
                pipeline.addLast("webSocketHandler",
                    new WebSocketServerProtocolHandler("/mqtt", MQTT_SUBPROTOCOL_CSV_LIST));
                pipeline.addLast("ws2bytebufDecoder", new NettyAcceptor.WebSocketFrameToByteBufDecoder());
                pipeline.addLast("bytebuf2wsEncoder", new NettyAcceptor.ByteBufToWebSocketFrameEncoder());
                pipeline.addFirst("idleStateHandler", new IdleStateHandler(nettyChannelTimeoutSeconds, 0, 0));
                pipeline.addAfter("idleStateHandler", "idleEventHandler", timeoutHandler);
                pipeline.addFirst("bytemetrics", new BytesMetricsHandler(m_bytesMetricsCollector));
                pipeline.addLast("decoder", new MqttDecoder(maxBytesInMessage));
                pipeline.addLast("encoder", MqttEncoder.INSTANCE);
                pipeline.addLast("metrics", new MessageMetricsHandler(m_metricsCollector));
                pipeline.addLast("messageLogger", new MQTTMessageLogger());

                addMicrometerHandlers(pipeline, "web-socket");

                pipeline.addLast("handler", handler);

            }
        });
    }

    private void initializeSSLTCPTransport(final NettyMQTTHandler handler, IConfig props, final SSLContext sslContext)
        throws IOException {
        LOG.info("Configuring SSL MQTT transport");
        String sslPortProp = props.getProperty(SSL_PORT_PROPERTY_NAME, DISABLED_PORT_BIND);
        if (DISABLED_PORT_BIND.equals(sslPortProp)) {
            // Do nothing no SSL configured
            LOG.info("Property {} has been set to {}. SSL MQTT will be disabled",
                BrokerConstants.SSL_PORT_PROPERTY_NAME, DISABLED_PORT_BIND);
            return;
        }

        int sslPort = Integer.parseInt(sslPortProp);
        LOG.info("Starting SSL on port {}", sslPort);

        final MoquetteIdleTimeoutHandler timeoutHandler = new MoquetteIdleTimeoutHandler();
        String host = props.getProperty(BrokerConstants.HOST_PROPERTY_NAME);
        String sNeedsClientAuth = props.getProperty(BrokerConstants.NEED_CLIENT_AUTH, "false");
        final boolean needsClientAuth = Boolean.valueOf(sNeedsClientAuth);
        initFactory(host, sslPort, "SSL MQTT", new SpringNettyAcceptor.PipelineInitializer() {

            @Override
            void init(ChannelPipeline pipeline) throws Exception {
                pipeline.addLast("ssl", createSslHandler(sslContext, needsClientAuth));
                pipeline.addFirst("idleStateHandler", new IdleStateHandler(nettyChannelTimeoutSeconds, 0, 0));
                pipeline.addAfter("idleStateHandler", "idleEventHandler", timeoutHandler);
                // pipeline.addLast("logger", new LoggingHandler("Netty", LogLevel.ERROR));
                pipeline.addFirst("bytemetrics", new BytesMetricsHandler(m_bytesMetricsCollector));
                pipeline.addLast("decoder", new MqttDecoder(maxBytesInMessage));
                pipeline.addLast("encoder", MqttEncoder.INSTANCE);
                pipeline.addLast("metrics", new MessageMetricsHandler(m_metricsCollector));
                pipeline.addLast("messageLogger", new MQTTMessageLogger());

                addMicrometerHandlers(pipeline, "ssl-tcp");

                pipeline.addLast("handler", handler);
            }
        });
    }

    private void initializeWSSTransport(final NettyMQTTHandler handler, IConfig props, final SSLContext sslContext)
        throws IOException {
        LOG.info("Configuring secure websocket MQTT transport");
        String sslPortProp = props.getProperty(WSS_PORT_PROPERTY_NAME, DISABLED_PORT_BIND);
        if (DISABLED_PORT_BIND.equals(sslPortProp)) {
            // Do nothing no SSL configured
            LOG.info("Property {} has been set to {}. Secure websocket MQTT will be disabled",
                BrokerConstants.WSS_PORT_PROPERTY_NAME, DISABLED_PORT_BIND);
            return;
        }
        int sslPort = Integer.parseInt(sslPortProp);
        final MoquetteIdleTimeoutHandler timeoutHandler = new MoquetteIdleTimeoutHandler();
        String host = props.getProperty(BrokerConstants.HOST_PROPERTY_NAME);
        String sNeedsClientAuth = props.getProperty(BrokerConstants.NEED_CLIENT_AUTH, "false");
        final boolean needsClientAuth = Boolean.valueOf(sNeedsClientAuth);
        initFactory(host, sslPort, "Secure websocket", new SpringNettyAcceptor.PipelineInitializer() {

            @Override
            void init(ChannelPipeline pipeline) throws Exception {
                pipeline.addLast("ssl", createSslHandler(sslContext, needsClientAuth));
                pipeline.addLast("httpEncoder", new HttpResponseEncoder());
                pipeline.addLast("httpDecoder", new HttpRequestDecoder());
                pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
                pipeline.addLast("webSocketHandler",
                    new WebSocketServerProtocolHandler("/mqtt", MQTT_SUBPROTOCOL_CSV_LIST));
                pipeline.addLast("ws2bytebufDecoder", new NettyAcceptor.WebSocketFrameToByteBufDecoder());
                pipeline.addLast("bytebuf2wsEncoder", new NettyAcceptor.ByteBufToWebSocketFrameEncoder());
                pipeline.addFirst("idleStateHandler", new IdleStateHandler(nettyChannelTimeoutSeconds, 0, 0));
                pipeline.addAfter("idleStateHandler", "idleEventHandler", timeoutHandler);
                pipeline.addFirst("bytemetrics", new BytesMetricsHandler(m_bytesMetricsCollector));
                pipeline.addLast("decoder", new MqttDecoder(maxBytesInMessage));
                pipeline.addLast("encoder", MqttEncoder.INSTANCE);
                pipeline.addLast("metrics", new MessageMetricsHandler(m_metricsCollector));
                pipeline.addLast("messageLogger", new MQTTMessageLogger());

                addMicrometerHandlers(pipeline, "web-socket-secure");

                pipeline.addLast("handler", handler);
            }
        });
    }

    public void close() {
        LOG.info("Closing Netty acceptor...");
        if (m_workerGroup == null || m_bossGroup == null) {
            LOG.error("Netty acceptor is not initialized");
            throw new IllegalStateException("Invoked close on an Acceptor that wasn't initialized");
        }
        Future<?> workerWaiter = m_workerGroup.shutdownGracefully();
        Future<?> bossWaiter = m_bossGroup.shutdownGracefully();

        /*
         * We shouldn't raise an IllegalStateException if we are interrupted. If we did so, the
         * broker is not shut down properly.
         */
        LOG.info("Waiting for worker and boss event loop groups to terminate...");
        try {
            workerWaiter.await(10, TimeUnit.SECONDS);
            bossWaiter.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException iex) {
            LOG.warn("An InterruptedException was caught while waiting for event loops to terminate...");
        }

        if (!m_workerGroup.isTerminated()) {
            LOG.warn("Forcing shutdown of worker event loop...");
            m_workerGroup.shutdownGracefully(0L, 0L, TimeUnit.MILLISECONDS);
        }

        if (!m_bossGroup.isTerminated()) {
            LOG.warn("Forcing shutdown of boss event loop...");
            m_bossGroup.shutdownGracefully(0L, 0L, TimeUnit.MILLISECONDS);
        }

        LOG.info("Collecting message metrics...");
        MessageMetrics metrics = m_metricsCollector.computeMetrics();
        LOG.info("Metrics have been collected. Read messages={}, written messages={}", metrics.messagesRead(),
            metrics.messagesWrote());

        LOG.info("Collecting bytes metrics...");
        BytesMetrics bytesMetrics = m_bytesMetricsCollector.computeMetrics();
        LOG.info("Bytes metrics have been collected. Read bytes={}, written bytes={}", bytesMetrics.readBytes(),
            bytesMetrics.wroteBytes());
    }

    private ChannelHandler createSslHandler(SSLContext sslContext, boolean needsClientAuth) {
        SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(false);
        if (needsClientAuth) {
            sslEngine.setNeedClientAuth(true);
        }
        return new SslHandler(sslEngine);
    }

    private void addMicrometerHandlers(final ChannelPipeline pipeline, final String protocolTag) {
        if (micrometerConfig.isEnabled()) {
            final String[] tags = new String[]{"protocol", protocolTag};
            pipeline.addFirst("byteMetricsMicrometer", new MicrometerBytesHandler(tags));
            pipeline.addLast("msgMetricsMicrometer", new MicrometerMessageHandler(tags));
            pipeline.addLast("msgTypeMetricsMicrometer", new MicrometerMsgTypeHandler(tags));
        }
    }

}
