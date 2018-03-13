package io.moquette.server;

import io.netty.handler.codec.mqtt.MqttPublishMessage;

/**
 * @author Shalbanov, Kostiantyn (kostiantyn.shalbanov@intecsoft.de)
 */
public interface InternalPublisher {
    void publish(MqttPublishMessage msg, final String clientId);
}
