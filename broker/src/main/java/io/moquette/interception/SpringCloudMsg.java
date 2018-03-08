package io.moquette.interception;

import io.moquette.interception.messages.InterceptPublishMessage;

import java.io.Serializable;

import static io.moquette.spi.impl.Utils.readBytesAndRewind;

/**
 * @author Shalbanov, Kostiantyn (kostiantyn.shalbanov@intecsoft.de)
 */
public class SpringCloudMsg implements Serializable {
    private String clientId;
    private int qos;
    private byte[] payload;
    private String topic;

    public SpringCloudMsg(){}

    public SpringCloudMsg(InterceptPublishMessage msg) {
        this.clientId = msg.getClientID();
        this.topic = msg.getTopicName();
        this.qos = msg.getQos().value();
        this.payload = readBytesAndRewind(msg.getPayload());
    }

    public String getClientId() {
        return clientId;
    }

    public int getQos() {
        return qos;
    }

    public byte[] getPayload() {
        return payload;
    }

    public String getTopic() {
        return topic;
    }

    public void setClientId(final String clientId) {
        this.clientId = clientId;
    }

    public void setQos(final int qos) {
        this.qos = qos;
    }

    public void setPayload(final byte[] payload) {
        this.payload = payload;
    }

    public void setTopic(final String topic) {
        this.topic = topic;
    }
}
