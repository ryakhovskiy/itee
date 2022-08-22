package org.kr.intp.client.protocol;

import java.util.Properties;

/**
 * Created by kr on 18.12.13.
 */
public class IntpMessage {

    private MessageType type = MessageType.UNDEFINED;
    private Properties properties = new Properties();

    public IntpMessage() { }

    public MessageType getType() {
        return type;
    }

    public Properties getProperties() {
        Properties ret = new Properties();
        ret.putAll(properties);
        return ret;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public String toString() {
        return String.format("type: %s; Properties: %s", type, properties);
    }
}
