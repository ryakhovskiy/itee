package org.kr.intp;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 17.11.13
 * Time: 22:05
 * To change this template use File | Settings | File Templates.
 */
public final class IntpServerInfo {

    private static final char[] SUPPORTED_TYPES = {'Q', 'D', 'P'};
    private final int port;
    private final String instanceId;
    private final String name;
    private final char type;
    private final int intpSize;

    public IntpServerInfo(String instanceId, String name, char type, int port, int sz) {
        if (instanceId.length() > 3)
            throw new IllegalArgumentException("INSTANCE_ID cannot have more than 3 characters");
        if (name.length() > 100)
            throw new IllegalArgumentException("NAME cannot have more than 100 characters");
        boolean supported_type = false;
        for (char c : SUPPORTED_TYPES)
            supported_type = supported_type || c == type;
        if (!supported_type)
            throw new IllegalArgumentException("Unsupported instance type: " + type);
        this.port = port;
        this.name = name;
        this.type = type;
        this.instanceId = instanceId;
        this.intpSize = sz;
    }

    public int getPort() {
        return port;
    }

    public int getIntpSize() {
        return intpSize;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getName() {
        return name;
    }

    public char getType() {
        return type;
    }

    @Override
    public int hashCode() {
        return 7 * port +
                11 * type +
                13 * instanceId.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (null == other)
            return false;
        if (this == other)
            return true;
        if (!(other instanceof IntpServerInfo))
            return false;
        final IntpServerInfo info = (IntpServerInfo)other;
        return info.instanceId.equals(this.instanceId) &&
                info.port == this.port &&
                info.type == this.type;
    }
}
