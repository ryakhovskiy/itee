package org.kr.intp;

import org.kr.intp.application.agent.IntpServer;
import org.kr.intp.config.IntpConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created bykron 22.11.2016.
 */
public class ServerContainer {

    private final Map<String, IntpServer> servers = new HashMap<>();

    public synchronized boolean startServer(IntpConfig config) throws Exception {
        Class.forName("com.sap.db.jdbc.Driver");
        final String host = config.getDbHost();
        final int port = config.getDbPort();
        final String schema = config.getIntpSchema();
        final String key = buildKey(host, port, schema);
        IntpServer server = servers.get(key);
        if (null == server) {
            IntpServerInfo info = new IntpServerInfo(config.getIntpInstanceId(), config.getIntpName(),
                    config.getIntpType(), config.getIntpPort(), config.getIntpSize());
            server = new IntpServer(info, config);
            servers.put(key, server);
        }
        if (server.isRunning())
            return false;
        server.start();
        return true;
    }

    public synchronized boolean stopServer(String host, int port, String schema) throws IOException {
        final String key = buildKey(host, port, schema);
        IntpServer server = servers.remove(key);
        if (null == server)
            return false;
        server.close();
        server = null;
        return true;
    }

    public synchronized boolean isRunning(String host, int port, String schema) {
        final String key = buildKey(host, port, schema);
        final IntpServer server = servers.get(key);
        return null != server && server.isRunning();
    }

    private String buildKey(String host, int port, String schema) {
        if (null == host)
            throw new NullPointerException("host cannot be null");
        /*if (host.length() == 0)
            throw new IllegalArgumentException("host cannot be empty");*/
        if (port < 0 || (1 << 16) < port)
            throw new IllegalArgumentException("wrong port number: " + port);
        if (null == schema)
            throw new NullPointerException("schema cannot be null");
        /*if (schema.length() == 0)
            throw new IllegalArgumentException("schema cannot be empty");*/
        return String.format("%s%d%s", host, port, schema);
    }

}
