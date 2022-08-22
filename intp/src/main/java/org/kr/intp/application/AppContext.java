package org.kr.intp.application;

import org.kr.intp.IntpMessages;
import org.kr.intp.application.agent.IServer;
import org.kr.intp.config.IntpConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *
 */
public class AppContext {

    private static final AppContext DEFAULT_CONTEXT = new AppContext();
    public static AppContext instance() { return DEFAULT_CONTEXT; }
    public static AppContext newContext() { return new AppContext(); }

    private IServer server;
    private IntpConfig config;
    private IntpMessages intpMessages;

    public IServer getServer() {
        return server;
    }

    public void setServer(IServer server) {
        this.server = server;
    }

    public IntpMessages getIntpMessages() {
        return intpMessages;
    }

    public void setIntpMessage(IntpMessages intpMessages) {
        this.intpMessages = intpMessages;
    }

    public IntpConfig getConfiguration() {
        return config;
    }

    public void setConfiguration(IntpConfig config) {
        this.config = config;
    }

    public static String getVersion() {
        try (InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream("server.properties")) {
            Properties props = new Properties();
            props.load(stream);
            return props.getProperty("intp.version");
        } catch (IOException e) {
            return "";
        }
    }
}
