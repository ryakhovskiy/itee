package org.kr.itee.perfmon.ws.conf;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 */
public class Log4jConfigurator {

    private static final String LOG4J_NAME = "log4j.properties";
    private final String home;

    public Log4jConfigurator(String home) {
        this.home = home;
    }

    public void loadLog4j()  {
        Path path = Paths.get(home, "conf", LOG4J_NAME);
        if (Files.exists(path)) {
            loadLog4j(path.toAbsolutePath().toString());
            return;
        }
        path = Paths.get(home, LOG4J_NAME);
        if (Files.exists(path)) {
            loadLog4j(path.toAbsolutePath().toString());
            return;
        }
        path = Paths.get(".", "conf", LOG4J_NAME);
        if (Files.exists(path)) {
            loadLog4j(path.toAbsolutePath().toString());
            return;
        }
        path = Paths.get(".", LOG4J_NAME);
        if (Files.exists(path)) {
            loadLog4j(path.toAbsolutePath().toString());
            return;
        }
        loadLog4jConfigFromRecourse();
    }

    private void loadLog4j(String path) {
        PropertyConfigurator.configure(path);
        Logger.getLogger(Log4jConfigurator.class).debug("log4j configuration loaded from: " + path);
    }

    private static void loadLog4jConfigFromRecourse() {
        try (InputStream inputStream = ClassLoader.getSystemResourceAsStream("log4j.properties")) {
            PropertyConfigurator.configure(inputStream);
            Logger.getLogger(Log4jConfigurator.class).debug("log4j configuration loaded from resources");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
