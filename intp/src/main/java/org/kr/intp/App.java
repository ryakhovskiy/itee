package org.kr.intp;

import org.kr.intp.application.AppContext;
import org.kr.intp.application.agent.IntpServer;
import org.kr.intp.application.monitor.JVMPauseMonitor;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.config.IntpFileConfig;
import org.kr.intp.logging.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Main Entry Point for a standalone app
 * NOT SUPPORTED since 11.2016
 */
public final class App {

    public static final String HOME_NAME = "INTP_HOME";
    public static final String INTP_HOME;

    static {
        INTP_HOME = initHome(HOME_NAME);
        System.setProperty(HOME_NAME, INTP_HOME);
    }

    private static final long offset = getOffset();
    public static IntpMessages intpMessages;
    private static IntpServer server;

    public static long currentTimeMillis() {
        return System.currentTimeMillis() + offset;
    }

    private static long getOffset() {
        final TimeZone tz = TimeZone.getDefault();
        return (-1) * tz.getOffset(System.currentTimeMillis());
    }

    public static void main(String... args) throws IOException, InterruptedException {
        if (args.length > 0 && (args[0].startsWith("/?") || args[0].toLowerCase().startsWith("--help"))) {
            printHint();
            System.exit(0);
        }

        if (null == INTP_HOME || INTP_HOME.trim().length() == 0) {
            System.err.println("INTP_HOME environment variable is not set.");
            System.exit(-1);
        }
        loadLog4jConfig();
        new JVMPauseMonitor().start();
        Logger logger = Logger.getLogger(App.class);
        logger.info("Logger initialized");
        logger.debug("INTP_HOME: " + INTP_HOME);
        final IntpConfig config = new IntpConfig(IntpFileConfig.getFileInstance().getPropertiesMap());
        final String instanceId = config.getIntpInstanceId();
        final String name = config.getIntpName();
        final char type = config.getIntpType();
        final int port = config.getIntpPort();
        final int intpSize = config.getIntpSize();
        final IntpServerInfo serverInfo = new IntpServerInfo(instanceId, name, type, port, intpSize);
        intpMessages = new IntpMessages(config.getLocale());
        AppContext.instance().setIntpMessage(intpMessages);
        Runtime.getRuntime().addShutdownHook(new Thread("SHUTDOWN_THREAD") {
            public void run() {
                App.stop();
            }
        });
        start(serverInfo, config);
    }

    private static void printHint() {
        System.out.println();
        System.out.println("In-Time Server");
        System.out.printf("In-Time Home: %s%n", INTP_HOME);
        System.out.printf("Java version: %s; vendor: %s%n", System.getProperty("java.version"),
                System.getProperty("java.vendor"));
        System.out.printf("Java Home: %s%n", System.getProperty("java.home"));
        System.out.printf("Default Locale: %s%n", Locale.getDefault());
        System.out.printf("OS Name: %s; version: %s; arch: %s%n", System.getProperty("os.name"),
                System.getProperty("os.version"), System.getProperty("os.arch"));
    }

    public static void start(IntpServerInfo serverInfo, IntpConfig config) {
        Logger logger = Logger.getLogger(App.class);
        logger.info("starting In-Time Server");
        try {
            server = new IntpServer(serverInfo, config);
            server.start();
        } catch (Exception e) {
            String message = intpMessages.getString("org.kr.intp.app.001", "Error while starting IntpServer: ");
            logger.error(message + e.getMessage(), e);
            System.exit(-1);
        }
    }

    public static void stop() {
        Logger logger = Logger.getLogger(App.class);
        if (null == server)
            return;
        try {
            server.close();
        } catch (Exception e) {
            String message = intpMessages.getString("org.kr.intp.app.002",
                    "Error while stopping IntpServer: %s");
            logger.error(String.format(message, e.getMessage()), e);
            e.printStackTrace();
        }
    }

    /**
     * loads log4j.properties file located in resources directory
     * @throws java.io.IOException
     */
    private static void loadLog4jConfig() throws IOException {
        if (new File(INTP_HOME + "/conf/log4j.properties").exists())
            PropertyConfigurator.configure(INTP_HOME + "/conf/log4j.properties");
        else
            loadLog4jConfigFromRecourse();
    }

    private static void loadLog4jConfigFromRecourse() throws IOException {
        InputStream inputStream = null;
        try {
            inputStream = ClassLoader.getSystemResourceAsStream("log4j.properties");
            PropertyConfigurator.configure(inputStream);
        } finally {
            if (null != inputStream)
                inputStream.close();
        }
    }

    private static String initHome(String homeName) {
        try {
            String home = System.getenv(homeName);
            if (null == home || home.length() == 0) //first fall-back
                home = System.getProperty(homeName);
            if (null == home || home.length() == 0) { //second fall-back
                home = Paths.get("..").toFile().getCanonicalPath();
                System.err.printf("Home directory is not specified. Setting home directory: [%s]=%s", homeName, home);
            }
            return home;
        } catch (Exception e) {
            e.printStackTrace();
            return ".";
        }
    }
}


