package org.kr.itee.perfmon.ws;

import org.kr.itee.perfmon.ws.conf.ConfigurationManager;
import org.kr.itee.perfmon.ws.conf.Log4jConfigurator;
import org.kr.itee.perfmon.ws.web.server.Server;

import java.nio.file.Paths;

/**
 *
 */
public class Bootstrap {

    private static final String HOME_NAME = "PERFMON_HOME";
    private static final String home;
    public static String getHome() { return home; }

    static {
        home = initHome();
    }

    static String initHome() {
        String home = null;
        try {
            home = System.getenv(HOME_NAME);
            if (null == home || home.length() == 0)
                home = System.getProperty(HOME_NAME); //first fall-back
            if (null == home || home.length() == 0)
                home = System.getProperty("user.dir"); //second fall-back
            if (null != home) { //last fall-back
                home = Paths.get(home).toFile().getCanonicalPath();
                System.setProperty(HOME_NAME, home);
                System.out.printf("Home directory:%n%s=%s%n", HOME_NAME, home);
            }
        } catch (Exception e) {
            handleFailedHomeInitialization();
        }
        if (null == home || home.length() == 0)
            handleFailedHomeInitialization();
        initDerbyProperties(home);
        return home;
    }

    private static void initDerbyProperties(String home) {
        System.setProperty("derby.system.home", home);
        String fileSeparator = System.getProperty("file.separator");
        System.setProperty("derby.stream.error.file", home + fileSeparator + "logs" + fileSeparator + "derby.log");
    }

    private static void handleFailedHomeInitialization() {
        System.err.printf("Home directory ${%s} is not specified. Cannot proceed.%n", HOME_NAME);
        System.exit(-1);
    }

    public static void main(String... args) {
        new Log4jConfigurator(home).loadLog4j();
        int port = ConfigurationManager.instance().getServerPort();
        Server s = new Server(port);
        Runtime.getRuntime().addShutdownHook(new Thread(new Teardown(s)));
        s.start();
    }





}
