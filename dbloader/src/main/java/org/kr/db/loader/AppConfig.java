package org.kr.db.loader;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by kr on 21.01.14.
 */
public class AppConfig {

    private static final AppConfig config = new AppConfig();
    private final Logger log = Logger.getLogger(AppConfig.class);
    private final Properties properties = new Properties();

    public static AppConfig getInstance() {
        return config;
    }

    private AppConfig() {
        try {
            File file = new File("dbloader.properties");
            if (file.exists())
                properties.load(new FileInputStream(file));
            else {
                InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("dbloader.properties");
                if (null == inputStream) {
                    log.error("Cannot find dbloader.properties file!");
                    System.exit(-1);
                }
                properties.load(inputStream);
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public String getJdbcDriverName() { return properties.getProperty("jdbc.driver"); }

    public boolean isConnectionPoolingEnabled() { return properties.getProperty("connection.pooling.enabled").toLowerCase().equals("true"); }

    public String getJdbcURL() {
        return properties.getProperty("jdbc.url");
    }

    public long getSchedulerInterval() {
        return getLongProperty("scheduler.interval.ms");
    }

    public int getQueriesPerInterval() {
        return getIntProperty("queries.per.interval");
    }

    public DbQueryType getQueryType() {
        return properties.getProperty("query.type").toLowerCase().equals("query") ? DbQueryType.QUERY : DbQueryType.CALL;
    }

    public String getQueryFile() {
        return properties.getProperty("query.file");
    }

    public String getQueryFileEncoding() {
        return properties.getProperty("query.file.encoding");
    }

    public boolean isRoundRobinEnabled() {
        return properties.getProperty("round.robin").toLowerCase().equals("true");
    }

    public long getExecutionTime() {
        return getLongProperty("exec.time");
    }

    public int getConcurrentExecutors() {
        return getIntProperty("concurrent.executors");
    }

    private int getIntProperty(String name) {
        return Integer.valueOf(properties.getProperty(name, "0"));
    }

    private long getLongProperty(String name) {
        return Long.valueOf(properties.getProperty(name, "0"));
    }


}
