package org.kr.intp.util.db.pool;

import org.kr.intp.application.AppContext;
import org.kr.intp.application.pojo.job.JobProperties;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.TimeController;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DataSourceFactory {

    private static final Logger log = LoggerFactory.getLogger(DataSourceFactory.class);
    private static final IntpConfig config = AppContext.instance().getConfiguration();

    public static IDataSource newDataSource(int maxSize, Properties clientInfo,
                                            JobProperties.ConnectionType connectionType,
                                            long connectionTimeoutMS,
                                            long connectionRetryPauseMS,
                                            long connectionTotalTimeoutMS) {
        createTestConnection();
        switch (connectionType) {
            case HISTORY:
                long time = TimeController.getInstance().getServerUtcTimeMillis();
                return new SAPHistoricalDataSource(time, clientInfo, connectionRetryPauseMS,
                        connectionTimeoutMS, connectionTotalTimeoutMS);
            case UNDEFINED:
                log.warn("Connection type is undefined! returning default pool type");
            case POOL:
                return newDataSource(maxSize, clientInfo, connectionTimeoutMS, connectionRetryPauseMS, connectionTotalTimeoutMS);
            case SIMPLE:
            default:
                return new SAPSimpleDataSource(clientInfo,
                        connectionTimeoutMS, connectionRetryPauseMS, connectionTotalTimeoutMS);
        }
    }

    private static void createTestConnection() {
        final String jdbcUrl = config.getJdbcURL();
        try (Connection conn = DriverManager.getConnection(jdbcUrl)) {
            log.info("connection parameters are configured correctly");
        } catch (SQLException e) {
            final String host = config.getDbHost();
            final String instance = config.getDbInstance();
            final int port = config.getDbPort();
            final String user = config.getDbUser();
            final String error = String.format("Error while connecting to the server host [%s], instance [%s] (port [%d]), user [%s]: %s",
                    host, instance, port, user, e.getMessage());
            throw new RuntimeException(error, e);
        }
    }

    public static IDataSource newDataSource(int maxPoolSize, Properties clientInfo,
                                            long connectionTimeoutMS,
                                            long connectionRetryPauseMS,
                                            long connectionTotalTimeoutMS) {
        Properties poolProps = config.getJdbcPoolProperties();
        poolProps.setProperty("maxPoolSize", String.valueOf(maxPoolSize));
        poolProps.setProperty("acquireRetryDelay", String.valueOf(connectionRetryPauseMS));
        if (connectionTotalTimeoutMS == 0)
            poolProps.setProperty("acquireRetryAttempts", "0");
        else
            poolProps.setProperty("acquireRetryAttempts",
                    String.valueOf(connectionTotalTimeoutMS / connectionRetryPauseMS));
        log.warn("skipping property connectionTimeoutMS=" + connectionTimeoutMS);
        return C3P0ConnectionPool.newInstance(poolProps, clientInfo);
    }

    public static IDataSource newDataSource(int maxPoolSize) {
        Properties poolProps = config.getJdbcPoolProperties();
        poolProps.setProperty("maxPoolSize", String.valueOf(maxPoolSize));
        return newDataSource(poolProps, 0, 1000L, 0);
    }

    public static IDataSource newDataSource(Properties properties,
                                            long connectionTimeoutMS,
                                            long connectionRetryPauseMS,
                                            long connectionTotalTimeoutMS)  {
        createTestConnection();
        if (!properties.containsKey("pool.class")) {
            log.warn("Property pool.class is not defined, instantiating default pool");
            setDefaultPoolClass(properties);
        }

        properties.setProperty("acquireRetryDelay", String.valueOf(connectionRetryPauseMS));
        if (connectionTotalTimeoutMS == 0)
            properties.setProperty("acquireRetryAttempts", "0");
        else
            properties.setProperty("acquireRetryAttempts",
                    String.valueOf(connectionTotalTimeoutMS / connectionRetryPauseMS));
        log.warn("skipping property connectionTimeoutMS=" + connectionTimeoutMS);

        //else try to instantiate something
        final String className = properties.remove("pool.class").toString();
        if (log.isDebugEnabled())
            log.debug("creating pool for class: " + className);
        try {
            Class<?> klass = Class.forName(className);
            Constructor<?> constructor = klass.getDeclaredConstructor(Properties.class);
            constructor.setAccessible(true);
            Object o = constructor.newInstance(properties);
            if (o instanceof IDataSource) {
                return (IDataSource) o;
            } else {
                throw new RuntimeException("Class " + className +
                        " is not derived from org.kr.intp.util.db.pool");
            }
        } catch (Exception e) {
            throw new RuntimeException("Cannot instantiate class: " + className, e);
        }
    }

    public static IDataSource newDefaultDataSource() {
        Properties properties = config.getJdbcPoolProperties();
        setDefaultPoolClass(properties);
        return C3P0ConnectionPool.newInstance(properties);
    }

    private static void setDefaultPoolClass(Properties properties) {
        if (properties.containsKey("pool.class"))
            return;
        properties.setProperty("pool.class", C3P0ConnectionPool.class.getName());
    }

    private DataSourceFactory() { }
}
