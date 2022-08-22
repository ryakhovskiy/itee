package org.kr.intp.config;

import org.kr.intp.App;
import org.kr.intp.application.AppContext;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.DbUtils;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class IntpConfig {

    private final Logger log = LoggerFactory.getLogger(IntpConfig.class);
    private final Map<String, String> properties;

    private final String dbHost;
    private final String dbInstance;
    private final int dbPort;
    private final String dbUser;
    private final String dbPassword;
    private final String jdbcUrl;
    private volatile Locale locale; //lazy

    public IntpConfig(Properties properties) {
        this((Map) properties);
    }

    public IntpConfig(Map<String, String> properties) {
        this.properties = properties;
        this.dbHost = getStringProperty("db.host", IntpConfigDefaults.DB_HOST);
        this.dbInstance = getStringProperty("db.instance", IntpConfigDefaults.DB_INSTANCE);
        this.dbUser = getStringProperty("db.user", IntpConfigDefaults.DB_USER);
        this.dbPassword = getStringProperty("db.password", IntpConfigDefaults.DB_PASSWORD);
        final int port = getIntProperty("db.port", -1);
        if (-1 == port)
            this.dbPort = Integer.parseInt(String.format("3%s15", dbInstance));
        else
            this.dbPort = port;
        this.jdbcUrl = DbUtils.createSapJdbcUrl(dbHost, dbPort, dbUser, dbPassword);
        resolveSysEnv();
        AppContext.instance().setConfiguration(this); //leaking reference
    }

    public IntpConfig(IntpConfigPOJO config) {
        this.dbHost = config.getDbHost();
        this.dbPort = config.getDbPort();
        this.dbInstance = config.getDbInstance();
        this.dbUser = config.getDbUser();
        this.dbPassword = config.getDbPassword();

        this.properties = new HashMap<>();

        properties.put("db.host", config.getDbHost());
        properties.put("db.instance", config.getDbInstance());
        properties.put("db.port", String.valueOf(config.getDbPort()));
        properties.put("db.user", config.getDbUser());
        properties.put("db.password", config.getDbPassword());
        properties.put("connection.pooling.enabled", String.valueOf(config.isConnectionPoolingEnabled()));
        properties.put("service.connection.pool.size", String.valueOf(config.getServiceConnectionPoolSize()));
        properties.put("db.logging.enabled", String.valueOf(config.isDbLogging()));

        properties.put("intp.id", config.getIntpId());
        properties.put("intp.name", config.getIntpName());
        properties.put("intp.type", config.getIntpType().toString());
        properties.put("intp.port", String.valueOf(config.getIntpPort()));
        properties.put("intp.size", String.valueOf(config.getIntpSize()));

        properties.put("db.schema", config.getDbSchema());
        properties.put("db.gen.objects.schema", config.getDbGenObjectsSchema());
        properties.put("db.ficalendar.schema", config.getDbFiCalendarSchema());
        properties.put("db.ficalendar.table", config.getDbFiCalendarTable());

        properties.put("db.app.monitor.frequency", String.valueOf(config.getDbAppMonitorFrequency()));
        properties.put("hardware.monitor.enabled", String.valueOf(config.isHardwareMonitorEnabled()));
        properties.put("hardware.monitor.frequency", String.valueOf(config.getHardwareMonitorFrequency()));

        properties.put("aggr.placeholder", config.getAggregationPlaceholder());
        properties.put("clear.empty.log.activities", String.valueOf(config.isClearEmptyLogActivities()));
        properties.put("archive.log.period.factor", String.valueOf(config.getArchiveLogPeriodFactor()));
        properties.put("db.load.threshold.cpu", String.valueOf(config.getDbLoadThresholdCPU()));
        properties.put("db.load.threshold.mem", String.valueOf(config.getDbLoadThresholdMem()));
        properties.put("concurrent.delta", String.valueOf(config.isConcurrentDelta()));
        properties.put("db.ha.enabled", String.valueOf(config.isDbHaEnabled()));
        properties.put("db.ha.pause.time.increasing", String.valueOf(config.isDbHaPauseTimeIncreasing()));
        properties.put("executor.optimization.strategy", config.getExecutorOptimizationStrategy().toString());

        properties.put("intp.locale.language", config.getIntpLocaleLanguage());
        properties.put("intp.locale.country", config.getIntpLocaleCountry());

        // Pool properties
        addPoolProperty("pool.class", config.getPoolClass());
        addPoolProperty("pool.acquireIncrement", String.valueOf(config.getAcquireIncrement()));
        addPoolProperty("pool.acquireRetryAttempts", String.valueOf(config.getAcquireRetryAttempts()));
        addPoolProperty("pool.acquireRetryDelay", String.valueOf(config.getAcquireRetryDelay()));
        addPoolProperty("pool.autoCommitOnClose", String.valueOf(config.isAutoCommitOnClose()));
        addPoolProperty("pool.automaticTestTable", config.getAutomaticTestTable());
        addPoolProperty("pool.breakAfterAcquireFailure", String.valueOf(config.isBreakAfterAcquireFailure()));
        addPoolProperty("pool.checkoutTimeout", String.valueOf(config.getCheckoutTimeout()));
        addPoolProperty("pool.connectionCustomizerClassName", config.getConnectionCustomizerClassName());
        addPoolProperty("pool.connectionTesterClassName", config.getConnectionTesterClassName());
        addPoolProperty("pool.contextClassLoaderSource", config.getContextClassLoaderSource());
        addPoolProperty("pool.dataSourceName", config.getDataSourceName());
        addPoolProperty("pool.debugUnreturnedConnectionStackTraces", String.valueOf(config.isDebugUnreturnedConnectionStackTraces()));
        addPoolProperty("pool.driverClass", config.getDriverClass());
        addPoolProperty("pool.factoryClassLocation", config.getFactoryClassLocation());
        addPoolProperty("pool.forceIgnoreUnresolvedTransactions", String.valueOf(config.isForceIgnoreUnresolvedTransactions()));
        addPoolProperty("pool.forceSynchronousCheckins", String.valueOf(config.isForceSynchronousCheckins()));
        addPoolProperty("pool.forceUseNamedDriverClass", String.valueOf(config.isForceUseNamedDriverClass()));
        addPoolProperty("pool.idleConnectionTestPeriod", String.valueOf(config.getIdleConnectionTestPeriod()));
        addPoolProperty("pool.initialPoolSize", String.valueOf(config.getInitialPoolSize()));
        addPoolProperty("pool.maxAdministrativeTaskTime", String.valueOf(config.getMaxAdministrativeTaskTime()));
        addPoolProperty("pool.maxConnectionAge", String.valueOf(config.getMaxConnectionAge()));
        addPoolProperty("pool.maxIdleTime", String.valueOf(config.getMaxIdleTime()));
        addPoolProperty("pool.maxIdleTimeExcessConnections", String.valueOf(config.getMaxIdleTimeExcessConnections()));
        addPoolProperty("pool.maxPoolSize", String.valueOf(config.getMaxPoolSize()));
        addPoolProperty("pool.maxStatements", String.valueOf(config.getMaxStatements()));
        addPoolProperty("pool.maxStatementsPerConnection", String.valueOf(config.getMaxStatementsPerConnection()));
        addPoolProperty("pool.minPoolSize", String.valueOf(config.getMinPoolSize()));
        addPoolProperty("pool.numHelperThreads", String.valueOf(config.getNumHelperThreads()));
        addPoolProperty("pool.overrideDefaultUser", config.getOverrideDefaultUser());
        addPoolProperty("pool.overrideDefaultPassword", config.getOverrideDefaultPassword());
        addPoolProperty("pool.password", config.getPassword());
        addPoolProperty("pool.preferredTestQuery", config.getPreferredTestQuery());
        addPoolProperty("pool.privilegeSpawnedThreads", String.valueOf(config.isPrivilegeSpawnedThreads()));
        addPoolProperty("pool.propertyCycle", String.valueOf(config.getPropertyCycle()));
        addPoolProperty("pool.statementCacheNumDeferredCloseThreads", String.valueOf(config.getStatementCacheNumDeferredCloseThreads()));
        addPoolProperty("pool.testConnectionOnCheckin", String.valueOf(config.isTestConnectionOnCheckin()));
        addPoolProperty("pool.testConnectionOnCheckout", String.valueOf(config.isTestConnectionOnCheckout()));
        addPoolProperty("pool.unreturnedConnectionTimeout", String.valueOf(config.getUnreturnedConnectionTimeout()));
        addPoolProperty("pool.user", config.getUser());
        addPoolProperty("pool.usesTraditionalReflectiveProxies", String.valueOf(config.isUsesTraditionalReflectiveProxies()));
        this.jdbcUrl = DbUtils.createSapJdbcUrl(dbHost, dbPort, dbUser, dbPassword);
        resolveSysEnv();
        AppContext.instance().setConfiguration(this); //leaking reference
    }

    private void addPoolProperty(String key, String value) {
        if (null == value || value.length() == 0)
            return;
        properties.put(key, value);
    }

    public Map<String, String> getPropertiesMap() {
        return new HashMap<>(properties);
    }

    public String getJdbcURL() {
        return jdbcUrl;
    }

    public String getDbHost() {
        return dbHost;
    }

    public String getDbInstance() {
        return dbInstance;
    }

    public int getDbPort() {
        return dbPort;
    }

    public String getDbUser() {
        return dbUser;
    }

    public long getDbAppMonitorFrequencyMillis() {
        long value = getLongProperty("db.app.monitor.frequency", IntpConfigDefaults.DB_APP_MONITOR_FREQUENCY_L);
        return TimeUnit.SECONDS.toMillis(value);
    }

    public Locale getLocale() {
        if (null != locale)
            return locale;
        Locale defaultLocale = Locale.getDefault();
        final String sLocaleLanguage = getStringProperty("intp.locale.language", defaultLocale.getLanguage());
        final String sLocaleCountry = getStringProperty("intp.locale.country", defaultLocale.getCountry());
        Locale ret = null;
        if (null == sLocaleLanguage || null == sLocaleCountry) {
            ret = defaultLocale;
        } else {
            ret = new Locale(sLocaleLanguage, sLocaleCountry);
        }
        synchronized (this) {
            if (null == locale)
                locale = ret;
        }
        return locale;
    }

    public String getIntpSchema() {
        return getStringProperty("db.schema", IntpConfigDefaults.DB_SCHEMA).toUpperCase();
    }

    public String getIntpGenObjectsSchema() {
        return getStringProperty("db.gen.objects.schema", IntpConfigDefaults.DB_GEN_OBJECTS_SCHEMA);
    }

    public String getFiscalCalendarSchema() {
        return getStringProperty("db.ficalendar.schema", IntpConfigDefaults.DB_FISCAL_CALENDAR_SCHEMA);
    }

    public String getFiscalCalendarTable() {
        return getStringProperty("db.ficalendar.table", IntpConfigDefaults.DB_FISCAL_CALENDAR_TABLE);
    }

    public String getAggregationPlaceholder() {
        return getStringProperty("aggr.placeholder", IntpConfigDefaults.AGGREGATION_PLACEHOLDER);
    }

    public boolean isClearEmptyLogActivitiesEnabled() {
        return getBooleanProperty("clear.empty.log.activities", IntpConfigDefaults.CLEAR_EMPTY_LOG_ACTIVITIES_B);
    }

    public Properties getJdbcPoolProperties() {
        final Properties props = new Properties();
        for (Map.Entry<String, String> e : properties.entrySet()) {
            final String key = e.getKey();
            if (key.startsWith("pool."))
                props.put(key.substring(5), e.getValue());

        }
        if (!props.containsKey("jdbcUrl")) {
            props.put("jdbcUrl", getJdbcURL());
        }
        return props;
    }

    public boolean getConcurrentDeltaAllowed() {
        return getBooleanProperty("concurrent.delta", IntpConfigDefaults.CONCURRENT_DELTA_B);
    }

    public int getServiceConnectionPoolSize() {
        return getIntProperty("service.connection.pool.size", IntpConfigDefaults.SERVICE_CONNECTION_POOL_SIZE_I);
    }

    public int getDbmsLoadCpuThreshold() {
        return getIntProperty("db.load.threshold.cpu", IntpConfigDefaults.DB_LOAD_THRESHOLD_CPU_I);
    }

    public int getDbmsLoadMemoryThreshold() {
        return getIntProperty("db.load.threshold.mem", IntpConfigDefaults.DB_LOAD_THRESHOLD_MEM_I);
    }

    public int getIntpSize() {
        return getIntProperty("intp.size", IntpConfigDefaults.INTP_SIZE_DEFAULT_I);
    }

    public long getArchivelogPeriodFactor() {
        return getLongProperty("archive.log.period.factor", IntpConfigDefaults.ARCHIVELOG_PERIOD_FACTOR_I);
    }

    public long getHwMonitorFrequencyMS() {
        return getLongProperty("hardware.monitor.frequency", IntpConfigDefaults.HW_MONITOR_FREQUENCY_L);
    }

    public boolean isHwMonitorEnabled() {
        return getBooleanProperty("hardware.monitor.enabled", IntpConfigDefaults.HW_MONITOR_ENABLED_B);
    }

    public boolean isDbHaEnabled() {
        return getBooleanProperty("db.ha.enabled", IntpConfigDefaults.DB_HA_ENABLED_B);
    }

    public boolean isDbLoggingEnabled() {
        return getBooleanProperty("db.logging.enabled", false);
    }

    public boolean isDbHaPauseTimeIncreasing() {
        return getBooleanProperty("db.ha.pause.time.increasing", true);
    }

    public String getWorkloadPriority() {
        return getStringProperty("workload.priority.default", IntpConfigDefaults.DEFAULT_WORKLOAD_PRIORITY);
    }

    public String getExecutorOptimizationStrategy() {
        return getStringProperty("executor.optimization.strategy", IntpConfigDefaults.EXECUTOR_OPTIMIZATION_STRATEGY);
    }

    public String getIntpInstanceId() {
        return getStringProperty("intp.id", "000");
    }

    public String getIntpName() {
        return getStringProperty("intp.name", "N/A, check intp.properties file");
    }

    public int getIntpPort() {
        return getIntProperty("intp.port", 0);
    }

    public String getIntpHost() {
        return getStringProperty("intp.host", "");
    }

    public char getIntpType() {
        return getStringProperty("intp.type", "D").charAt(0);
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public long getDefaultGlobalTimeouts(String propertyName) {
        final String key = "global.timeouts." + propertyName;
        return getLongProperty(key, 0L);
    }

    /************************************************************************************/

    public String getStringProperty(String key, String defaultValue) {
        String value = properties.get(key);
        if (null == value) {
            log.trace("Property {" + key + "} not found, returning default value: " + defaultValue);
            return defaultValue;
        } else
            return value;
    }

    public boolean getBooleanProperty(String key, boolean defaultValue) {
        String value = getStringProperty(key, String.valueOf(defaultValue));
        return "true".equalsIgnoreCase(value);
    }

    public int getIntProperty(String key, int defaultValue) {
        String value = getStringProperty(key, String.valueOf(defaultValue));
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            log.error("Property {" + key + "} is not numeric: " + value);
            return defaultValue;
        }
    }

    public long getLongProperty(String key, long defaultValue) {

        String value = getStringProperty(key, String.valueOf(defaultValue));
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            log.error("Property {" + key + "} is not numeric: " + value);
            return defaultValue;
        }
    }

    private Properties readExternalProperties(Path path) {
        Properties properties = new Properties();
        try (InputStream stream = Files.newInputStream(path, StandardOpenOption.READ)) {
            properties.load(stream);
        } catch (Exception e) {
            log.error("Error while reading file: " + path, e);
        }
        return properties;
    }

    private Properties readInternalProperties(String name) {
        Properties properties = new Properties();
        try (InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(name)) {
            properties.load(stream);
        } catch (Exception e) {
            log.error("Error while reading resource: " + name, e);
        }
        return properties;
    }

    private void resolveSysEnv() {
        Map<String, String> env = System.getenv();
        Map<String, String> sys = (Map) System.getProperties();
        Set<String> keys = properties.keySet();

        for (String key : keys) {
            String val = properties.get(key);
            boolean changed = false;
            while (val.contains("${")) {
                int startIndex = val.indexOf("${");
                int endIndex = val.indexOf("}", startIndex + 2);
                if (endIndex < 0)
                    break;
                String var = val.substring(startIndex + 2, endIndex);
                log.debug("variable found: " + var);
                String varValue = null;
                if (env.containsKey(val))
                    varValue = env.get(var);
                else if (sys.containsKey(var))
                    varValue = sys.get(var);
                else if (var.equals(App.HOME_NAME))
                    varValue = App.INTP_HOME;

                if (null == varValue) {
                    log.warn("variable found, but value was not set: " + var);
                    break;
                }
                changed = true;
                val = val.replace("${" + var + "}", varValue);
            }
            if (changed)
                properties.put(key, val);
        }
    }
}
