package org.kr.intp.config;

import org.kr.intp.App;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 9/14/13
 * Time: 3:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class IntpFileConfig {

    private static final String HOME = App.INTP_HOME;
    private static final String PROPERTIES_FILE = "intp.properties";
    private final Logger log = LoggerFactory.getLogger(IntpFileConfig.class);

    private static final IntpFileConfig instance = new IntpFileConfig();

    public static IntpFileConfig getFileInstance() {
        return instance;
    }

    public static IntpFileConfig getResourceInstance() {
        // read config from resource
        return new IntpFileConfig(true);
    }

    private final Properties properties = new Properties();
    private final String dbHost;
    private final String dbInstance;
    private final int dbPort;
    private final String dbUser;
    private final String dbPassword;

    private IntpFileConfig() {
        this(false);
    }

    private IntpFileConfig(boolean resource) {
        initConfig(resource);
        resolveSysEnv();
        this.dbHost = properties.getProperty("db.host", IntpConfigDefaults.DB_HOST);
        this.dbInstance = properties.getProperty("db.instance", IntpConfigDefaults.DB_INSTANCE);
        this.dbUser = properties.getProperty("db.user", IntpConfigDefaults.DB_USER);
        this.dbPassword = properties.getProperty("db.password", IntpConfigDefaults.DB_PASSWORD);
        final int port = getIntProperty("db.port", "0");
        if (0 == port)
            this.dbPort = Integer.parseInt(String.format("3%s15", dbInstance));
        else this.dbPort = port;
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getPropertiesMap() {
        return new HashMap(properties);
    }

    private void initConfig(boolean resource) {
        if (resource) //read from resource
            initResourceConfig();
        else //read from file
            initFileConfig();
    }

    private void initResourceConfig() {
        log.info("loading test config file...");
        try (InputStream inputStream = ClassLoader.getSystemResourceAsStream(PROPERTIES_FILE)) {
            properties.load(inputStream);
        } catch (IOException e) {
            log.error("Cannot load config file: " + e.getMessage(), e);
        }
    }

    private void initFileConfig() {
        log.info("reading config file...");

        boolean loaded = loadConfig(Paths.get(HOME, "conf", PROPERTIES_FILE));
        if (loaded)
            return;

        loaded = loadConfig(Paths.get(HOME, PROPERTIES_FILE));
        if (loaded)
            return;

        loaded = loadConfig(Paths.get("..", "conf", PROPERTIES_FILE));
        if (loaded)
            return;

        loaded = loadConfig(Paths.get("conf", PROPERTIES_FILE));
        if (loaded)
            return;

        loaded = loadConfig(Paths.get(PROPERTIES_FILE));
        if (loaded)
            return;

        Path outPath = Paths.get(HOME, "conf", PROPERTIES_FILE);

        try {
            if (!Files.exists(Paths.get(HOME, "conf")))
                Files.createDirectories(Paths.get(HOME, "conf"));

            unloadConfig(outPath);
            log.error("Config file was not found." +
                    " New config created, specify the parameters in the config file: "
                    + outPath);
            System.exit(-1);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            System.exit(-1);
        }
    }

    private boolean loadConfig(Path path) {
        log.debug("looking up configuration file: " + path);
        if (!Files.exists(path)) {
            log.debug("File not found: " + path);
            return false;
        }
        log.debug("loading configuration from: " + path);
        try (InputStream stream = Files.newInputStream(path, StandardOpenOption.READ)) {
            properties.load(stream);
            return true;
        } catch (IOException e) {
            log.error("Error while reading configuration file: " + e.getMessage(), e);
            return false;
        }
    }

    public String getJdbcURL() {
        return String.format("jdbc:sap://%s:%d?user=%s&password=%s", dbHost, dbPort, dbUser, dbPassword);
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

    private void unloadConfig(Path outPath) throws IOException {
        log.debug("unloading configuration file to: " + outPath);
        InputStream inputStream = null;
        BufferedReader reader = null;
        PrintWriter writer = null;
        try {
            inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("intp.properties");
            if (null == inputStream) {
                log.error("Config template does not exists in the resources.");
                System.exit(-1);
            }
            String line;
            reader = new BufferedReader(new InputStreamReader(inputStream));
            writer = new PrintWriter(Files.newOutputStream(outPath, StandardOpenOption.CREATE_NEW));
            while (null != (line = reader.readLine()))
                writer.println(line);
            writer.flush();
        } finally {
            if (null != inputStream)
                inputStream.close();
            if (null != reader)
                reader.close();
            if (null != writer)
                writer.close();
        }
    }

    public long getDbAppMonitorFrequencyMillis() {
        long value = getLongProperty("db.app.monitor.frequency", IntpConfigDefaults.DB_APP_MONITOR_FREQUENCY);
        return TimeUnit.SECONDS.toMillis(value);
    }

    public Locale getLocale() {
        final String sLocaleLanguage = properties.getProperty("intp.locale.language");
        final String sLocaleCountry = properties.getProperty("intp.locale.country");
        if (null == sLocaleLanguage || null == sLocaleCountry) {
            return Locale.getDefault();
        } else {
            return new Locale(sLocaleLanguage, sLocaleCountry);
        }
    }

    public String getIntpSchema() {
        return properties.getProperty("db.schema", IntpConfigDefaults.DB_SCHEMA);
    }

    public String getIntpGenObjectsSchema() {
        return properties.getProperty("db.gen.objects.schema", IntpConfigDefaults.DB_GEN_OBJECTS_SCHEMA);
    }

    public String getFiscalCalendarSchema() {
        return properties.getProperty("db.ficalendar.schema", IntpConfigDefaults.DB_FISCAL_CALENDAR_SCHEMA);
    }

    public String getFiscalCalendarTable() {
        return properties.getProperty("db.ficalendar.table", IntpConfigDefaults.DB_FISCAL_CALENDAR_TABLE);
    }

    public String getAggregationPlaceholder() {
        return properties.getProperty("aggr.placeholder", IntpConfigDefaults.AGGREGATION_PLACEHOLDER);
    }

    public boolean isClearEmptyLogActivitiesEnabled() {
        return getBooleanProperty("clear.empty.log.activities", IntpConfigDefaults.CLEAR_EMPTY_LOG_ACTIVITIES);
    }

    public Properties getJdbcPoolProperties() {
        final String spath = properties.getProperty("jdbc.connection.pool.properties",
                IntpConfigDefaults.CONNECTION_POOL_PROPERTIES);
        Path path = Paths.get(spath);
        Properties properties = null;
        if (Files.exists(path)) {
            properties = readExternalProperties(path);
        } else {
            log.warn("File not found: " + path + "; reading internal properties: " +
                    IntpConfigDefaults.CONNECTION_POOL_PROPERTIES);
            properties = readInternalProperties(IntpConfigDefaults.CONNECTION_POOL_PROPERTIES);
        }
        if (!properties.containsKey("jdbcUrl")) {
            properties.setProperty("jdbcUrl", getJdbcURL());
        }
        return properties;
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

    public boolean getConcurrentDeltaAllowed() {
        return getBooleanProperty("concurrent.delta", IntpConfigDefaults.CONCURRENT_DELTA);
    }

    public boolean isConnectionPoolingEnabled() {
        return getBooleanProperty("connection.pooling.enabled", IntpConfigDefaults.IS_CONNECTION_POOLING_ENABLED);
    }

    public int getServiceConnectionPoolSize() {
        return getIntProperty("service.connection.pool.size", IntpConfigDefaults.SERVICE_CONNECTION_POOL_SIZE);
    }

    public int getDbmsLoadCpuThreshold() {
        return getIntProperty("db.load.threshold.cpu", IntpConfigDefaults.DB_LOAD_THRESHOLD_CPU);
    }

    public int getDbmsLoadMemoryThreshold() {
        return getIntProperty("db.load.threshold.mem", IntpConfigDefaults.DB_LOAD_THRESHOLD_MEM);
    }

    public int getDefaultIntpSize() {
        return getIntProperty("intp.size", IntpConfigDefaults.INTP_SIZE_DEFAULT);
    }

    public long getArchivelogPeriodFactor() {
        return getLongProperty("archive.log.period.factor", IntpConfigDefaults.ARCHIVELOG_PERIOD_FACTOR);
    }

    public long getHwMonitorFrequencyMS() {
        return getLongProperty("hardware.monitor.frequency", IntpConfigDefaults.HW_MONITOR_FREQUENCY);
    }

    public boolean isHwMonitorEnabled() {
        return getBooleanProperty("hardware.monitor.enabled", IntpConfigDefaults.HW_MONITOR_ENABLED);
    }

    public boolean isDbHaEnabled() {
        return getBooleanProperty("db.ha.enabled", IntpConfigDefaults.DB_HA_ENABLED);
    }

    public boolean isDbHaPauseTimeIncreasing() {
        return getBooleanProperty("db.ha.pause.time.increasing", "true");
    }

    public String getWorkloadPriority() {
        return properties.getProperty("workload.priority.default", IntpConfigDefaults.DEFAULT_WORKLOAD_PRIORITY);
    }

    public String getExecutorOptimizationStrategy() {
        return properties.getProperty("executor.optimization.strategy", IntpConfigDefaults.EXECUTOR_OPTIMIZATION_STRATEGY);
    }

    /*
    * *************************************************************************************************************
    * *************************************************************************************************************
    */
    private int getIntProperty(String key, String defaultValue) {
        final String value = properties.getProperty(key, defaultValue);
        return Integer.valueOf(value);
    }

    private long getLongProperty(String key, String defaultValue) {
        final String value = properties.getProperty(key, defaultValue);
        return Long.valueOf(value);
    }

    private boolean getBooleanProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue).toLowerCase().equals("true");
    }

    public String getIntpInstanceId() {
        return properties.getProperty("intp.id", "000");
    }

    public String getIntpName() {
        return properties.getProperty("intp.name", "N/A, check intp.properties file");
    }

    public int getIntpPort() {
        return getIntProperty("intp.port", "0");
    }

    public String getIntpHost() {
        return properties.getProperty("intp.host", "");
    }

    public char getIntpType() {
        return properties.getProperty("intp.type", "D").charAt(0);
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public long getDefaultGlobalTimeouts(String propertyName) {
        final String key = "global.timeouts." + propertyName;
        return getLongProperty(key, "0");
    }

    private void resolveSysEnv() {
        Map<String, String> env = System.getenv();
        Map<String, String> sys = (Map) System.getProperties();
        Set<String> keys = ((Map) properties).keySet();

        for (String key : keys) {
            String val = properties.getProperty(key);
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
                properties.setProperty(key, val);
        }
    }
}