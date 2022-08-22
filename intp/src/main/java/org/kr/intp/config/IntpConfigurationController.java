package org.kr.intp.config;

import org.kr.intp.ServerContainer;
import org.kr.intp.application.AppContext;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.model.pojo.ExecutorOptimizationStrategy;
import org.kr.intp.model.pojo.ServerType;
import org.kr.intp.util.db.DbUtils;
import org.kr.intp.util.jsf.JSFHelper;
import org.kr.intp.util.license.LicenseInstaller;

import javax.annotation.PostConstruct;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ApplicationScoped;
import javax.faces.bean.ManagedBean;

import javax.inject.Inject;
import javax.servlet.http.Part;
import java.io.*;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;


/**
 */
@ManagedBean(eager = true)
@ApplicationScoped
public class IntpConfigurationController implements Serializable {

    private static final long serialVersionUID = -2139615125060395730L;

    private static final ServerContainer serverContainer = new ServerContainer();

    private static final Logger log = LoggerFactory.getLogger(IntpConfigurationController.class);

    private boolean connectionVerified = false;

    private IntpConfigPOJO config;

    // License
    private Part file;

    @Inject
    JSFHelper jsfHelper;

    @PostConstruct
    public void init() {
        config = new IntpConfigPOJO();
    }

    public IntpConfigPOJO getConfig() {
        return config;
    }

    public void setConfig(IntpConfigPOJO config) {
        this.config = config;
    }

    /**
     * Starts the In-Time server.
     *
     * @return <code>true</code> if In-Time server was started. Otherwise, if the In-Time server has been already
     * started or could not be started it returns <code>false</code>.
     */
    public boolean startServer() {
        if (!hostParametersFilled())
            return false;
        try {
            final IntpConfig iConfig = new IntpConfig(config);
            return serverContainer.startServer(iConfig);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            stopServer();
            return false;
        }
    }

    /**
     * Checks whether an In-Time server is already running.
     *
     * @return <code>true</code> if In-Time server is running. Otherwise false.
     */
    public boolean isRunning() {
        return hostParametersFilled()
                && serverContainer.isRunning(config.getDbHost(), config.getDbPort(), config.getDbSchema());
    }

    private boolean hostParametersFilled() {
        return null != config.getDbHost()
                && !config.getDbHost().trim().isEmpty()
                && config.getDbPort() > 0
                && null != config.getDbSchema()
                && !config.getDbSchema().trim().isEmpty();
    }

    /**
     * Stops the In-Time server.
     *
     * @return <code>true</code> if In-Time server has been stopped. Otherwise, <code>false</code>.
     */
    public boolean stopServer() {
        try {
            return serverContainer.stopServer(config.getDbHost(), config.getDbPort(), config.getDbSchema());
        } catch (IOException e) {
            log.error("Error occured while stopping server. {}", e);
            return false;
        }
    }

    /**
     * Verifies the connection properties as specified by the Frontend.
     *
     * @return <code>true</code> if a connection could be established successfully. Otherwise, <code>false</code>.
     */
    public boolean verifyConnectionProperties() {
        if (!checkJdbcDriver("com.sap.db.jdbc.Driver")) return false;
        String url = getConnectionString(this.config);
        connectionVerified = verifyConnection(url);
        return connectionVerified;
    }

    /**
     * Helper method to retrieve all possible server type values.
     *
     * @return all server type values.
     */
    public ServerType[] getServerTypes() {
        return ServerType.values();
    }

    public ExecutorOptimizationStrategy[] getExecutorOptimizationStrategies() {
        return ExecutorOptimizationStrategy.values();
    }

    public boolean updateLicense(){
        try {
            final String licenseString = readLicenseFile();
            new LicenseInstaller().install(licenseString, config.getIntpId(), config.getIntpName(),
                    config.getIntpType().getLabel(), config.getIntpSize(), config.getDbSchema(),
                    config.getDbHost(), config.getDbPort(), config.getDbUser(), config.getDbPassword());
        } catch (Exception e) {
            log.error("Error while installing license: " + e.getMessage(), e);
            jsfHelper.addFacesMessageFromResourceBundle("intp_messages",
                    "org.kr.intp.web.index.license.002", FacesMessage.SEVERITY_INFO);
            return false;
        }
        jsfHelper.addFacesMessageFromResourceBundle("intp_messages",
                "org.kr.intp.web.index.license.001", FacesMessage.SEVERITY_INFO);
        return true;
    }

    public String readLicenseFile(){
        final StringBuilder sb = new StringBuilder();
        String line;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(file.getInputStream(), Charset.forName("UTF8")))) {
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException e) {
            jsfHelper.addFacesMessageFromResourceBundle("intp_messages",
                    "org.kr.intp.config.load.009", FacesMessage.SEVERITY_ERROR);
        }
        return sb.toString();
    }

    public boolean loadConfiguration() {
        if (!connectionVerified) {
            jsfHelper.addFacesMessageFromResourceBundle("intp_messages",
                    "org.kr.intp.config.load.001", FacesMessage.SEVERITY_ERROR);
            return false;
        }
        final String url = getConnectionString(config);
        try (Connection connection = DriverManager.getConnection(url)) {
            if (!checkDbSchemaPrivilege(connection, config))
                return false;
            if (!checkForPropertyTable(connection, config))
                return false;
            final Map<String, String> properties = loadPropertiesFromTable(connection, config);
            updateProperties(properties, config);
            jsfHelper.addFacesMessageFromResourceBundle("intp_messages",
                    "org.kr.intp.config.load.008", FacesMessage.SEVERITY_INFO);
            return true;
        } catch (SQLException e) {
            jsfHelper.addFacesMessageFromResourceBundle("intp_messages", "org.kr.intp.config.connection.002", FacesMessage.SEVERITY_ERROR);
        }
        return false;
    }

    private boolean checkDbSchemaPrivilege(Connection connection, IntpConfigPOJO config) {
        final String query = String.format("SELECT HAS_PRIVILEGES FROM SCHEMAS WHERE SCHEMA_NAME = '%s'", config.getDbSchema());
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                boolean authorized = rs.getBoolean(1);
                if (authorized)
                    return true;
                jsfHelper.addFacesMessageFromResourceBundle("intp_messages",
                        "org.kr.intp.config.load.004", FacesMessage.SEVERITY_ERROR);
                return false;
            }
        } catch (SQLException e) {
            jsfHelper.addFacesMessageFromResourceBundle("intp_messages",
                    "org.kr.intp.config.load.002", FacesMessage.SEVERITY_ERROR);
            return false;
        }
        jsfHelper.addFacesMessageFromResourceBundle("intp_messages",
                "org.kr.intp.config.load.003", FacesMessage.SEVERITY_ERROR);
        return false;
    }

    private boolean checkForPropertyTable(Connection connection, IntpConfigPOJO config) {
        final String query = String.format("SELECT TABLE_NAME FROM TABLES WHERE SCHEMA_NAME = '%s' AND TABLE_NAME = 'PROPERTIES'", config.getDbSchema());
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) return true;
        } catch (SQLException e) {
            jsfHelper.addFacesMessageFromResourceBundle("intp_messages",
                    "org.kr.intp.config.load.005", FacesMessage.SEVERITY_ERROR);
            return false;
        }
        jsfHelper.addFacesMessageFromResourceBundle("intp_messages",
                "org.kr.intp.config.load.006", FacesMessage.SEVERITY_ERROR);
        return false;
    }

    private Map<String, String> loadPropertiesFromTable(Connection connection, IntpConfigPOJO config){
        Map<String, String> properties = new HashMap<> ();
        String query = String.format("SELECT KEY, VALUE FROM \"%s\".\"PROPERTIES\"", config.getDbSchema());
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()){
                properties.put(rs.getString(1), rs.getString(2));
            }
            return properties;
        } catch (SQLException e) {
            jsfHelper.addFacesMessageFromResourceBundle("intp_messages",
                    "org.kr.intp.config.load.007", FacesMessage.SEVERITY_ERROR);
            return new HashMap<> ();
        }
    }

    private boolean updateProperties(Map<String, String> properties, IntpConfigPOJO config){
        // In-Time Instance
        config.setIntpId(properties.get("intp.id"));
        config.setIntpName(properties.get("intp.name"));
        config.setIntpType(ServerType.valueOf(properties.get("intp.type")));
        config.setIntpPort(Integer.parseInt(properties.get("intp.port")));
        config.setIntpSize(Integer.parseInt(properties.get("intp.size")));
        config.setConnectionPoolingEnabled(Boolean.parseBoolean(properties.get("connection.pooling.enabled")));
        config.setServiceConnectionPoolSize(Integer.parseInt(properties.get("service.connection.pool.size")));
        config.setDbLogging(Boolean.parseBoolean(properties.get("db.logging.enabled")));

        // SAP HANA Objects
        config.setDbSchema(properties.get("db.schema"));
        config.setDbGenObjectsSchema(properties.get("db.gen.objects.schema"));
        config.setDbFiCalendarSchema(properties.get("db.ficalendar.schema"));
        config.setDbFiCalendarTable(properties.get("db.ficalendar.table"));

        // Monitoring
        config.setDbAppMonitorFrequency(Integer.parseInt(properties.get("db.app.monitor.frequency")));
        config.setHardwareMonitorEnabled(Boolean.parseBoolean(properties.get("hardware.monitor.enabled")));
        config.setHardwareMonitorFrequency(Integer.parseInt(properties.get("hardware.monitor.frequency")));

        // Runtime Parameters
        config.setAggregationPlaceholder(properties.get("aggr.placeholder"));
        config.setClearEmptyLogActivities(Boolean.parseBoolean(properties.get("clear.empty.log.activities")));
        config.setArchiveLogPeriodFactor(Integer.parseInt(properties.get("archive.log.period.factor")));
        config.setDbLoadThresholdCPU(Integer.parseInt(properties.get("db.load.threshold.cpu")));
        config.setDbLoadThresholdMem(Integer.parseInt(properties.get("db.load.threshold.mem")));
        config.setConcurrentDelta(Boolean.parseBoolean(properties.get("concurrent.delta")));
        config.setDbHaEnabled(Boolean.parseBoolean(properties.get("db.ha.enabled")));
        config.setExecutorOptimizationStrategy(ExecutorOptimizationStrategy.fromLabel(properties.get("executor.optimization.strategy")));

        // Language
        config.setIntpLocaleLanguage(properties.get("intp.locale.language"));
        config.setIntpLocaleCountry(properties.get("intp.locale.country"));

        // Pool Properties
        config.setPoolClass(properties.get("pool.class"));
        config.setAcquireIncrement(Integer.parseInt(properties.get("pool.acquireIncrement")));
        config.setAcquireRetryAttempts(Integer.parseInt(properties.get("pool.acquireRetryAttempts")));
        config.setAcquireRetryDelay(Integer.parseInt(properties.get("pool.acquireRetryDelay")));
        config.setAutoCommitOnClose(Boolean.parseBoolean(properties.get("pool.autoCommitOnClose")));
        config.setAutomaticTestTable(properties.get("pool.automaticTestTable"));
        config.setBreakAfterAcquireFailure(Boolean.parseBoolean(properties.get("pool.breakAfterAcquireFailure")));
        config.setCheckoutTimeout(Integer.parseInt(properties.get("pool.checkoutTimeout")));
        config.setConnectionCustomizerClassName(properties.get("pool.connectionCustomizerClassName"));
        config.setConnectionTesterClassName(properties.get("pool.connectionTesterClassName"));
        config.setContextClassLoaderSource(properties.get("pool.contextClassLoaderSource"));
        config.setDataSourceName(properties.get("pool.dataSourceName"));
        config.setDebugUnreturnedConnectionStackTraces(Boolean.parseBoolean(properties.get("pool.debugUnreturnedConnectionStackTraces")));
        config.setDriverClass(properties.get("pool.driverClass"));
        config.setFactoryClassLocation(properties.get("pool.factoryClassLocation"));
        config.setForceIgnoreUnresolvedTransactions(Boolean.parseBoolean(properties.get("pool.forceIgnoreUnresolvedTransactions")));
        config.setForceSynchronousCheckins(Boolean.parseBoolean(properties.get("pool.forceSynchronousCheckins")));
        config.setForceUseNamedDriverClass(Boolean.parseBoolean(properties.get("pool.forceUseNamedDriverClass")));
        config.setIdleConnectionTestPeriod(Integer.parseInt(properties.get("pool.idleConnectionTestPeriod")));
        config.setInitialPoolSize(Integer.parseInt(properties.get("pool.initialPoolSize")));
        config.setMaxAdministrativeTaskTime(Integer.parseInt(properties.get("pool.maxAdministrativeTaskTime")));
        config.setMaxConnectionAge(Integer.parseInt(properties.get("pool.maxConnectionAge")));
        config.setMaxIdleTime(Integer.parseInt(properties.get("pool.maxIdleTime")));
        config.setMaxIdleTimeExcessConnections(Integer.parseInt(properties.get("pool.maxIdleTimeExcessConnections")));
        config.setMaxPoolSize(Integer.parseInt(properties.get("pool.maxPoolSize")));
        config.setMaxStatements(Integer.parseInt(properties.get("pool.maxStatements")));
        config.setMaxStatementsPerConnection(Integer.parseInt(properties.get("pool.maxStatementsPerConnection")));
        config.setMinPoolSize(Integer.parseInt(properties.get("pool.minPoolSize")));
        config.setNumHelperThreads(Integer.parseInt(properties.get("pool.numHelperThreads")));
        config.setOverrideDefaultUser(properties.get("pool.overrideDefaultUser"));
        config.setOverrideDefaultPassword(properties.get("pool.overrideDefaultPassword"));
        config.setPassword(properties.get("pool.password"));
        config.setPreferredTestQuery(properties.get("pool.preferredTestQuery"));
        config.setPrivilegeSpawnedThreads(Boolean.parseBoolean(properties.get("pool.privilegeSpawnedThreads")));
        config.setPropertyCycle(Integer.parseInt(properties.get("pool.propertyCycle")));
        config.setStatementCacheNumDeferredCloseThreads(Integer.parseInt(properties.get("pool.statementCacheNumDeferredCloseThreads")));
        config.setTestConnectionOnCheckin(Boolean.parseBoolean(properties.get("pool.testConnectionOnCheckin")));
        config.setTestConnectionOnCheckout(Boolean.parseBoolean(properties.get("pool.testConnectionOnCheckout")));
        config.setUnreturnedConnectionTimeout(Integer.parseInt(properties.get("pool.unreturnedConnectionTimeout")));
        config.setUser(properties.get("pool.user"));
        config.setUsesTraditionalReflectiveProxies(Boolean.parseBoolean(properties.get("pool.usesTraditionalReflectiveProxies")));
        return true;
    }

    private boolean checkJdbcDriver(String name) {
        try {
            Class.forName(name);
            return true;
        } catch (ClassNotFoundException e) {
            jsfHelper.addFacesMessageFromResourceBundle("intp_messages", "org.kr.intp.config.connection.001", FacesMessage.SEVERITY_ERROR);
            return false;
        }
    }

    private String getConnectionString(IntpConfigPOJO configPOJO) {
        return DbUtils.createSapJdbcUrl(configPOJO.getDbHost(), configPOJO.getDbPort(),
                configPOJO.getDbUser(), configPOJO.getDbPassword());
    }

    private boolean verifyConnection(String url) {
        try (Connection connection  = DriverManager.getConnection(url)) {
            jsfHelper.addFacesMessageFromResourceBundle("intp_messages", "org.kr.intp.config.connection.003", FacesMessage.SEVERITY_INFO);
            return true;
        } catch (SQLException e) {
            jsfHelper.addFacesMessageFromResourceBundle("intp_messages", "org.kr.intp.config.connection.002", FacesMessage.SEVERITY_ERROR);
        }
        return false;
    }

    public boolean isConnectionVerified() {
        return connectionVerified;
    }

    public void setConnectionVerified(boolean connectionVerified) {
        this.connectionVerified = connectionVerified;
    }

    public Part getFile() {
        return file;
    }

    public void setFile(Part file) {
        this.file = file;
    }

    public String getVersion() {
        return AppContext.getVersion();
    }
}