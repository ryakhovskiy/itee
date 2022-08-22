package org.kr.intp.config;

import org.kr.intp.model.pojo.ExecutorOptimizationStrategy;
import org.kr.intp.model.pojo.ServerType;

import java.io.Serializable;

/**
 */
public class IntpConfigPOJO implements Serializable {
    // Connection Properties
    private String dbHost = "";
    private int dbPort;
    private String dbInstance = "";
    private String dbUser = "";
    private String dbPassword = "";
    private boolean connectionPoolingEnabled = true;
    private int serviceConnectionPoolSize = 50;

    // In-Time Instance
    private String intpId = "000";
    private String intpName = "D_PCS_Report_App";
    private ServerType intpType = ServerType.D;
    private int intpPort = 4455;
    private int intpSize = 10000;

    // SAP HANA Objects
    private String dbSchema = "INTP";
    private String dbGenObjectsSchema = "INTP_GEN";
    private String dbFiCalendarSchema = "PCS";
    private String dbFiCalendarTable = "DIM_FIRM_CALENDAR";

    // Runtime Parameters
    private String aggregationPlaceholder = "_NO_LIM_";
    private ExecutorOptimizationStrategy executorOptimizationStrategy = ExecutorOptimizationStrategy.FFDAUTO;
    private boolean clearEmptyLogActivities = true;
    private int archiveLogPeriodFactor = 20;
    private boolean dbHaEnabled = true;
    private boolean dbHaPauseTimeIncreasing =true;
    private boolean concurrentDelta = false;
    private int dbLoadThresholdCPU = 99;
    private int dbLoadThresholdMem = 99;

    // Monitoring
    private boolean hardwareMonitorEnabled = true;
    private int hardwareMonitorFrequency = 5000;
    private int dbAppMonitorFrequency = 20;

    // DbLogging (true by default)
    private boolean dbLogging = true;

    // Localization
    private String intpLocaleLanguage = "en";
    private String intpLocaleCountry = "US";

    // Connection Pooling
    private String poolClass = "C3P0ConnectionPool";
    private int acquireIncrement = 3;
    private int acquireRetryAttempts = 5;
    private int acquireRetryDelay = 1000;
    private boolean autoCommitOnClose = false;
    private String automaticTestTable = null;
    private boolean breakAfterAcquireFailure = false;
    private int checkoutTimeout = 0;
    private String connectionCustomizerClassName = null;
    private String connectionTesterClassName = "com.mchange.v2.c3p0.impl.DefaultConnectionTester";
    private String contextClassLoaderSource = "caller";
    private String dataSourceName = "InTimeDS";
    private boolean debugUnreturnedConnectionStackTraces = false;
    private String driverClass = "com.sap.db.jdbc.Driver";
    private String factoryClassLocation = null;
    private boolean forceIgnoreUnresolvedTransactions = false;
    private boolean forceSynchronousCheckins = false;
    private boolean forceUseNamedDriverClass = false;
    private int idleConnectionTestPeriod = 0;
    private int initialPoolSize = 3;
    private int maxAdministrativeTaskTime = 0;
    private int maxConnectionAge = 60;
    private int maxIdleTime = 30;
    private int maxIdleTimeExcessConnections = 0;
    private int maxPoolSize = 50;
    private int maxStatements = 0;
    private int maxStatementsPerConnection = 10;
    private int minPoolSize = 5;
    private int numHelperThreads = 5;
    private String overrideDefaultUser = null;
    private String overrideDefaultPassword = null;
    private String password = null;
    private String preferredTestQuery = "select 0 from dummy";
    private boolean privilegeSpawnedThreads = false;
    private int propertyCycle = 0;
    private int statementCacheNumDeferredCloseThreads = 0;
    private boolean testConnectionOnCheckin = false;
    private boolean testConnectionOnCheckout = false;
    private int unreturnedConnectionTimeout = 0;
    private String user = null;
    private boolean usesTraditionalReflectiveProxies = false;

    // Timeouts
    private int globalTimeoutsConnectionTimeoutMs = 0;
    private int globalTimeoutsConnectionTotalTimeoutMs = 0;
    private int globalTimeoutsConnectionRetryPauseMs = 0;

    private int globalTimeoutsHookExecutionMaxDurationMs  = 0;
    private int globalTimeoutsHookTotalTimeoutMs = 0;
    private int globalTimeoutsHookRetryPauseMs = 0;

    private int globalTimeoutsKeyProcExecutionMaxDurationMs = 0;
    private int globalTimeoutsKeyProcTotalTimeoutMs = 0;
    private int globalTimeoutsKeyProcRetryPauseMs = 0;

    public IntpConfigPOJO() {

    }

    public String getDbHost() {
        return dbHost;
    }

    public void setDbHost(String dbHost) {
        this.dbHost = dbHost;
    }

    public int getDbPort() {
        return dbPort;
    }

    public void setDbPort(int dbPort) {
        this.dbPort = dbPort;
    }

    public String getDbInstance() {
        return dbInstance;
    }

    public void setDbInstance(String dbInstance) {
        this.dbInstance = dbInstance;
    }

    public String getDbUser() {
        return dbUser;
    }

    public void setDbUser(String dbUser) {
        this.dbUser = dbUser;
    }

    public String getDbPassword() {
        return dbPassword;
    }

    public void setDbPassword(String dbPassword) {
        this.dbPassword = dbPassword;
    }

    public String getIntpId() {
        return intpId;
    }

    public void setIntpId(String intpId) {
        this.intpId = intpId;
    }

    public String getIntpName() {
        return intpName;
    }

    public void setIntpName(String intpName) {
        this.intpName = intpName;
    }

    public ServerType getIntpType() {
        return intpType;
    }

    public void setIntpType(ServerType intpType) {
        this.intpType = intpType;
    }

    public int getIntpPort() {
        return intpPort;
    }

    public void setIntpPort(int intpPort) {
        this.intpPort = intpPort;
    }

    public int getIntpSize() {
        return intpSize;
    }

    public void setIntpSize(int intpSize) {
        this.intpSize = intpSize;
    }

    public String getDbSchema() {
        return dbSchema;
    }

    public void setDbSchema(String dbSchema) {
        this.dbSchema = dbSchema;
    }

    public String getDbGenObjectsSchema() {
        return dbGenObjectsSchema;
    }

    public void setDbGenObjectsSchema(String dbGenObjectsSchema) {
        this.dbGenObjectsSchema = dbGenObjectsSchema;
    }

    public String getDbFiCalendarSchema() {
        return dbFiCalendarSchema;
    }

    public void setDbFiCalendarSchema(String dbFiCalendarSchema) {
        this.dbFiCalendarSchema = dbFiCalendarSchema;
    }

    public String getDbFiCalendarTable() {
        return dbFiCalendarTable;
    }

    public void setDbFiCalendarTable(String dbFiCalendarTable) {
        this.dbFiCalendarTable = dbFiCalendarTable;
    }

    public String getAggregationPlaceholder() {
        return aggregationPlaceholder;
    }

    public void setAggregationPlaceholder(String aggregationPlaceholder) {
        this.aggregationPlaceholder = aggregationPlaceholder;
    }

    public ExecutorOptimizationStrategy getExecutorOptimizationStrategy() {
        return executorOptimizationStrategy;
    }

    public void setExecutorOptimizationStrategy(ExecutorOptimizationStrategy executorOptimizationStrategy) {
        this.executorOptimizationStrategy = executorOptimizationStrategy;
    }

    public boolean isClearEmptyLogActivities() {
        return clearEmptyLogActivities;
    }

    public void setClearEmptyLogActivities(boolean clearEmptyLogActivities) {
        this.clearEmptyLogActivities = clearEmptyLogActivities;
    }

    public int getArchiveLogPeriodFactor() {
        return archiveLogPeriodFactor;
    }

    public void setArchiveLogPeriodFactor(int archiveLogPeriodFactor) {
        this.archiveLogPeriodFactor = archiveLogPeriodFactor;
    }

    public boolean isDbHaEnabled() {
        return dbHaEnabled;
    }

    public void setDbHaEnabled(boolean dbHaEnabled) {
        this.dbHaEnabled = dbHaEnabled;
    }

    public boolean isConcurrentDelta() {
        return concurrentDelta;
    }

    public void setConcurrentDelta(boolean concurrentDelta) {
        this.concurrentDelta = concurrentDelta;
    }

    public int getDbLoadThresholdCPU() {
        return dbLoadThresholdCPU;
    }

    public void setDbLoadThresholdCPU(int dbLoadThresholdCPU) {
        this.dbLoadThresholdCPU = dbLoadThresholdCPU;
    }

    public int getDbLoadThresholdMem() {
        return dbLoadThresholdMem;
    }

    public void setDbLoadThresholdMem(int dbLoadThresholdMem) {
        this.dbLoadThresholdMem = dbLoadThresholdMem;
    }

    public boolean isHardwareMonitorEnabled() {
        return hardwareMonitorEnabled;
    }

    public void setHardwareMonitorEnabled(boolean hardwareMonitorEnabled) {
        this.hardwareMonitorEnabled = hardwareMonitorEnabled;
    }

    public int getHardwareMonitorFrequency() {
        return hardwareMonitorFrequency;
    }

    public void setHardwareMonitorFrequency(int hardwareMonitorFrequency) {
        this.hardwareMonitorFrequency = hardwareMonitorFrequency;
    }

    public int getDbAppMonitorFrequency() {
        return dbAppMonitorFrequency;
    }

    public void setDbAppMonitorFrequency(int dbAppMonitorFrequency) {
        this.dbAppMonitorFrequency = dbAppMonitorFrequency;
    }

    public String getIntpLocaleLanguage() {
        return intpLocaleLanguage;
    }

    public void setIntpLocaleLanguage(String intpLocaleLanguage) {
        this.intpLocaleLanguage = intpLocaleLanguage;
    }

    public String getIntpLocaleCountry() {
        return intpLocaleCountry;
    }

    public void setIntpLocaleCountry(String intpLocaleCountry) {
        this.intpLocaleCountry = intpLocaleCountry;
    }

    public boolean isConnectionPoolingEnabled() {
        return connectionPoolingEnabled;
    }

    public void setConnectionPoolingEnabled(boolean connectionPoolingEnabled) {
        this.connectionPoolingEnabled = connectionPoolingEnabled;
    }

    public int getServiceConnectionPoolSize() {
        return serviceConnectionPoolSize;
    }

    public void setServiceConnectionPoolSize(int serviceConnectionPoolSize) {
        this.serviceConnectionPoolSize = serviceConnectionPoolSize;
    }

    public boolean isDbLogging() {
        return dbLogging;
    }

    public void setDbLogging(boolean dbLogging) {
        this.dbLogging = dbLogging;
    }

    public String getPoolClass() {
        return poolClass;
    }

    public void setPoolClass(String poolClass) {
        this.poolClass = poolClass;
    }

    public int getAcquireIncrement() {
        return acquireIncrement;
    }

    public void setAcquireIncrement(int acquireIncrement) {
        this.acquireIncrement = acquireIncrement;
    }

    public int getAcquireRetryAttempts() {
        return acquireRetryAttempts;
    }

    public void setAcquireRetryAttempts(int acquireRetryAttempts) {
        this.acquireRetryAttempts = acquireRetryAttempts;
    }

    public int getAcquireRetryDelay() {
        return acquireRetryDelay;
    }

    public void setAcquireRetryDelay(int acquireRetryDelay) {
        this.acquireRetryDelay = acquireRetryDelay;
    }

    public boolean isAutoCommitOnClose() {
        return autoCommitOnClose;
    }

    public void setAutoCommitOnClose(boolean autoCommitOnClose) {
        this.autoCommitOnClose = autoCommitOnClose;
    }

    public String getAutomaticTestTable() {
        return automaticTestTable;
    }

    public void setAutomaticTestTable(String automaticTestTable) {
        this.automaticTestTable = automaticTestTable;
    }

    public boolean isBreakAfterAcquireFailure() {
        return breakAfterAcquireFailure;
    }

    public void setBreakAfterAcquireFailure(boolean breakAfterAcquireFailure) {
        this.breakAfterAcquireFailure = breakAfterAcquireFailure;
    }

    public int getCheckoutTimeout() {
        return checkoutTimeout;
    }

    public void setCheckoutTimeout(int checkoutTimeout) {
        this.checkoutTimeout = checkoutTimeout;
    }

    public String getConnectionCustomizerClassName() {
        return connectionCustomizerClassName;
    }

    public void setConnectionCustomizerClassName(String connectionCustomizerClassName) {
        this.connectionCustomizerClassName = connectionCustomizerClassName;
    }

    public String getConnectionTesterClassName() {
        return connectionTesterClassName;
    }

    public void setConnectionTesterClassName(String connectionTesterClassName) {
        this.connectionTesterClassName = connectionTesterClassName;
    }

    public String getContextClassLoaderSource() {
        return contextClassLoaderSource;
    }

    public void setContextClassLoaderSource(String contextClassLoaderSource) {
        this.contextClassLoaderSource = contextClassLoaderSource;
    }

    public String getDataSourceName() {
        return dataSourceName;
    }

    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    public boolean isDebugUnreturnedConnectionStackTraces() {
        return debugUnreturnedConnectionStackTraces;
    }

    public void setDebugUnreturnedConnectionStackTraces(boolean debugUnreturnedConnectionStackTraces) {
        this.debugUnreturnedConnectionStackTraces = debugUnreturnedConnectionStackTraces;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getFactoryClassLocation() {
        return factoryClassLocation;
    }

    public void setFactoryClassLocation(String factoryClassLocation) {
        this.factoryClassLocation = factoryClassLocation;
    }

    public boolean isForceIgnoreUnresolvedTransactions() {
        return forceIgnoreUnresolvedTransactions;
    }

    public void setForceIgnoreUnresolvedTransactions(boolean forceIgnoreUnresolvedTransactions) {
        this.forceIgnoreUnresolvedTransactions = forceIgnoreUnresolvedTransactions;
    }

    public boolean isForceSynchronousCheckins() {
        return forceSynchronousCheckins;
    }

    public void setForceSynchronousCheckins(boolean forceSynchronousCheckins) {
        this.forceSynchronousCheckins = forceSynchronousCheckins;
    }

    public boolean isForceUseNamedDriverClass() {
        return forceUseNamedDriverClass;
    }

    public void setForceUseNamedDriverClass(boolean forceUseNamedDriverClass) {
        this.forceUseNamedDriverClass = forceUseNamedDriverClass;
    }

    public int getIdleConnectionTestPeriod() {
        return idleConnectionTestPeriod;
    }

    public void setIdleConnectionTestPeriod(int idleConnectionTestPeriod) {
        this.idleConnectionTestPeriod = idleConnectionTestPeriod;
    }

    public int getInitialPoolSize() {
        return initialPoolSize;
    }

    public void setInitialPoolSize(int initialPoolSize) {
        this.initialPoolSize = initialPoolSize;
    }

    public int getMaxAdministrativeTaskTime() {
        return maxAdministrativeTaskTime;
    }

    public void setMaxAdministrativeTaskTime(int maxAdministrativeTaskTime) {
        this.maxAdministrativeTaskTime = maxAdministrativeTaskTime;
    }

    public int getMaxConnectionAge() {
        return maxConnectionAge;
    }

    public void setMaxConnectionAge(int maxConnectionAge) {
        this.maxConnectionAge = maxConnectionAge;
    }

    public int getMaxIdleTime() {
        return maxIdleTime;
    }

    public void setMaxIdleTime(int maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
    }

    public int getMaxIdleTimeExcessConnections() {
        return maxIdleTimeExcessConnections;
    }

    public void setMaxIdleTimeExcessConnections(int maxIdleTimeExcessConnections) {
        this.maxIdleTimeExcessConnections = maxIdleTimeExcessConnections;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public void setMaxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
    }

    public int getMaxStatements() {
        return maxStatements;
    }

    public void setMaxStatements(int maxStatements) {
        this.maxStatements = maxStatements;
    }

    public int getMaxStatementsPerConnection() {
        return maxStatementsPerConnection;
    }

    public void setMaxStatementsPerConnection(int maxStatementsPerConnection) {
        this.maxStatementsPerConnection = maxStatementsPerConnection;
    }

    public int getMinPoolSize() {
        return minPoolSize;
    }

    public void setMinPoolSize(int minPoolSize) {
        this.minPoolSize = minPoolSize;
    }

    public int getNumHelperThreads() {
        return numHelperThreads;
    }

    public void setNumHelperThreads(int numHelperThreads) {
        this.numHelperThreads = numHelperThreads;
    }

    public String getOverrideDefaultUser() {
        return overrideDefaultUser;
    }

    public void setOverrideDefaultUser(String overrideDefaultUser) {
        this.overrideDefaultUser = overrideDefaultUser;
    }

    public String getOverrideDefaultPassword() {
        return overrideDefaultPassword;
    }

    public void setOverrideDefaultPassword(String overrideDefaultPassword) {
        this.overrideDefaultPassword = overrideDefaultPassword;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPreferredTestQuery() {
        return preferredTestQuery;
    }

    public void setPreferredTestQuery(String preferredTestQuery) {
        this.preferredTestQuery = preferredTestQuery;
    }

    public boolean isPrivilegeSpawnedThreads() {
        return privilegeSpawnedThreads;
    }

    public void setPrivilegeSpawnedThreads(boolean privilegeSpawnedThreads) {
        this.privilegeSpawnedThreads = privilegeSpawnedThreads;
    }

    public int getPropertyCycle() {
        return propertyCycle;
    }

    public void setPropertyCycle(int propertyCycle) {
        this.propertyCycle = propertyCycle;
    }

    public int getStatementCacheNumDeferredCloseThreads() {
        return statementCacheNumDeferredCloseThreads;
    }

    public void setStatementCacheNumDeferredCloseThreads(int statementCacheNumDeferredCloseThreads) {
        this.statementCacheNumDeferredCloseThreads = statementCacheNumDeferredCloseThreads;
    }

    public boolean isTestConnectionOnCheckin() {
        return testConnectionOnCheckin;
    }

    public void setTestConnectionOnCheckin(boolean testConnectionOnCheckin) {
        this.testConnectionOnCheckin = testConnectionOnCheckin;
    }

    public boolean isTestConnectionOnCheckout() {
        return testConnectionOnCheckout;
    }

    public void setTestConnectionOnCheckout(boolean testConnectionOnCheckout) {
        this.testConnectionOnCheckout = testConnectionOnCheckout;
    }

    public int getUnreturnedConnectionTimeout() {
        return unreturnedConnectionTimeout;
    }

    public void setUnreturnedConnectionTimeout(int unreturnedConnectionTimeout) {
        this.unreturnedConnectionTimeout = unreturnedConnectionTimeout;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public boolean isUsesTraditionalReflectiveProxies() {
        return usesTraditionalReflectiveProxies;
    }

    public void setUsesTraditionalReflectiveProxies(boolean usesTraditionalReflectiveProxies) {
        this.usesTraditionalReflectiveProxies = usesTraditionalReflectiveProxies;
    }

    public boolean isDbHaPauseTimeIncreasing() {
        return dbHaPauseTimeIncreasing;
    }

    public void setDbHaPauseTimeIncreasing(boolean dbHaPauseTimeIncreasing) {
        this.dbHaPauseTimeIncreasing = dbHaPauseTimeIncreasing;
    }

    public int getGlobalTimeoutsConnectionTimeoutMs() {
        return globalTimeoutsConnectionTimeoutMs;
    }

    public void setGlobalTimeoutsConnectionTimeoutMs(int globalTimeoutsConnectionTimeoutMs) {
        this.globalTimeoutsConnectionTimeoutMs = globalTimeoutsConnectionTimeoutMs;
    }

    public int getGlobalTimeoutsConnectionTotalTimeoutMs() {
        return globalTimeoutsConnectionTotalTimeoutMs;
    }

    public void setGlobalTimeoutsConnectionTotalTimeoutMs(int globalTimeoutsConnectionTotalTimeoutMs) {
        this.globalTimeoutsConnectionTotalTimeoutMs = globalTimeoutsConnectionTotalTimeoutMs;
    }

    public int getGlobalTimeoutsConnectionRetryPauseMs() {
        return globalTimeoutsConnectionRetryPauseMs;
    }

    public void setGlobalTimeoutsConnectionRetryPauseMs(int globalTimeoutsConnectionRetryPauseMs) {
        this.globalTimeoutsConnectionRetryPauseMs = globalTimeoutsConnectionRetryPauseMs;
    }

    public int getGlobalTimeoutsHookExecutionMaxDurationMs() {
        return globalTimeoutsHookExecutionMaxDurationMs;
    }

    public void setGlobalTimeoutsHookExecutionMaxDurationMs(int globalTimeoutsHookExecutionMaxDurationMs) {
        this.globalTimeoutsHookExecutionMaxDurationMs = globalTimeoutsHookExecutionMaxDurationMs;
    }

    public int getGlobalTimeoutsHookTotalTimeoutMs() {
        return globalTimeoutsHookTotalTimeoutMs;
    }

    public void setGlobalTimeoutsHookTotalTimeoutMs(int globalTimeoutsHookTotalTimeoutMs) {
        this.globalTimeoutsHookTotalTimeoutMs = globalTimeoutsHookTotalTimeoutMs;
    }

    public int getGlobalTimeoutsHookRetryPauseMs() {
        return globalTimeoutsHookRetryPauseMs;
    }

    public void setGlobalTimeoutsHookRetryPauseMs(int globalTimeoutsHookRetryPauseMs) {
        this.globalTimeoutsHookRetryPauseMs = globalTimeoutsHookRetryPauseMs;
    }

    public int getGlobalTimeoutsKeyProcExecutionMaxDurationMs() {
        return globalTimeoutsKeyProcExecutionMaxDurationMs;
    }

    public void setGlobalTimeoutsKeyProcExecutionMaxDurationMs(int globalTimeoutsKeyProcExecutionMaxDurationMs) {
        this.globalTimeoutsKeyProcExecutionMaxDurationMs = globalTimeoutsKeyProcExecutionMaxDurationMs;
    }

    public int getGlobalTimeoutsKeyProcTotalTimeoutMs() {
        return globalTimeoutsKeyProcTotalTimeoutMs;
    }

    public void setGlobalTimeoutsKeyProcTotalTimeoutMs(int globalTimeoutsKeyProcTotalTimeoutMs) {
        this.globalTimeoutsKeyProcTotalTimeoutMs = globalTimeoutsKeyProcTotalTimeoutMs;
    }

    public int getGlobalTimeoutsKeyProcRetryPauseMs() {
        return globalTimeoutsKeyProcRetryPauseMs;
    }

    public void setGlobalTimeoutsKeyProcRetryPauseMs(int globalTimeoutsKeyProcRetryPauseMs) {
        this.globalTimeoutsKeyProcRetryPauseMs = globalTimeoutsKeyProcRetryPauseMs;
    }

}
