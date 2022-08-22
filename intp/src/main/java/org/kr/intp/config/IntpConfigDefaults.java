package org.kr.intp.config;

/**
 * Created by kr on 31.03.2014.
 */
public class IntpConfigDefaults {

    static final String DB_HOST = "localhost";
    static final String DB_INSTANCE = "00";
    static final String DB_USER = "system";
    static final String DB_PASSWORD = "manager";
    static final String DB_SCHEMA = "INTE";
    static final String DB_GEN_OBJECTS_SCHEMA = "INTE_GEN";
    static final String DB_FISCAL_CALENDAR_SCHEMA = "PCS";
    static final String DB_FISCAL_CALENDAR_TABLE = "DIM_FIRM_CALENDAR";
    static final long DB_APP_MONITOR_FREQUENCY_L = 60L;
    static final String DB_APP_MONITOR_FREQUENCY = String.valueOf(DB_APP_MONITOR_FREQUENCY_L);
    static final String AGGREGATION_PLACEHOLDER = "_NO_LIM_";
    static final int INTP_SIZE_DEFAULT_I = 10000;
    static final String INTP_SIZE_DEFAULT = String.valueOf(INTP_SIZE_DEFAULT_I);
    static final boolean CLEAR_EMPTY_LOG_ACTIVITIES_B = true;
    static final String CLEAR_EMPTY_LOG_ACTIVITIES = Boolean.toString(CLEAR_EMPTY_LOG_ACTIVITIES_B);
    static final String JMS_INTERFACE_ENABLED = Boolean.toString(false);
    static final String JMS_BROKER_URL = "tcp://localhost:61616";
    static final String JMS_BROKER_USER = "system";
    static final String JMS_BROKER_PASSWORD = "manager";
    static final String IS_CONNECTION_POOLING_ENABLED = "false";
    static final int SERVICE_CONNECTION_POOL_SIZE_I = 20;
    static final String SERVICE_CONNECTION_POOL_SIZE = String.valueOf(SERVICE_CONNECTION_POOL_SIZE_I);
    static final String JDBC_DRIVER_NAME = "com.sap.db.jdbc.Driver";
    static final int ARCHIVELOG_PERIOD_FACTOR_I = 20;
    static final String ARCHIVELOG_PERIOD_FACTOR = String.valueOf(ARCHIVELOG_PERIOD_FACTOR_I);
    static final int DB_LOAD_THRESHOLD_CPU_I = 98;
    static final String DB_LOAD_THRESHOLD_CPU = String.valueOf(DB_LOAD_THRESHOLD_CPU_I);
    static final int DB_LOAD_THRESHOLD_MEM_I = 98;
    static final String DB_LOAD_THRESHOLD_MEM = String.valueOf(DB_LOAD_THRESHOLD_MEM_I);
    static final long HW_MONITOR_FREQUENCY_L = 10000L;
    static final String HW_MONITOR_FREQUENCY = String.valueOf(HW_MONITOR_FREQUENCY_L);
    static final boolean HW_MONITOR_ENABLED_B = false;
    static final String HW_MONITOR_ENABLED = String.valueOf(HW_MONITOR_ENABLED_B);
    static final boolean CONCURRENT_DELTA_B = false;
    static final String CONCURRENT_DELTA = String.valueOf(CONCURRENT_DELTA_B);
    static final boolean DB_HA_ENABLED_B = true;
    static final String DB_HA_ENABLED = String.valueOf(DB_HA_ENABLED_B);
    static final String EXECUTOR_OPTIMIZATION_STRATEGY = "auto";
    static final String DEFAULT_WORKLOAD_PRIORITY = "INTIME_DEFAULT_PRIORITY";
    static final String CONNECTION_POOL_PROPERTIES = "jdbc.c3p0.properties";

    private IntpConfigDefaults() {
    }

}
