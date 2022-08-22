package org.kr.intp.application.pojo.job;

import org.kr.intp.application.AppContext;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kr on 7/13/2014.
 */
public class JobProperties {


    private static final IntpConfig CONFIG = AppContext.instance().getConfiguration();
    private static final String INTIME_SCHEMA = CONFIG.getIntpSchema();

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String QUERY = "select p.PARAMETERS from " + INTIME_SCHEMA + ".RT_PARAMETERS p\n" +
            "where p.PROJECT_ID = ? and p.version = ? and p.ts = \n" +
            "(select max(ts) from " + INTIME_SCHEMA + ".RT_PARAMETERS where PROJECT_ID = p.PROJECT_ID and version = p.version)";

    public static JobProperties[] loadProperties(String projectId, int version) throws SQLException, IOException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.prepareStatement(QUERY);
            statement.setString(1, projectId);
            statement.setInt(2, version);
            resultSet = statement.executeQuery();
            if (!resultSet.next())
                throw new RuntimeException(String.format("No parameters for %s v.%d", projectId, version));
            final String json = resultSet.getString(1);
            return parseProps(json);
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private static JobProperties[] parseProps(String json) throws IOException {
        final Map map = mapper.readValue(json, Map.class);
        Map initial = (Map)map.get("initial");
        Map delta = (Map)map.get("delta");
        int sla = (Integer)map.get("sla");
        return new JobProperties[] { new JobProperties(initial, sla), new JobProperties(delta, sla) };
    }

    private final long startdate;
    private final long enddate;
    private final long period;
    private final int processExecutors;
    private final boolean fullload;
    private final long sla;
    private final String flRestriction;
    private final Map<String, Boolean> calendarInfo = new HashMap<String, Boolean>();
    private final SchedulingMethod schedulingMethod;
    private final PeriodChoice periodChoice;
    private final ConnectionType connectionType;

    private final long keyProcessingExecutionMaxDurationMS;
    private final long keyProcessingTotalTimeoutMS;
    private final long keyProcessingRetryPauseMS;

    private final long connectionEstablishingMaxDurationMS;
    private final long connectionTotalTimeoutMS;
    private final long connectionRetryPauseMS;


    private JobProperties(Map props, long sla) {
        this.sla = sla;
        this.startdate = parseStartDate(props);
        this.enddate = parseEndDate(props);
        this.fullload = props.containsKey("fullload") && props.get("fullload").toString().equals("true");
        this.period = (Integer)props.get("period");
        this.processExecutors = (Integer)props.get("executors");
        this.flRestriction = parseFullLoadRestrictions(props);
        this.schedulingMethod = parseSchedulingMethod(props);
        this.periodChoice = parsePeriodChoice(props);
        if (props.containsKey("calendar"))
            calendarInfo.putAll((Map)props.get("calendar"));
        this.connectionType = parseConnectionType(props.get("conn_type"));

        this.keyProcessingExecutionMaxDurationMS = parseLongProperty(props, "key_proc_execution_max_duration_ms");
        this.keyProcessingTotalTimeoutMS = parseLongProperty(props, "key_proc_total_timeout_ms");
        this.keyProcessingRetryPauseMS = parseLongProperty(props, "key_proc_retry_pause_ms");

        this.connectionEstablishingMaxDurationMS = parseLongProperty(props, "connection_timeout_ms");
        this.connectionTotalTimeoutMS = parseLongProperty(props, "connection_total_timeout_ms");
        this.connectionRetryPauseMS = parseLongProperty(props, "connection_retry_pause_ms");
    }

    private long parseLongProperty(Map props, String propertyName) {
        if (props.containsKey(propertyName))
            return (long)props.get(propertyName);
        return CONFIG.getDefaultGlobalTimeouts(propertyName);
    }

    private PeriodChoice parsePeriodChoice(Map props) {
        final PeriodChoice defaultPeriodChoice = PeriodChoice.SECONDS;
        return props.containsKey("periodchoice") ?
                PeriodChoice.valueOf(props.get("periodchoice").toString()) : defaultPeriodChoice;
    }

    private SchedulingMethod parseSchedulingMethod(Map props) {
        final SchedulingMethod defaultSchedulingMethod = SchedulingMethod.PERIOD;
        return props.containsKey("scheduling") ?
                SchedulingMethod.valueOf(props.get("scheduling").toString()) : defaultSchedulingMethod;
    }

    private String parseFullLoadRestrictions(Map props) {
        return props.containsKey("flrestrictions") ?
                props.get("flrestrictions").toString().trim() : "";
    }

    private long parseEndDate(Map props) {
        return props.containsKey("enddate") ?
                java.sql.Timestamp.valueOf((String)props.get("enddate")).getTime() : 0;
    }

    private long parseStartDate(Map props) {
        return props.containsKey("startdate") ?
                java.sql.Timestamp.valueOf((String)props.get("startdate")).getTime() : 0;
    }

    private ConnectionType parseConnectionType(Object connType) {
        if (null == connType)
            return ConnectionType.UNDEFINED;
        String type = connType.toString().trim().toLowerCase();
        if ("simple".equals(type))
            return ConnectionType.SIMPLE;
        else if ("history".equals(type))
            return ConnectionType.HISTORY;
        else if ("pool".equals(type))
            return ConnectionType.POOL;
        else
            return ConnectionType.UNDEFINED;
    }

    public long getStartdate() {
        return startdate;
    }

    public long getEnddate() {
        return enddate;
    }

    public long getPeriod() {
        return period;
    }

    public int getProcessExecutors() {
        return processExecutors;
    }

    public boolean isFullload() { return fullload; }

    public String getFullLoadRestrictions() {
        return flRestriction;
    }

    public long getSla() { return sla; }

    public ConnectionType getConnectionType() {
        return connectionType;
    }

    public SchedulingMethod getSchedulingMethod() { return schedulingMethod; }
    public Map<String, Boolean> getCalendarInfo() { return calendarInfo; }
    public PeriodChoice getPeriodChoice() { return periodChoice; }

    public long getKeyProcessingExecutionMaxDurationMS() {
        return keyProcessingExecutionMaxDurationMS;
    }

    public long getKeyProcessingTotalTimeoutMS() {
        return keyProcessingTotalTimeoutMS;
    }

    public long getKeyProcessingRetryPauseMS() {
        return keyProcessingRetryPauseMS;
    }

    public long getConnectionEstablishingMaxDurationMS() {
        return connectionEstablishingMaxDurationMS;
    }

    public long getConnectionTotalTimeoutMS() {
        return connectionTotalTimeoutMS;
    }

    public long getConnectionRetryPauseMS() {
        return connectionRetryPauseMS;
    }

    @Override
    public String toString() {
        return String.format(
                "START: %d; END: %d; PERIOD: %d; EXECUTORS: %d; SLA: %d, FL: %b, FLR: %s, SCH: %s, PC: %s, CLNDR: %s; cn: %s",
                startdate, enddate, period, processExecutors, sla, fullload, flRestriction, schedulingMethod, periodChoice, calendarInfo, connectionType.toString());
    }

    public enum SchedulingMethod {
        PERIOD,
        CALENDAR
    }

    public enum PeriodChoice {
        SECONDS,
        HOUR,
        DAY,
        WEEK,
        MONTH,
        YEAR
    }

    public enum ConnectionType {
        SIMPLE,
        POOL,
        HISTORY,
        UNDEFINED
    }
}
