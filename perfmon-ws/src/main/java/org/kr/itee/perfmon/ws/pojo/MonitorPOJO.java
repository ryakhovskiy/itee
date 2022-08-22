package org.kr.itee.perfmon.ws.pojo;

/**
 *
 */
public class MonitorPOJO {

    private final JdbcPOJO jdbcPOJO;
    private final String server;
    private final String outfile;
    private final int monitorAgeSeconds;
    private final int topXValues;
    private final long monitorQueryIntervalMS;
    private final long expensiveStatementsDurationMS;
    private final long powerConsumtionMonitorDurationMS;
    private final int powerConcumtionMonitorPort;

    public MonitorPOJO(JdbcPOJO jdbcPOJO, String outfile, int monitorAgeSeconds, int topXValues,
                       long monitorQueryIntervalMS, long expensiveStatementsDurationMS,
                       long powerConsumtionMonitorDurationMS, int powerConcumtionMonitorPort) {
        this.jdbcPOJO = jdbcPOJO;
        this.server = null == jdbcPOJO ? "" : jdbcPOJO.getHost().split(":", -1)[0];
        this.outfile = outfile;
        this.monitorAgeSeconds = monitorAgeSeconds;
        this.topXValues = topXValues;
        this.monitorQueryIntervalMS = monitorQueryIntervalMS;
        this.expensiveStatementsDurationMS = expensiveStatementsDurationMS;
        this.powerConsumtionMonitorDurationMS = powerConsumtionMonitorDurationMS;
        this.powerConcumtionMonitorPort = powerConcumtionMonitorPort;
    }

    public JdbcPOJO getJdbcPOJO() {
        return jdbcPOJO;
    }

    public String getOutfile() {
        return outfile;
    }

    public int getMonitorAgeSeconds() {
        return monitorAgeSeconds;
    }

    public int getTopXValues() {
        return topXValues;
    }

    public long getMonitorQueryIntervalMS() {
        return monitorQueryIntervalMS;
    }

    public long getExpensiveStatementsDurationMS() {
        return expensiveStatementsDurationMS;
    }

    public long getPowerConsumtionMonitorDurationMS() {
        return powerConsumtionMonitorDurationMS;
    }

    public int getPowerConcumtionMonitorPort() {
        return powerConcumtionMonitorPort;
    }

    public String getServer() {
        return server;
    }
}
