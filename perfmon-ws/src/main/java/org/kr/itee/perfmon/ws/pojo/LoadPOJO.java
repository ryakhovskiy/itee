package org.kr.itee.perfmon.ws.pojo;

/**
 *
 */
public class LoadPOJO {

    private final JdbcPOJO jdbcPOJO;
    private final long schedulerIntervalMS;
    private final int queriesPerInterval;
    private final int concurrentExecutors;
    private final String queryType;
    private final String file;
    private final String fileEncoding;
    private final int executionTimeSeconds;
    private final boolean roundRobin;

    public LoadPOJO(JdbcPOJO jdbcPOJO, long schedulerIntervalMS, int queriesPerInterval, int concurrentExecutors,
                    String queryType, String file, String fileEncoding, int executionTimeSeconds, boolean roundRobin) {
        this.jdbcPOJO = jdbcPOJO;
        this.schedulerIntervalMS = schedulerIntervalMS;
        this.queriesPerInterval = queriesPerInterval;
        this.concurrentExecutors = concurrentExecutors;
        this.queryType = queryType;
        this.file = file;
        this.fileEncoding = fileEncoding;
        this.executionTimeSeconds = executionTimeSeconds;
        this.roundRobin = roundRobin;
    }

    public JdbcPOJO getJdbcPOJO() {
        return jdbcPOJO;
    }

    public long getSchedulerIntervalMS() {
        return schedulerIntervalMS;
    }

    public int getQueriesPerInterval() {
        return queriesPerInterval;
    }

    public int getConcurrentExecutors() {
        return concurrentExecutors;
    }

    public String getQueryType() {
        return queryType;
    }

    public String getFile() {
        return file;
    }

    public String getFileEncoding() {
        return fileEncoding;
    }

    public int getExecutionTimeSeconds() {
        return executionTimeSeconds;
    }

    public boolean isRoundRobin() {
        return roundRobin;
    }
}
