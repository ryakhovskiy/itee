package org.kr.itee.perfmon.ws.pojo;

/**
 *
 */
public class AutorunPOJO {

    private static final long DEFAULT_TIMEOUT_MS = 0;

    private final long queryProcessAddPeriodMS;
    private final int queryProcessesLimit;
    private final int queryProcessesBatchSize;
    private final long updateProcessAddPeriodMS;
    private final int updateProcessesLimit;
    private final int updateProcessesBatchSize;
    private final long timeoutMS;

    public AutorunPOJO(long queryProcessAddPeriodMS, int queryProcessesLimit, int queryProcessesBatchSize,
                       long updateProcessAddPeriodMS, int updateProcessesLimit, int updateProcessesBatchSize) {
        this(queryProcessAddPeriodMS, queryProcessesLimit, queryProcessesBatchSize,
                updateProcessAddPeriodMS, updateProcessesLimit, updateProcessesBatchSize, DEFAULT_TIMEOUT_MS);
    }

    public AutorunPOJO(long queryProcessAddPeriodMS, int queryProcessesLimit, int queryProcessesBatchSize,
                       long updateProcessAddPeriodMS, int updateProcessesLimit, int updateProcessesBatchSize,
                       long timeoutMS) {
        this.queryProcessAddPeriodMS = queryProcessAddPeriodMS;
        this.queryProcessesLimit = queryProcessesLimit;
        this.queryProcessesBatchSize = queryProcessesBatchSize;
        this.updateProcessAddPeriodMS = updateProcessAddPeriodMS;
        this.updateProcessesLimit = updateProcessesLimit;
        this.updateProcessesBatchSize = updateProcessesBatchSize;
        this.timeoutMS = timeoutMS;
    }

    public long getQueryProcessAddPeriodMS() {
        return queryProcessAddPeriodMS;
    }

    public int getQueryProcessesLimit() {
        return queryProcessesLimit;
    }

    public int getQueryProcessesBatchSize() {
        return queryProcessesBatchSize;
    }

    public long getUpdateProcessAddPeriodMS() {
        return updateProcessAddPeriodMS;
    }

    public int getUpdateProcessesLimit() {
        return updateProcessesLimit;
    }

    public int getUpdateProcessesBatchSize() {
        return updateProcessesBatchSize;
    }

    public long getTimeoutMS() {
        return timeoutMS;
    }

    @Override
    public String toString() {
        return String.format("QPA %d; QPL %d; QPB %d; UPA %d; UPL %d; UPB %d; TMS %d",
                queryProcessAddPeriodMS, queryProcessesLimit, queryProcessesBatchSize,
                updateProcessAddPeriodMS, updateProcessesLimit, updateProcessesBatchSize, timeoutMS);
    }
}

