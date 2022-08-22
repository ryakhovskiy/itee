package org.kr.db.loader.ui.pojo;

import java.io.Serializable;

/**
 * Created bykron 30.07.2014.
 */
public class Scenario implements Serializable {

    private final int queryProcPeriod;
    private final int updateProcPeriod;
    private final int timeout;
    private final int queryMaxProcesses;
    private final int updateMaxProcesses;
    private final int queryProcessesCount;
    private final int updateProcessesCount;

    public Scenario(int queryProcPeriod, int updateProcPeriod, int timeout, int queryMaxProcesses,
                    int updateMaxProcesses, int queryProcessesCount, int updateProcessesCount) {
        this.queryProcPeriod = queryProcPeriod;
        this.updateProcPeriod = updateProcPeriod;
        this.timeout = timeout;
        this.queryMaxProcesses = queryMaxProcesses;
        this.updateMaxProcesses = updateMaxProcesses;
        this.queryProcessesCount = queryProcessesCount;
        this.updateProcessesCount = updateProcessesCount;
    }

    public int getQueryProcPeriod() {
        return queryProcPeriod;
    }

    public int getUpdateProcPeriod() {
        return updateProcPeriod;
    }

    public int getTimeout() {
        return timeout;
    }

    public int getQueryMaxProcesses() {
        return queryMaxProcesses;
    }

    public int getUpdateMaxProcesses() {
        return updateMaxProcesses;
    }

    public int getQueryProcessesCount() {
        return queryProcessesCount;
    }

    public int getUpdateProcessesCount() {
        return updateProcessesCount;
    }
}
