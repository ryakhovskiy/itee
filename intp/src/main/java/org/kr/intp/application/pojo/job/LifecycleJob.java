package org.kr.intp.application.pojo.job;

/**
 * Created by kr on 29.01.14.
 */
public class LifecycleJob {

    private final String projectId;
    private final String name;
    private final String procedure;
    private final long periodMS;
    private final long startDate;
    private final long endDate;
    private final int executors;

    public LifecycleJob(String projectId, String name, String procedure, long periodMS, long startDate, long endDate, int executors) {
        this.projectId = projectId;
        this.name = name;
        this.procedure = procedure;
        this.periodMS = periodMS;
        this.startDate = startDate;
        this.endDate = endDate;
        this.executors = executors;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getName() {
        return name;
    }

    public String getProcedure() {
        return procedure;
    }

    public long getPeriodMS() {
        return periodMS;
    }

    public long getStartDate() {
        return startDate;
    }

    public long getEndDate() {
        return endDate;
    }

    public int getExecutorsCount() {
        return executors;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (null == other)
            return false;
        if (!(other instanceof LifecycleJob))
            return false;
        LifecycleJob otherJob = (LifecycleJob)other;
        return otherJob.name.equals(name) && otherJob.projectId.equals(projectId);
    }

    @Override
    public int hashCode() {
        return 11 * projectId.hashCode() + 13 * name.hashCode();
    }

    @Override
    public String toString() {
        return String.format("%s_%s", projectId, name);
    }
}
