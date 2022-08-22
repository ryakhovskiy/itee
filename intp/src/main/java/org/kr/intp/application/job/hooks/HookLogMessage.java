package org.kr.intp.application.job.hooks;

/**
 * Created by kr on 12/7/2014.
 */
public class HookLogMessage {

    private final String projectId;
    private final int projectVersion;
    private final String name;
    private final String type;
    private final String jobId;
    private final long jobTs;
    private final Object[] args;
    private final String message;
    private final long time;

    public HookLogMessage(String projectId, int version, String name, String type, String jobId, long jobTs, Object[] args,
                          String message, long time) {
        this.projectId = projectId;
        this.projectVersion = version;
        this.name = name;
        this.type = type;
        this.jobId = jobId;
        this.jobTs = jobTs;
        this.args = args;
        this.message = message;
        this.time = time;
    }

    public String getProjectId() {
        return projectId;
    }

    public int getProjectVersion() { return projectVersion; }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getJobId() {
        return jobId;
    }

    public long getJobTs() {
        return jobTs;
    }

    public Object[] getArgs() {
        return args;
    }

    public String getMessage() {
        return message;
    }

    public long getTime() { return time; }
}
