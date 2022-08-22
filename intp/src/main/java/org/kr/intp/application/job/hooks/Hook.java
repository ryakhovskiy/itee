package org.kr.intp.application.job.hooks;

import org.kr.intp.application.job.JobInitializator;
import org.kr.intp.application.job.retry.RetryingSapCallableStatementInvoker;
import org.kr.intp.application.manager.DbApplicationManager;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.TimeController;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import org.kr.intp.util.retry.RetryingInvokable;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Hook {

    private static final HookLogger HOOK_LOGGER = HookLogger.getInstance();
    private final Logger log = LoggerFactory.getLogger(Hook.class);
    private final boolean isTraceEnabled = log.isTraceEnabled();
    private final boolean enabled;
    private final String projectId;
    private final int projectVersion;
    private final String name;
    private final HookType type;
    private final String stype;
    private final String procSchema;
    private final String procName;
    private final boolean async;
    private final ExecutorService service;
    private final String logPrefix;
    private final long hookExecutionMaxDurationMS;
    private final long hookTotalTimeoutMS;
    private final long hookRetryPauseMS;
    private final JobInitializator jobInitializator;
    private final DbApplicationManager dbApplicationManager;

    Hook(JobInitializator jobInitializator, DbApplicationManager dbApplicationManager, String projectId, int projectVersion, String name, boolean enabled, HookType type,
         String procSchema, String procName, boolean async, int executors,
         long hookExecutionMaxDurationMS, long hookTotalTimeoutMS,long hookRetryPauseMS) {
        this.jobInitializator = jobInitializator;
        this.dbApplicationManager = dbApplicationManager;
        this.projectId = projectId;
        this.projectVersion = projectVersion;
        this.enabled = enabled;
        this.name = name;
        this.type = type;
        this.stype = type.toString();
        this.procSchema = procSchema;
        this.procName = procName;
        this.async = async;
        this.logPrefix = String.format("%s_%s_%s", projectId, name, type);
        this.hookExecutionMaxDurationMS = hookExecutionMaxDurationMS;
        this.hookRetryPauseMS = hookRetryPauseMS;
        this.hookTotalTimeoutMS = hookTotalTimeoutMS;
        if (!async)
            this.service = null;
        else
            this.service = Executors.newFixedThreadPool(executors);
    }

    public void execute(final String jobId, final long jobTs, final Object[] args) {
        if (!enabled)
            return;
        if (async) {
            final Runnable task = new Runnable() {
                @Override
                public void run() {
                    if (isTraceEnabled)
                        log.trace(logPrefix + ": launching async hook");
                    runHookWithRetries(jobId, jobTs, args);
                }
            };
            service.submit(task);
        } else {
            if (isTraceEnabled)
                log.trace(logPrefix + ": launching sync hook");
            runHookWithRetries(jobId, jobTs, args);
        }

    }

    private void runHookWithRetries(final String jobId, final long jobTs, final Object[] args) {
        final RetryingSapCallableStatementInvoker invoker =
                new RetryingSapCallableStatementInvoker(hookRetryPauseMS, hookExecutionMaxDurationMS);
        final RetryingInvokable invokable = new RetryingInvokable() {
            @Override
            public Object invoke() throws Exception {
                try {
                    runTask(jobId, jobTs, args);
                } catch (Exception e) {
                    log.error(logPrefix + ": HOOK ERROR", e);
                }
                return null;
            }
        };
        invoker.invokeWithRetries(invokable, hookTotalTimeoutMS);
    }

    private void runTask(final String jobId, final long jobTs, final Object[] args) throws SQLException {
        final long start = TimeController.getInstance().getServerUtcTimeMillis();
        HOOK_LOGGER.log(projectId, projectVersion, name, stype, jobId, jobTs, args, "Hook scheduled",
                TimeController.getInstance().getServerUtcTimeMillis());
        Connection connection = null;
        CallableStatement statement = null;
        String errorMessage = "";
        boolean errorOccurs = false;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            final StringBuilder sql = new StringBuilder();
            sql.append("call \"").append(procSchema).append("\".\"").append(procName).append("\"(?,?");
            for (Object o : args)
                sql.append(",?");
            sql.append(')');
            if (isTraceEnabled) {
                log.trace(logPrefix + ": HOOK SQL: " + sql.toString());
                log.trace(logPrefix + ": HOOK PARAMS: " + jobId + "; " + jobTs + "; " + Arrays.toString(args));
            }
            statement = connection.prepareCall(sql.toString());
            statement.setString(1, jobId);
            statement.setTimestamp(2, new Timestamp(jobTs));
            for (int i = 0; i < args.length; i++) {
                statement.setObject(i + 3, args[i]);
            }
            statement.execute();
            long duration = TimeController.getInstance().getServerUtcTimeMillis() - start;
            if (isTraceEnabled) {
                log.trace(logPrefix + ": HOOK EXECUTION took (ms): " + duration
                        + " args: " + Arrays.toString(args));
            }
            HOOK_LOGGER.log(projectId, projectVersion, name, stype, jobId, jobTs, args, "Hook finished",
                    TimeController.getInstance().getServerUtcTimeMillis());
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            HOOK_LOGGER.log(projectId, projectVersion, name, stype, jobId, jobTs, args, "HOOK ERROR: "
                    + e.getMessage(), TimeController.getInstance().getServerUtcTimeMillis());
            errorOccurs = true;
            errorMessage = e.getMessage();
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
        if (errorOccurs) {
            dbApplicationManager.stopOnError(projectId, errorMessage);
            jobInitializator.interruptJob(projectId);
        }
    }

    public String toString() {
        return String.format("HOOK; P: %s; A: %b; SP: %s.%s", logPrefix, async, procSchema, procName);
    }
}
