package org.kr.intp.application.job.hooks;

import org.kr.intp.application.job.JobInitializator;
import org.kr.intp.application.manager.DbApplicationManager;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Created by kr on 12/7/2014.
 */
public class HookManager {

    private static final Object[] DUMMY = new Object[0];
    private final Logger log = LoggerFactory.getLogger(HookManager.class);
    private final boolean isTraceEnabled = log.isTraceEnabled();
    private final String projectId;
    private final Map<HookType, List<Hook>> projectHooks;

    public HookManager(JobInitializator jobInitializator, DbApplicationManager dbAppManager,
                       String projectId, int version) throws SQLException {
        this.projectId = projectId;
        this.projectHooks = new HookReader(jobInitializator, dbAppManager).getHooksByProjectIdAndVersion(projectId, version);
    }

    public void invokeHooks(HookType type, String jobId, long jobTs) {
        invokeHooks(type, jobId, jobTs, DUMMY);
    }

    public void invokeHooks(HookType type, String jobId, long jobTs, Object[] args) {
        if (!projectHooks.containsKey(type)) {
            if (isTraceEnabled)
                log.trace("No hooks found: " + projectId + "; " + type);
            return;
        }
        if (isTraceEnabled)
            log.trace(projectId + "_" + type + ": invoking hooks");
        List<Hook> hooks = projectHooks.get(type);
        for (Hook h : hooks)
            h.execute(jobId, jobTs, args);
    }
}
