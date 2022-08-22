package org.kr.intp.application.job.hooks;

import org.kr.intp.application.AppContext;
import org.kr.intp.application.job.JobInitializator;
import org.kr.intp.application.manager.DbApplicationManager;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.TimeController;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.*;
import java.util.*;

/**
 * Created by kr on 12/6/2014.
 */
public class HookReader {

    private static final String SCHEMA = AppContext.instance().getConfiguration().getIntpSchema();
    private static final Logger log = LoggerFactory.getLogger(HookReader.class);
    private static final boolean isTraceEnabled = log.isTraceEnabled();
    private static final String SQL_BY_ID_AND_TYPE = "select project_id, project_version, name, enabled, type, proc_schema, proc_name, is_async, executors from " +
            SCHEMA +
            ".HOOKS where project_id = ? and type = ? and enabled = 'Y' and project_version = ?";

    private static final String SQL_BY_ID = "select project_id, project_version, name, enabled, type, proc_schema, proc_name, is_async, executors from " +
            SCHEMA +
            ".HOOKS where project_id = ? and enabled = 'Y' and project_version = ?";

    private final JobInitializator jobInitializator;
    private final DbApplicationManager dbAppManager;

    public HookReader(JobInitializator jobInitializator, DbApplicationManager dbAppManager) {
        this.jobInitializator = jobInitializator;
        this.dbAppManager = dbAppManager;
    }

    public List<Hook> getPreDeltaHooks(String projectId, int version) {
        return getHooksByProjectIdAndType(projectId, version, HookType.PRE_DELTA);
    }

    public List<Hook> getPostDeltaHooks(String projectId, int version) {
        return getHooksByProjectIdAndType(projectId, version, HookType.POST_DELTA);
    }

    public List<Hook> getPreInitialHooks(String projectId, int version) {
        return getHooksByProjectIdAndType(projectId, version, HookType.PRE_INITIAL);
    }

    public List<Hook> getPostInitialHooks(String projectId, int version) {
        return getHooksByProjectIdAndType(projectId, version, HookType.POST_INITIAL);
    }

    public List<Hook> getPreInitialSequentialHooks(String projectId, int version) {
        return getHooksByProjectIdAndType(projectId, version, HookType.PRE_INITIAL_SEQUENTIAL);
    }

    public List<Hook> getPostInitialSequentialHooks(String projectId, int version) {
        return getHooksByProjectIdAndType(projectId, version, HookType.POST_INITIAL_SEQUENTIAL);
    }

    public List<Hook> getPreDeltaSequentialHooks(String projectId, int version) {
        return getHooksByProjectIdAndType(projectId, version, HookType.PRE_DELTA_SEQUENTIAL);
    }

    public List<Hook> getPostDeltaSequentialHooks(String projectId, int version) {
        return getHooksByProjectIdAndType(projectId, version, HookType.POST_DELTA_SEQUENTIAL);
    }

    private List<Hook> getHooksByProjectIdAndType(String projectId, int version, HookType type) {
        try {
            return getHooksFromDB(projectId, version, type);
        } catch (Exception e) {
            log.error("Cannot find hooks: " + projectId + "; " + type);
            return Collections.EMPTY_LIST;
        }
    }

    private List<Hook> getHooksFromDB(String projectId, int version, HookType type) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet set = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.prepareStatement(SQL_BY_ID_AND_TYPE);
            statement.setString(1, projectId);
            statement.setString(2, type.toString());
            statement.setInt(3, version);
            set = statement.executeQuery();
            List<Hook> hooks = new ArrayList<Hook>();
            while (set.next()) {
                String name = set.getString(3);
                boolean enabled = set.getString(4).toLowerCase().equals("y");
                String procSchema = set.getString(6);
                String procName = set.getString(7);
                boolean isAsync = set.getString(8).toLowerCase().equals("y");
                int executors = set.getInt(9);
                long hookExecutionMaxDurationMS = getLongProperty(set, "hook_execution_max_duration_ms");
                long hookTotalTimeoutMS = getLongProperty(set, "hook_total_timeout_ms");
                long hookRetryPauseMS = getLongProperty(set, "hook_retry_pause_ms");

                hooks.add(new Hook(jobInitializator, dbAppManager, projectId, version, name, enabled, type, procSchema,
                        procName, isAsync, executors, hookExecutionMaxDurationMS, hookTotalTimeoutMS, hookRetryPauseMS));
            }
            return hooks;
        } finally {
            if (null != set)
                set.close();
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    public Map<HookType, List<Hook>> getHooksByProjectIdAndVersion(String projectId, int version) throws SQLException {
        long start = TimeController.getInstance().getServerUtcTimeMillis();
        if (isTraceEnabled)
            log.trace("loading hooks for project: " + projectId);
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet set = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.prepareStatement(SQL_BY_ID);
            statement.setString(1, projectId);
            statement.setInt(2, version);
            set = statement.executeQuery();
            Map<HookType, List<Hook>> hooks = new HashMap<HookType, List<Hook>>();
            while (set.next()) {
                String name = set.getString(3);
                boolean enabled = set.getString(4).toLowerCase().equals("y");
                String stype = set.getString(5).toUpperCase();
                HookType type = HookType.valueOf(stype);
                String procSchema = set.getString(6);
                String procName = set.getString(7);
                boolean isAsync = set.getString(8).toLowerCase().equals("y");
                int executors = set.getInt(9);

                long hookExecutionMaxDurationMS = getLongProperty(set, "hook_execution_max_duration_ms");
                long hookTotalTimeoutMS = getLongProperty(set, "hook_total_timeout_ms");
                long hookRetryPauseMS = getLongProperty(set, "hook_retry_pause_ms");

                Hook hook = new Hook(jobInitializator, dbAppManager, projectId, version, name, enabled, type,
                        procSchema, procName, isAsync, executors, hookExecutionMaxDurationMS, hookTotalTimeoutMS, hookRetryPauseMS);
                if (!hooks.containsKey(type))
                    hooks.put(type, new ArrayList<Hook>());
                hooks.get(type).add(hook);
                if (isTraceEnabled)
                    log.trace("Hook found: " + hook);
            }
            if (isTraceEnabled)
                log.trace("loading hooks took (ms): " +
                        (TimeController.getInstance().getServerUtcTimeMillis() - start));
            return hooks;
        } finally {
            if (null != set)
                set.close();
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private long getLongProperty(ResultSet resultSet, String columnName) throws SQLException {
        String columnNameUpperCase = columnName.toUpperCase();
        boolean columnExists = false;
        ResultSetMetaData rsMD = resultSet.getMetaData();
        for (int i = 1; i <= rsMD.getColumnCount(); i++) {
            if (rsMD.getColumnName(i).equals(columnNameUpperCase)) {
                columnExists = true;
                break;
            }
        }
        if (columnExists)
            return resultSet.getLong(columnNameUpperCase);
        else
            return AppContext.instance().getConfiguration().getDefaultGlobalTimeouts(columnName);
    }
}
