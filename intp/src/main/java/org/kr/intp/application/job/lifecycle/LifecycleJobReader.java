package org.kr.intp.application.job.lifecycle;

import org.kr.intp.application.AppContext;
import org.kr.intp.application.pojo.job.LifecycleJob;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created bykron 09.09.2014.
 */
public class LifecycleJobReader {

    private static final LifecycleJobReader instance = new LifecycleJobReader();

    public static LifecycleJobReader getInstance() { return instance; }

    private final Logger log = LoggerFactory.getLogger(LifecycleJobReader.class);

    private LifecycleJobReader() { }

    public LifecycleJob[] getActiveLifecycleJobs(String projectId) throws SQLException {
        log.trace("searching LC jobs for project: " + projectId);
        ServiceConnectionPool connectionManager = ServiceConnectionPool.instance();
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = connectionManager.getConnection();
            statement = connection.prepareStatement(project_lc_sql);
            statement.setString(1, projectId);
            statement.execute();
            resultSet = statement.getResultSet();
            List<LifecycleJob> jobs = new ArrayList<LifecycleJob>();
            while (resultSet.next()) {
                final LifecycleJob lc = parseResultSet(resultSet);
                jobs.add(lc);
            }
            log.trace("LC jobs for project " + projectId + " found: " + jobs);
            return jobs.toArray(new LifecycleJob[jobs.size()]);
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    public LifecycleJob[] getActiveLifecycleJobs() throws SQLException {
        log.trace("searching common LC jobs...");
        ServiceConnectionPool connectionManager = ServiceConnectionPool.instance();
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = connectionManager.getConnection();
            statement = connection.prepareStatement(lc_sql);
            statement.execute();
            resultSet = statement.getResultSet();
            List<LifecycleJob> jobs = new ArrayList<LifecycleJob>();
            while (resultSet.next()) {
                final LifecycleJob lc = parseResultSet(resultSet);
                jobs.add(lc);
            }
            log.trace("common LC jobs found: " + jobs);
            return jobs.toArray(new LifecycleJob[jobs.size()]);
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private LifecycleJob parseResultSet(ResultSet set) throws SQLException {
        String projectId = set.getString(1);
        if (null == projectId)
            projectId = "";
        String name = set.getString(2);
        String procedure = set.getString(3);
        int periodS = set.getInt(4);
        long periodMS = TimeUnit.SECONDS.toMillis(periodS);
        Timestamp start = set.getTimestamp(5);
        long startDate = 0;
        if (null != start)
            startDate = start.getTime();
        Timestamp end = set.getTimestamp(6);
        long endDate = 0;
        if (null != end)
            endDate = end.getTime();
        int executors = set.getInt(7);
        return new LifecycleJob(projectId, name, procedure, periodMS, startDate, endDate, executors);
    }

    private static final String SCHEMA = AppContext.instance().getConfiguration().getIntpSchema();

    private static final String project_lc_sql =
            "select project_id, name, procedure, period_second, start_date, end_date, executors\n" +
            "from " + SCHEMA + ".RT_LIFECYCLE_JOBS\n" +
            "where is_active = 'Y' and project_id = ?";

    private static final String lc_sql =
            "select project_id, name, procedure, period_second, start_date, end_date, executors\n" +
            "from " + SCHEMA + ".RT_LIFECYCLE_JOBS\n" +
            "where is_active = 'Y' and ifnull(project_id, '') = ''";
}
