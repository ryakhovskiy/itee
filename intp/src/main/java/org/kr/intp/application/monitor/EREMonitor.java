package org.kr.intp.application.monitor;

import org.kr.intp.application.AppContext;
import org.kr.intp.application.job.JobObserver;
import org.kr.intp.application.pojo.job.EreJob;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class EREMonitor implements Runnable {

    private static final int IT_MODE = 1;
    private static final int RT_MODE = 0;

    private static final String SCHEMA = AppContext.instance().getConfiguration().getIntpSchema();

    private final Logger log = LoggerFactory.getLogger(EREMonitor.class);
    private final boolean isTraceEnabled = log.isTraceEnabled();

    private final String projectId;
    private final String name;
    private final int stats_age_sec;
    private final String rt_object;
    private final long sla_millis;
    private final long monitor_frequency_ms;
    private final long mode_min_runtime_ms;
    private final int cpu_threshold;
    private final int mem_threshold;
    private final Map<String, Integer> connectionThresholds;
    private final double avg_ratio_threshold;
    private final double max_ratio_threshold;
    private volatile boolean done = false;
    private volatile boolean isItRunning = false;

    public EREMonitor(EreJob job) throws SQLException {
        this(job.getProjectId(), job.getName(), job.getStats_age_sec(), job.getRt_object(), job.getSla_millis(),
                job.getMonitor_frequency_ms(), job.getMode_min_runtime_ms(), job.getCpu_threshold(),
                job.getMem_threshold(), job.getAvg_ratio_threshold(), job.getMax_ratio_threshold(),
                job.getConnectionThresholds());
    }

    public EREMonitor(String projectId, String name, int stats_age_sec, String rt_object, long sla_millis,
                       long monitor_frequency_ms, long mode_min_runtime_ms, int cpu_threshold, int mem_threshold,
                       double avg_ratio_threshold, double max_ratio_threshold,
                       Map<String, Integer> connectionThresholds) throws SQLException {
        this.projectId = projectId;
        this.name = name;
        this.stats_age_sec = stats_age_sec;
        this.rt_object = rt_object;
        if (sla_millis > 0)
            this.sla_millis = sla_millis;
        else
            this.sla_millis = -1;
        this.connectionThresholds = connectionThresholds;
        this.monitor_frequency_ms = monitor_frequency_ms;
        this.mode_min_runtime_ms = mode_min_runtime_ms;
        this.cpu_threshold = cpu_threshold;
        this.mem_threshold = mem_threshold;
        this.avg_ratio_threshold = avg_ratio_threshold;
        this.max_ratio_threshold = max_ratio_threshold;
        init();
    }

    private void init() throws SQLException {
        log.debug("initializing ERE Monitor: " + projectId + "; " + name);
        Connection connection = null;
        PreparedStatement getStmnt = null;
        PreparedStatement addStmnt = null;
        ResultSet resultSet = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            getStmnt = connection.prepareStatement(GET_ERE_MODE_SQL);
            getStmnt.setString(1, projectId);
            getStmnt.setString(2, name);
            resultSet = getStmnt.executeQuery();
            if (resultSet.next()) {
                final int mode = resultSet.getInt(1);
                log.trace(projectId + "; " + name + "; ERE row exists; mode: " + mode);
                this.isItRunning = mode == IT_MODE;
            } else {
                addStmnt = connection.prepareStatement(ADD_ERE_MODE_SQL);
                addStmnt.setString(1, projectId);
                addStmnt.setString(2, name);
                addStmnt.execute();
                log.trace(projectId + "; " + name + "; ERE row does not exist; mode: " + RT_MODE);
                this.isItRunning = false;
            }
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != getStmnt)
                getStmnt.close();
            if (null != addStmnt)
                addStmnt.close();
            if (null != connection)
                connection.close();
        }
    }

    public void run() {
        while (!done && !Thread.currentThread().isInterrupted()) {
            try {
                monitor();
            } catch (InterruptedException e) {
                log.debug(projectId + " - ERE Monitor interrupted");
            } catch (Exception e) {
                log.error("ERE Monitor error", e);
            }
        }
        close();
    }

    public void stop() {
        this.done = true;
    }

    private void monitor() throws SQLException, InterruptedException {
        final int[] hwres = getHwResources();
        final int cpu = hwres[0];
        final int mem = hwres[1];

        final Map<String, Integer> acStats = getConnectionsStats();
        boolean con_threshold_reached = false;
        for (Map.Entry<String, Integer> e : connectionThresholds.entrySet()) {
            final Integer threshold = connectionThresholds.get(e.getKey());
            final Integer count = acStats.get(e.getKey());
            if (null == count)
                continue;
            if (count >= threshold) {
                con_threshold_reached = true;
                break;
            }
        }

        final int[] rtobjstats = getObjectStats(rt_object);
        final double avg_rt = rtobjstats[0];
        final double max_rt = rtobjstats[1];

        final double avg_ratio = avg_rt > 0 ? avg_rt / sla_millis : 0;
        final double max_ratio = max_rt > 0 ? max_rt / sla_millis : 0;

        final boolean mem_threshold_reached = mem >= mem_threshold;
        final boolean cpu_threshold_reached = cpu >= cpu_threshold;
        final boolean avg_threshold_reached = avg_ratio >= avg_ratio_threshold;
        final boolean max_threshold_reached = max_ratio >= max_ratio_threshold;
        final boolean isInitialJobRunning = JobObserver.getInstance(projectId).isInitialRunning();

        if (log.isTraceEnabled()) {
            final String msg =
                    String.format("initial triggered:%b%nstats:%ncpu: %d; mem: %d; avg_rt: %f; max_rt: %f%nusers: %s%ncpu:\t%b;%nmem:\t%b;%navg:\t%b;%nmax:\t%b;%ncncts:\t%b",
                            isInitialJobRunning, cpu, mem, avg_rt, max_rt, acStats, cpu_threshold_reached,
                            mem_threshold_reached, avg_threshold_reached, max_threshold_reached, con_threshold_reached);
            log.trace(msg);
        }


        if (!isInitialJobRunning &&
                (mem_threshold_reached || cpu_threshold_reached ||
                        avg_threshold_reached || max_threshold_reached || con_threshold_reached)) {
            final boolean itStarted = forceIT();
            if (isTraceEnabled) {
                final String msg =
                        String.format("it started: %b; sleeping: %d", itStarted, itStarted ? mode_min_runtime_ms : 0);
                log.trace(msg);
            }
            if (itStarted) {
                Thread.sleep(mode_min_runtime_ms);
            }
        } else {
            forceRT();
        }
        Thread.sleep(monitor_frequency_ms);
    }

    public void runRT() {
        if (done)
            return;
        if (isItRunning) {
            log.trace("forcing RT due to external request (initial is running)");
            try {
                forceRT();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private int[] getHwResources() throws SQLException {
        return AppContext.instance().getServer().getCpuMemStats();
    }

    private int[] getObjectStats(String object) throws SQLException {
        Connection connection = null;
        PreparedStatement objectStatsStmnt = null;
        ResultSet resultSet = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            objectStatsStmnt = connection.prepareStatement(OBJECT_STATS_SQL);
            objectStatsStmnt.setInt(1, stats_age_sec);
            objectStatsStmnt.setString(2, object);
            objectStatsStmnt.setString(3, object);
            objectStatsStmnt.execute();
            resultSet = objectStatsStmnt.getResultSet();
            if (!resultSet.next())
                return new int[] { 0, 0 };
            int avg = resultSet.getInt(1);
            int max = resultSet.getInt(2);
            return new int[] { avg, max };
        } finally {
            if (null != resultSet)
                resultSet.close();
        }
    }

    private Map<String, Integer> getConnectionsStats() {
        return AppContext.instance().getServer().getActiveConnectionsStats();
    }

    private boolean forceIT() throws SQLException {
        if (isItRunning)
            return false;
        log.debug("forcing IT: " + projectId + "; " + name);
        Connection connection = null;
        PreparedStatement changeModeStmnt = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            changeModeStmnt = connection.prepareStatement(SET_ERE_MODE_SQL);
            changeModeStmnt.setInt(1, IT_MODE);
            changeModeStmnt.setString(2, projectId);
            changeModeStmnt.setString(3, name);
            changeModeStmnt.execute();
            isItRunning = true;
        } finally {
            if (null != changeModeStmnt)
                changeModeStmnt.close();
            if (null != connection)
                connection.close();
        }
        return true;
    }

    private boolean forceRT() throws SQLException {
        if (!isItRunning)
            return false;
        log.debug("forcing RT: " + projectId + "; " + name);
        Connection connection = null;
        PreparedStatement changeModeStmnt = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            changeModeStmnt = connection.prepareStatement(SET_ERE_MODE_SQL);
            changeModeStmnt.setInt(1, RT_MODE);
            changeModeStmnt.setString(2, projectId);
            changeModeStmnt.setString(3, name);
            changeModeStmnt.execute();
            isItRunning = false;
        } finally {
            if (null != changeModeStmnt)
                changeModeStmnt.close();
            if (null != connection)
                connection.close();
        }
        return true;
    }

    private void close() {
        log.debug("closing ERE Monitor: " + projectId + "; " + name);
        cleanup();
    }

    private void cleanup() {
        try {
            removeRtEreInfo();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }

    private void removeRtEreInfo() throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.prepareStatement(REMOVE_ERE_MODE_SQL);
            statement.setString(1, projectId);
            statement.setString(2, name);
            statement.execute();
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    @Override
    public String toString() {
        return String.format("%s-%s; age: %d; sla: %d; rtobj: %s; mon_freq: %d; mode_min_rt: %d; cpu: %d; mem: %d; avg_r: %f; max_r: %f",
                projectId, name, stats_age_sec, sla_millis, rt_object, monitor_frequency_ms, mode_min_runtime_ms, cpu_threshold, mem_threshold, avg_ratio_threshold, max_ratio_threshold);
    }

    private static final String OBJECT_STATS_SQL =
            "select\n" +
            "    ifnull(avg(p.avg_execution_time) / 1000, 0) as AVG_EXEC_TIME,\n" +
            "    ifnull(avg(p.max_execution_time) / 1000, 0) as MAX_EXEC_TIME\n" +
            "from m_sql_plan_cache p\n" +
            "where seconds_between(p.last_execution_timestamp, current_timestamp) < ? and\n" +
            "   p.statement_string like '%'||?||'%'\n" +
            "   and\n" +
            "   p.statement_string not like '%m_sql_plan_cache%'||?||'%%%'";

    private static final String SET_ERE_MODE_SQL =
            "update " + SCHEMA + ".RT_ERE_INFO set mode = ? where PROJECT_ID = ? and name = ?";

    private static final String GET_ERE_MODE_SQL =
            "select mode from " + SCHEMA + ".RT_ERE_INFO where PROJECT_ID = ? and name = ?";

    private static final String ADD_ERE_MODE_SQL =
            "insert into " + SCHEMA + ".RT_ERE_INFO values (?,?,0)";

    private static final String REMOVE_ERE_MODE_SQL =
            "delete from " + SCHEMA + ".RT_ERE_INFO where PROJECT_ID = ? and name = ?";
}




