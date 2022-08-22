package org.kr.intp.application.job.hooks;

import org.kr.intp.application.AppContext;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by kr on 12/7/2014.
 */
public class HookLogger implements Runnable {

    private static final Object[] DUMMY = new Object[0];
    private static final HookLogger instance = new HookLogger();
    private static final String SCHEMA = AppContext.instance().getConfiguration().getIntpSchema();
    private static final String SQL = "insert into " + SCHEMA + ".HOOKS_EXECUTION_LOG values (?,?,?,?,?,?,?,?,?)";
    public static HookLogger getInstance() {
        return instance;
    }

    private final Logger log = LoggerFactory.getLogger(HookLogger.class);
    private final BlockingQueue<HookLogMessage> queue = new LinkedBlockingQueue<HookLogMessage>();
    private HookLogger() { }

    public void log(String projectId, int version, String name, String type, String message, long time) {
        try {
            queue.put(new HookLogMessage(projectId, version, name, type, "", 0, DUMMY, message, time));
        } catch (InterruptedException e) {
            log.debug("HookLogger interrupted");
            Thread.currentThread().interrupt();
        }
    }

    public void log(String projectId, int version, String name, String type, String jobId, long jobTs, String message, long time) {
        try {
            queue.put(new HookLogMessage(projectId, version, name, type, jobId, jobTs, DUMMY, message, time));
        } catch (InterruptedException e) {
            log.debug("HookLogger interrupted");
            Thread.currentThread().interrupt();
        }
    }

    public void log(String projectId, int version, String name, String type, String jobId, long jobTs, Object[] args, String message, long time) {
        try {
            queue.put(new HookLogMessage(projectId, version, name, type, jobId, jobTs, args, message, time));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                performHookLoggerJob();
            }
        } finally {
            close();
        }
    }

    private void performHookLoggerJob() {
        try {
            HookLogMessage message = queue.take();
            log(message);
        } catch (InterruptedException e) {
            log.debug("HookLogger interrupted");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void log(HookLogMessage message) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.prepareStatement(SQL);
            statement.setString(1, message.getProjectId());
            statement.setInt(2, message.getProjectVersion());
            statement.setString(3, message.getName());
            statement.setString(4, message.getType());
            statement.setString(5, message.getJobId());
            statement.setTimestamp(6, new Timestamp(message.getJobTs()));
            statement.setString(7, Arrays.toString(message.getArgs()));
            statement.setString(8, message.getMessage());
            statement.setTimestamp(9, new Timestamp(message.getTime()));
            statement.execute();
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void close() {
        log.debug("Closing HookLogger");
    }
}
