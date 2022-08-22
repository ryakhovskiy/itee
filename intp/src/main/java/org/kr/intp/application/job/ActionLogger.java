package org.kr.intp.application.job;

import org.kr.intp.application.AppContext;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import org.kr.intp.util.thread.NamedThreadFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by kr on 3/9/14.
 */
public class ActionLogger implements Runnable {

    private static final String SCHEMA = AppContext.instance().getConfiguration().getIntpSchema();

    private static final String UPDATE_STATUS_QUERY = String.format(
            "update %s.RT_ACTIVE_PROJECTS set ACTION = ? where PROJECT_ID = ?", SCHEMA);

    private static final Map<String, ActionLogger> loggers = new HashMap<String, ActionLogger>();

    public static ActionLogger getInstance(String projectId) {
        synchronized (loggers) {
            if (loggers.containsKey(projectId))
                return loggers.get(projectId);
            final ActionLogger actionLogger = new ActionLogger(projectId);
            actionLogger.executorService.submit(actionLogger);
            loggers.put(projectId, actionLogger);
            return actionLogger;
        }
    }

    private final BlockingQueue<String> queue = new LinkedBlockingDeque<String>();
    private final Logger log = LoggerFactory.getLogger(ActionLogger.class);
    private final String projectId;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor(NamedThreadFactory.newInstance("AL"));
    private volatile boolean closed = false;

    private ActionLogger(String projectId) {
        this.projectId = projectId;
    }

    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted())
                logAction();
        } catch (InterruptedException e) {
            log.debug("ActionLogger has been interrupted");
            Thread.currentThread().interrupt();
        }
    }

    public void log(String action) {
        try {
            queue.put(action);
        } catch (InterruptedException e) {
            log.debug("interrupted");
            Thread.currentThread().interrupt();
        }
    }

    private void logAction() throws InterruptedException {
        String action = queue.take();
        try {
            logAction(action);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
        if (queue.size() == 0) {
            synchronized (queue) {
                queue.notify();
            }
        }
    }

    private void logAction(String action) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.prepareStatement(UPDATE_STATUS_QUERY);
            statement.setString(1, action);
            statement.setString(2, projectId);
            statement.execute();
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    public synchronized void close() {
        if (closed)
            return;
        closed = true;
        log.trace("closing Action Logger...");
        try {
            waitForEmptyQueue();
            logAction("");
            executorService.shutdownNow();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        } finally {
            synchronized (loggers) {
                loggers.remove(projectId);
            }
        }
    }

    private void waitForEmptyQueue() {
        while (!queue.isEmpty()) {
            synchronized (queue) {
                try {
                    queue.wait();
                } catch (InterruptedException e) {
                    log.trace("interrupted");
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
