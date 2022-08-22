package org.kr.intp.application.job.lifecycle;

import org.kr.intp.application.job.JobInitializator;
import org.kr.intp.application.manager.DbApplicationManager;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.IDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

/**
 * Created by kr on 29.01.14.
 */
public class LifecycleJobExecutor implements Runnable {

    private final Logger log = LoggerFactory.getLogger(LifecycleJobExecutor.class);
    private final IDataSource dataSource;
    private final ExecutorService executorService;
    private final EventLogger eventLogger;
    private final String sql;
    private final String projectId;
    private final JobInitializator jobInitializator;
    private final DbApplicationManager dbApplicationManager;

    public LifecycleJobExecutor(JobInitializator jobInitializator, DbApplicationManager dbApplicationManager,
                                String projectId, String procedure, IDataSource dataSource, EventLogger eventLogger,
                                ExecutorService executorService) throws SQLException {
        this.jobInitializator = jobInitializator;
        this.dbApplicationManager = dbApplicationManager;
        this.sql = String.format("call %s()", procedure);
        this.dataSource = dataSource;
        this.executorService = executorService;
        this.eventLogger = eventLogger;
        this.projectId = projectId;
    }

    public void run() {
        if (Thread.currentThread().isInterrupted()) {
            log.debug("already interrupted, cannot start.");
            return;
        }
        final String uuid = UUID.randomUUID().toString();
        eventLogger.logTriggered(uuid);
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                execute(uuid);
            }
        };
        executorService.submit(runnable);
    }

    private void execute(final String uuid) {
        try {
            log.debug(" starting...");
            eventLogger.logStarted(uuid);
            doQuery();
            eventLogger.logFinished(uuid);
            log.debug(" done.");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            eventLogger.logError(uuid, e);
            dbApplicationManager.stopOnError(projectId, e.getMessage());
            jobInitializator.interruptJob(projectId);
        }
    }

    private void doQuery() throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(sql);
            log.trace(sql);
            statement.execute();
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

}
