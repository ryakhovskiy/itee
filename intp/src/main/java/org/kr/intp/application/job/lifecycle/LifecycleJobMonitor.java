package org.kr.intp.application.job.lifecycle;

import org.kr.intp.application.AppContext;
import org.kr.intp.application.job.JobInitializator;
import org.kr.intp.application.manager.DbApplicationManager;
import org.kr.intp.application.pojo.job.LifecycleJob;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;

import java.sql.SQLException;

/**
 * Created by kr on 30.01.14.
 */
public class LifecycleJobMonitor implements Runnable {

    public static LifecycleJobMonitor newInstance(JobInitializator jobInitializator, DbApplicationManager dbAppManager) {
        return new LifecycleJobMonitor(jobInitializator, dbAppManager);
    }

    private final Logger log = LoggerFactory.getLogger(LifecycleJobMonitor.class);
    private final long pollingInterval = AppContext.instance().getConfiguration().getDbAppMonitorFrequencyMillis();
    private final CommonLcManager jobManager;

    private LifecycleJobMonitor(JobInitializator jobInitializator, DbApplicationManager dbAppManager) {
        log.info("initializing...");
        try {
            this.jobManager = new CommonLcManager(jobInitializator, dbAppManager);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException("Lifecycle Job Monitor cannot be initialized", e);
        }
    }

    @Override
    public void run() {
        log.info("starting...");
        try {
            while (!Thread.currentThread().isInterrupted()) {
                doWork();
                Thread.sleep(pollingInterval);
            }
        } catch (InterruptedException e) {
            log.debug("interrupted");
            Thread.currentThread().interrupt();
        } finally {
            close();
        }
    }

    private void doWork() {
        try {
            LifecycleJob[] activeJobs = LifecycleJobReader.getInstance().getActiveLifecycleJobs();
            jobManager.updateJobs(activeJobs);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void close() {
        log.debug("closing...");
        jobManager.close();
    }
}
