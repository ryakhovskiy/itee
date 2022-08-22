package org.kr.intp.application.job;

import org.kr.intp.IntpMessages;
import org.kr.intp.application.AppContext;
import org.kr.intp.application.manager.EREManager;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.CloseableResource;
import org.kr.intp.util.db.load.DBMSLoadChecker;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by kr on 1/16/14.
 */
public class JobObserver implements CloseableResource {

    private static final Map<String, JobObserver> observers = new HashMap<>();
    private final IntpConfig config = AppContext.instance().getConfiguration();
    private final boolean concurrentDeltaAllowed = config.getConcurrentDeltaAllowed();
    private final IntpMessages intpMessages = AppContext.instance().getIntpMessages();

    private final Logger log = LoggerFactory.getLogger(JobObserver.class);
    private final DBMSLoadChecker dbmsLoadChecker = DBMSLoadChecker.newHanaDBMSLoadChecker();
    private final boolean isTraceEnabled = log.isTraceEnabled();
    private final String projectId;
    private final ReentrantLock jobGuardLock = new ReentrantLock();
    private final Condition deltasExists = jobGuardLock.newCondition();
    private final ActionLogger logger;
    private final InitialJobDBController dbController;
    private int deltaCounter;
    private int deltasStartedFromLastInitial = 0;
    private boolean runInitialOnStart = true;
    private volatile boolean isInitialTriggered = false;

    private JobObserver(String projectId) {
        this.projectId = projectId;
        this.logger = ActionLogger.getInstance(projectId);
        this.dbController = new InitialJobDBController(AppContext.instance().getConfiguration().getIntpSchema());
    }

    public static JobObserver getInstance(String projectId) {
        synchronized (observers) {
            if (observers.containsKey(projectId))
                return observers.get(projectId);
            else {
                final JobObserver observer = new JobObserver(projectId);
                observers.put(projectId, observer);
                return observer;
            }
        }
    }

    public boolean tryStartInitial() {
        if (dbmsLoadChecker.isLoadThresholdReached()) {
            if (log.isDebugEnabled()) {
                String message = intpMessages.getString("org.kr.intp.application.job.jobobserver.001",
                        "skipping initial due to Load-Threshold reached.");
                log.debug(message);
            }
            return false;
        }
        boolean startAllowed = false;
        try {
            jobGuardLock.lock();
            if (!isInitialTriggered)
                startAllowed = true;
            if (deltasStartedFromLastInitial > 0)
                startAllowed = true;
            else if (runInitialOnStart)
                startAllowed = true;
            if (startAllowed) {
                isInitialTriggered = true;
                deltasStartedFromLastInitial = 0;
                if (runInitialOnStart)
                    runInitialOnStart = false;
                EREManager.getInstance(projectId).forceRT4allMonitors();
            }
        } finally {
            jobGuardLock.unlock();
        }
        if (isTraceEnabled) {
            if (startAllowed)
                log.info(projectId + " Initial triggered");
            else
                log.debug(projectId + " Initial already running, skipping new one...");
        }
        return startAllowed && dbController.notifyStarted(projectId);
    }

    public void setInitialFinished(String uuid, boolean hasErrorsGlobal) {
        String message;
        boolean stopped = false;
        try {
            jobGuardLock.lock();
            stopped = isInitialTriggered;
            if (stopped) {
                isInitialTriggered = false;
                logger.log("awaiting...");
            }
        } finally {
            jobGuardLock.unlock();
        }
        if (!hasErrorsGlobal)
            dbController.notifyFinished(projectId);
        else
            logger.log("");
        if (!stopped) {
            message = intpMessages.getString("org.kr.intp.application.job.jobobserver.002",
                    "Cannot finish Initial job, Initial is not running!");
            log.error(projectId + message);
        }
        else if (isTraceEnabled)
            log.info(projectId + " Initial finished: " + uuid);
    }

    public boolean tryStartDelta() {
        String message;
        if (dbmsLoadChecker.isLoadThresholdReached()) {
            if (log.isDebugEnabled()) {
                message = intpMessages.getString("org.kr.intp.application.job.jobobserver.003",
                        "skipping delta due to Load-Threshold reached.");
                log.debug(message);
            }
            return false;
        }
        if (!dbController.isProjectInitialized(projectId)) {
            log.warn(projectId + ": cannot start delta, project was not initialized by initial job");
            return false;
        }
        boolean startAllowed = false;
        try {
            jobGuardLock.lock();
            if (deltaCounter > 0 && !concurrentDeltaAllowed) {
                message = intpMessages.getString("org.kr.intp.application.job.jobobserver.004",
                        "There are non-finished deltas, concurrent deltas are not allowed, new delta skipped");
                log.warn(message);
                return false;
            }
            startAllowed = !isInitialTriggered;
            if (startAllowed) {
                deltaCounter++;
                deltasStartedFromLastInitial++;
            }
        } finally {
            jobGuardLock.unlock();
        }
        if (isTraceEnabled) {
            if (startAllowed) {
                log.info(projectId + " Delta triggered");
            } else {
                message = intpMessages.getString("org.kr.intp.application.job.jobobserver.005",
                        " Delta skipping - Initial was triggered but was not finished");
                log.debug(projectId + message);
            }
        }
        return startAllowed;
    }

    public void setDeltaFinished(String uuid) {
        try {
            jobGuardLock.lock();
            --deltaCounter;
            if (0 == deltaCounter)
                logger.log("awaiting...");
            deltasExists.signal();
        } finally {
            jobGuardLock.unlock();
        }
        if (isTraceEnabled)
            log.info(projectId + " Delta finished: " + uuid);
    }

    public void awaitForDeltasCompletion() throws InterruptedException {
        String message;
        if (log.isDebugEnabled()) {
            message = intpMessages.getString("org.kr.intp.application.job.jobobserver.006",
                    " Initial job, awaiting deltas completion...");
            log.debug(projectId + message);
        }
        while (true) {
            try {
                jobGuardLock.lock();
                if (deltaCounter == 0) {
                    if (log.isDebugEnabled()) {
                        message = intpMessages.getString("org.kr.intp.application.job.jobobserver.007",
                                " Initial job, no deltas are running.");
                        log.debug(projectId + message);
                    }
                    return;
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug(String.format("%s Initial job, [%d] deltas are running, awaiting for completion.",
                                projectId, deltaCounter));
                    }
                    deltasExists.await();
                }
            } finally {
                jobGuardLock.unlock();
            }
        }
    }

    public void close() {
        String message = intpMessages.getString("org.kr.intp.application.job.jobobserver.008",
                " closing JobObserver...");
        log.info(message);
        logger.close();
        synchronized (observers) {
            observers.remove(projectId);
        }
    }

    public boolean isInitialRunning() {
        return isInitialTriggered;
    }
}