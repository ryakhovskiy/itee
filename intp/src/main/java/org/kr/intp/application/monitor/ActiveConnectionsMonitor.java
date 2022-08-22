package org.kr.intp.application.monitor;

import org.kr.intp.application.AppContext;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class ActiveConnectionsMonitor implements Runnable {

    public static ActiveConnectionsMonitor getHanaActiveConnectionsMonitor() {
        return HanaActiveConnectionsMonitor.getInstance();
    }

    private final IntpConfig config = AppContext.instance().getConfiguration();
    private final Logger log = LoggerFactory.getLogger(ActiveConnectionsMonitor.class);
    private final boolean isTraceEnabled = log.isTraceEnabled();
    private final Map<String, Integer> connections = new HashMap<>();

    private final boolean isMonitorEnabled = config.isHwMonitorEnabled();
    private final long monitorFrequency = config.getHwMonitorFrequencyMS();

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    private volatile boolean initialized = false;
    private volatile boolean done = false;

    protected ActiveConnectionsMonitor() { }

    protected void init() throws Exception {
        log.info("AC_MONITOR is being initialized...");
        updateStats();
        initialized = true;
    }

    @Override
    public final void run() {
        if (!isMonitorEnabled)
            return;
        try {
            init();
            while (!done && !Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(monitorFrequency);
                    updateStats();
                } catch (InterruptedException e) {
                    log.debug("AC_MONITOR has been interrupted");
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            close();
        }
    }

    private void updateStats() throws Exception {
        final Map<String, Integer> stats = monitor();
        copy(stats);
    }

    protected abstract Map<String, Integer> monitor() throws Exception;

    private void copy(Map<String, Integer> data) {
        try {
            writeLock.lock();
            connections.clear();
            connections.putAll(data);
        } finally {
            writeLock.unlock();
        }
    }

    public final Map<String, Integer> getActiveConnections() {
        if (!initialized) {
            if (isTraceEnabled) {
                if (!isMonitorEnabled)
                    log.trace("AC_MONITOR is disabled");
                else
                    log.trace("AC_MONITOR has not been initialized");
            }
            return new HashMap<String, Integer>();
        }
        try {
            readLock.lock();
            return new HashMap<String, Integer>(connections);
        } finally {
            readLock.unlock();
        }
    }

    public final void stop() {
        log.warn("Stopping active connections monitor");
        done = true;
    }

    protected void close() {
        log.info("AC_MONITOR is being closed...");
    }
}
