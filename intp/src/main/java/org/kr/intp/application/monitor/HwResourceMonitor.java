package org.kr.intp.application.monitor;

import org.kr.intp.application.AppContext;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class HwResourceMonitor implements Runnable {

    public static HwResourceMonitor getHanaHwResourceMonitor() {
        return HanaHwResourceMonitor.getInstance();
    }

    private final IntpConfig config = AppContext.instance().getConfiguration();
    private final Logger log = LoggerFactory.getLogger(HwResourceMonitor.class);
    private final boolean isMonitorEnabled = config.isHwMonitorEnabled();
    private final long monitorFrequency = config.getHwMonitorFrequencyMS();
    private final boolean isTraceEnabled = log.isTraceEnabled();
    private final int[] measures = new int[3];
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();
    private volatile boolean initialized = false;
    private volatile boolean done = false;

    protected HwResourceMonitor() { }

    @Override
    public void run() {
        if (!isMonitorEnabled)
            return;
        try {
            init();
            while (!done && !Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(monitorFrequency);
                    updateStats();
                } catch (InterruptedException e) {
                    log.debug("HW_MONITOR has been interrupted");
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

    protected void init() throws Exception {
        log.info("HW_MONITOR is being initialized...");
        updateStats();
        initialized = true;
    }

    private void updateStats() throws Exception {
        final int[] data = monitor();
        try {
            writeLock.lock();
            System.arraycopy(data, 0, measures, 0, measures.length);
        } finally {
            writeLock.unlock();
        }
    }

    protected void close() {
        log.info("HW_MONITOR is being closed...");
    }

    protected abstract int[] monitor() throws Exception;

    public void stop() {
        log.warn("stopping Hardware Resource Monitor");
        done = true;
    }

    public int[] getCpuMemStats() {
        if (!initialized) {
            if (isTraceEnabled) {
                if (!isMonitorEnabled)
                    log.trace("HW_MONITOR is disabled");
                else
                    log.trace("HW_MONITOR has not been initialized");
            }
            return new int[3];
        }
        try {
            readLock.lock();
            return new int[] { measures[0], measures[1], measures[2] };
        } finally {
            readLock.unlock();
        }
    }
}
