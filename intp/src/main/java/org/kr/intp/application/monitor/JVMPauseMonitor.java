package org.kr.intp.application.monitor;

import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;

/**
 *
 */
public class JVMPauseMonitor extends Thread {

    private static final long SLEEP_TIMEOUT_MS = 500L;
    private static final long MAX_ALLOWED_PAUSE_TIME_MS = 1000L;

    private final Logger log = LoggerFactory.getLogger(JVMPauseMonitor.class);

    public JVMPauseMonitor() {
        super("JVM_PAUSE_MONITOR");
        setDaemon(true);
    }

    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            final long start = System.currentTimeMillis();
            sleep();
            final long duration = System.currentTimeMillis() - start;
            if (duration > MAX_ALLOWED_PAUSE_TIME_MS)
                logWarning(duration);
        }
    }

    private void logWarning(long duration) {
        log.warn("!! PAUSE DETECTED (ms): " + duration);
    }

    private void sleep() {
        try {
            Thread.sleep(SLEEP_TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

}
