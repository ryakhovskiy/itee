package org.kr.intp.application.monitor;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created bykron 06.12.2016.
 */
public class JVMHeapMonitor extends Thread {

    private static final long PAUSE_TIME_MS = 100;

    private final Runtime runtime = Runtime.getRuntime();

    private final ReadWriteLock statsLock = new ReentrantReadWriteLock(false);
    private final Lock statsReadLock = statsLock.readLock();
    private final Lock statsWriteLock = statsLock.writeLock();

    private long freeHeapSizeBytes;
    private long maxHeapSizeBytes;
    private long currentHeapSizeBytes;

    public JVMHeapMonitor() {
        super("JVM_GC");
        this.setDaemon(true);
        updateStats();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            updateStats();
            sleep0();
        }
    }

    private void updateStats() {
        final long maxHeapSizeBytes = runtime.maxMemory();
        final long freeHeapSizeBytes = runtime.freeMemory();
        final long currentHeapSizeBytes = runtime.totalMemory();

        try {
            statsWriteLock.lock();
            this.maxHeapSizeBytes = maxHeapSizeBytes;
            this.freeHeapSizeBytes = freeHeapSizeBytes;
            this.currentHeapSizeBytes = currentHeapSizeBytes;
        } finally {
            statsWriteLock.unlock();
        }
    }

    public Statistics getStatistics() {
        long maxHeapSizeBytes;
        long freeHeapSizeBytes;
        long currentHeapSizeBytes;
        try {
            statsReadLock.lock();
            maxHeapSizeBytes = this.maxHeapSizeBytes;
            freeHeapSizeBytes = this.freeHeapSizeBytes;
            currentHeapSizeBytes = this.currentHeapSizeBytes;
        } finally {
            statsReadLock.unlock();
        }
        return new Statistics(maxHeapSizeBytes, freeHeapSizeBytes, currentHeapSizeBytes);
    }

    public void gc() {
        updateStats();
        System.gc();
        updateStats();
    }

    private void sleep0() {
        try {
            Thread.sleep(PAUSE_TIME_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public class Statistics {
        public final long maxHeapSizeBytes;
        public final long freeHeapSizeBytes;
        public final long currentHeapSizeBytes;
        public Statistics(long maxHeapSizeBytes, long freeHeapSizeBytes, long currentHeapSizeBytes) {
            this.maxHeapSizeBytes = maxHeapSizeBytes;
            this.freeHeapSizeBytes = freeHeapSizeBytes;
            this.currentHeapSizeBytes = currentHeapSizeBytes;
        }
        @Override public String toString() {
            long maxMB = maxHeapSizeBytes / (1 << 20);
            long freeMB = freeHeapSizeBytes / (1 << 20);
            long currMB = currentHeapSizeBytes / (1 << 20);
            return "maxHeap: " + maxMB + "; freeHeap: " + freeMB + "; currentHeap: " + currMB;
        }
    }

}
