package org.kr.itee.perfmon.ws.monitor;

import org.kr.itee.perfmon.ws.utils.DerbySaver;
import org.kr.itee.perfmon.ws.utils.IOUtils;
import org.kr.itee.perfmon.ws.utils.JdbcUtils;
import org.kr.itee.perfmon.ws.utils.Stats2DbSaver;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by kr on 5/27/2014.
 */
public class MonitorManager implements IMonitor {

    private static long start;
    public static void setMonitorStarted() {
        start = org.kr.itee.perfmon.ws.utils.Timer.currentTimeMillis();
    }
    public static long getMonitorStarted() {
        return start;
    }

    private final Logger log = Logger.getLogger(MonitorManager.class);
    private final Object doneJobLock = new Object();
    private final ExecutorService service;
    private final List<IMonitor> monitors = new ArrayList<IMonitor>();
    private final List<Future> futures = new ArrayList<Future>();
    private final CountDownLatch startGate = new CountDownLatch(1);
    private final String url;
    private final List<String> results = new ArrayList<String>();
    private final Object resultsLock = new Object();
    private boolean resultsReady = false;
    private final DerbySaver derbySaver = DerbySaver.getInstance();
    private final int topx;
    private final CustomMonitor customMonitor;
    private final String outputFile;
    private volatile boolean isDone;

    public MonitorManager(String driver, String url, String server, int pcport, long pcduration, int topx,
                          long monitor_sleep_ms, int age_seconds, long expensive_length_ms, String colName) throws Exception {
        this(driver, url, server, pcport, pcduration, topx, monitor_sleep_ms, age_seconds, expensive_length_ms, colName, "");
    }

    public MonitorManager(String driver, String url, String server, int pcport, long pcduration, int topx,
                          long monitor_sleep_ms, int age_seconds, long expensive_length_ms, String colName,
                          String outputFile) throws Exception {
        testConnection(driver, url);
        this.outputFile = outputFile;
        this.url = url;
        this.customMonitor = new CustomMonitor(url, monitor_sleep_ms);
        this.topx = topx;
        monitors.add(new ServicesMemoryMonitor(url, startGate, monitor_sleep_ms, derbySaver));
        monitors.add(new ServiceAllocMonitor("indexserver", url, topx, startGate, monitor_sleep_ms, null));
        monitors.add(new ServiceAllocMonitor("statisticsserver", url, topx, startGate, monitor_sleep_ms, null));
        monitors.add(new ServiceAllocMonitor("preprocessor", url, topx, startGate, monitor_sleep_ms, null));
        monitors.add(new ServiceAllocMonitor("compileserver", url, topx, startGate, monitor_sleep_ms, null));
        monitors.add(new ServiceAllocMonitor("xsengine", url, topx, startGate, monitor_sleep_ms, null));

        monitors.add(new PowerConsumptionMonitor(server, pcport, pcduration));

        monitors.add(new ExpensiveStatementsMonitor(url, startGate, monitor_sleep_ms, expensive_length_ms));

        monitors.add(new VPerformanceMonitor(url, monitor_sleep_ms, colName, age_seconds));

        service = Executors.newFixedThreadPool(monitors.size());
        for (IMonitor m : monitors)
            futures.add(service.submit(m));
    }

    private void testConnection(String driver, String url) throws SQLException, ClassNotFoundException, IOException {
        log.debug("initializing test connection...");
        Class.forName(driver);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url);
            JdbcUtils.checkStoredProcedure(connection);
        } finally {
            if (null != connection)
                connection.close();
        }
    }

    public void run() {
        try {
            launchMonitors();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void launchMonitors() throws InterruptedException {
        startGate.countDown();
        customMonitor.start();
        final ExecutorService overviewRetrieverService = Executors.newSingleThreadExecutor();
        final List<String> lines = new ArrayList<>();
        Stats2DbSaver stats2DbSaver = null;
        try {
            final String commonData = new DataFetcher(url).fetchData();
            lines.add(commonData);
            synchronized (doneJobLock) {
                while (!isDone)
                    doneJobLock.wait();
            }
            final Future<List<String>> selfMeasurementsFuture = overviewRetrieverService.submit(new JmsMonitor());
            for (IMonitor m : monitors)
                m.shutdown();

            stats2DbSaver = new Stats2DbSaver(url);
            final List<String> derbyLines = derbySaver.getLines();
            lines.addAll(derbyLines);
            //save2Db(stats2DbSaver, null, derbyLines);
            for (IMonitor m : monitors) {
                if (m instanceof MonitorBase)
                    continue;

                final List<String> data = new ArrayList<>();
                data.addAll(m.getResults());
                lines.addAll(data);
                final String msg = save2Db(stats2DbSaver, m, data);
                if (msg.length() > 0)
                    lines.add(msg);
            }
            lines.addAll(getSelfMeasurements(selfMeasurementsFuture));
            stats2DbSaver.saveCommonData(commonData);
        } catch (Exception e) {
            addResult(e.getMessage());
        } finally {
            if (null != stats2DbSaver)
                stats2DbSaver.close();
            synchronized (resultsLock) {
                results.addAll(lines);
                resultsReady = true;
                resultsLock.notify();
            }
            for (final Future f : futures)
                f.cancel(true);
            overviewRetrieverService.shutdownNow();
            service.shutdownNow();
        }
    }

    private List<String> getSelfMeasurements(Future<List<String>> future) {
        final List<String> selfMeasurements = new ArrayList<>();
        try {
            selfMeasurements.addAll(future.get());
        } catch (InterruptedException | ExecutionException e) {
            final String message = "SelfMeasurements were not collected: " + e.getMessage();
            log.error(e.getMessage(), e);
            selfMeasurements.add(message);
        }
        return selfMeasurements;
    }


    public void shutdown() {
        isDone = true;
        customMonitor.stop();
        synchronized (doneJobLock) {
            doneJobLock.notifyAll();
        }
    }

    public void saveResultsToFile() throws Exception {
        if (null == outputFile || outputFile.length() == 0)
            return;
        IOUtils.getInstance().saveBinaryLines(outputFile, getResults());
    }

    public List<String> getResults() {
        synchronized (resultsLock) {
            while (!resultsReady) {
                try {
                    resultsLock.wait();
                } catch (InterruptedException e) {
                    return results;
                }
            }
            return results;
        }
    }

    private void addResult(String data) {
        synchronized (resultsLock) {
            results.add(data);
        }
    }

    private String save2Db(Stats2DbSaver stats2DbSaver, IMonitor iMonitor, List<String> data) {
        try {
            if (iMonitor instanceof ServiceAllocMonitor)
                stats2DbSaver.saveServiceAllocatorInfo(data, ((ServiceAllocMonitor) iMonitor).getServiceName());

            if (iMonitor instanceof ExpensiveStatementsMonitor)
                stats2DbSaver.saveExpensiveStatementsInfo(data);

            if (iMonitor instanceof PowerConsumptionMonitor)
                stats2DbSaver.savePowerCollectorInfo(data);

            if (iMonitor instanceof ServicesMemoryMonitor)
                stats2DbSaver.saveServiceMemoryInfo(data);

            if (iMonitor instanceof VPerformanceMonitor)
                stats2DbSaver.saveVPerformanceInfo(data);

            return "";
        } catch (Exception e) {
            log.error("Error while saving data to DB", e);
            return "Error while saving data to DB: " + e.getMessage();
        }
    }
}
