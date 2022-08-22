package org.kr.db.loader.ui.monitor;

import org.kr.db.loader.ui.AppMain;
import org.kr.db.loader.ui.derby.DerbySaver;
import org.kr.db.loader.ui.panels.DbLoaderPanel;
import org.kr.db.loader.ui.utils.Stats2DbSaver;

import javax.swing.*;
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

    public MonitorManager(String driver, String url, String server, int pcport, long pcduration, int topx,
                          long monitor_sleep_ms, int age_seconds, long expensive_length_ms, JProgressBar[] progressBars,
                          DbLoaderPanel.RunningProcessesMonitor[] rpmonitors, String colName) throws Exception {
        testConnection(driver, url);
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

        monitors.add(new VPerformanceMonitor(url, monitor_sleep_ms, colName, progressBars, rpmonitors, age_seconds));

        service = Executors.newFixedThreadPool(monitors.size());
        for (IMonitor m : monitors)
            futures.add(service.submit(m));
    }

    private void testConnection(String driver, String url) throws SQLException, ClassNotFoundException, IOException {
        Class.forName(driver);
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url);
            AppMain.checkStoredProcedure(connection);
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
        final List<String> lines = new ArrayList<String>();
        Stats2DbSaver stats2DbSaver = null;
        try {
            final String commonData = new DataFetcher(url).fetchData();
            lines.add(commonData);
            synchronized (doneJobLock) {
                doneJobLock.wait();
            }
            final Future<List<String>> future = overviewRetrieverService.submit(new JmsMonitor());
            for (IMonitor m : monitors)
                m.shutdown();

            stats2DbSaver = new Stats2DbSaver(url);
            final List<String> derbyLines = derbySaver.getLines();
            lines.addAll(derbyLines);
            //save2Db(stats2DbSaver, null, derbyLines);
            for (IMonitor m : monitors) {
                final List<String> data = new ArrayList<String>();
                if (m instanceof MonitorBase) {
                    continue;
                } else {
                    data.addAll(m.getResults());
                    lines.addAll(data);
                }
                final String msg = save2Db(stats2DbSaver, m, data);
                if (msg.length() > 0)
                    lines.add(msg);
            }
            final List<String> selfMeasurements = future.get();
            lines.addAll(selfMeasurements);
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

    public void shutdown() {
        customMonitor.stop();
        synchronized (doneJobLock) {
            doneJobLock.notifyAll();
        }
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
            System.out.println("Error while saving data to DB");
            e.printStackTrace();
            return "Error while saving data to DB: " + e.getMessage();
        }
    }
}
