package org.kr.db.loader.ui.monitor;

import org.kr.db.loader.ui.TestCaseBase;
import org.kr.db.loader.ui.panels.DbLoaderPanel;

import javax.swing.*;
import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.*;

/**
 * Created by kr on 5/17/2014.
 */
public class MonitorManagerTest extends TestCaseBase {

    private static final String JDBC_DRIVER = "com.sap.db.jdbc.Driver";
    private static final String JDBC_URL = "jdbc:sap://192.168.178.23:30015?user=eintpadm&password=xxx";
    private static final String JDBC_URL1 = "jdbc:sap://10.118.15.49:30015?user=yyy&password=xxx";
    private static final int AGE_SECONDS = 60;
    private static final int TOP_X = 5;
    private final CountDownLatch startGate = new CountDownLatch(1);
    private final CountDownLatch initGate = new CountDownLatch(1);
    private static final long MONITOR_SLEEP_MS = 600;
    private static final long EXPENSIVE_STATEMENTS_LENGTH_MS = 2500;

    @Override
    public void setUp() throws ClassNotFoundException {
        Class.forName(JDBC_DRIVER);
    }

    public void testDataFetcher() throws Exception {
        final DataFetcher dataFetcherBase = new DataFetcher(JDBC_URL);
        final String data = dataFetcherBase.fetchData();
        assertNotNull(data);
        System.out.println("data: " + data);
    }

    public void testVperfMonitor() throws Exception {
        final VPerformanceMonitor perfmon =
                new VPerformanceMonitor(JDBC_URL, 5000, "Real-Time", new JProgressBar[0], new DbLoaderPanel.RunningProcessesMonitor[0], 120);
        runMonitor(perfmon);
    }

    public void testDoubleToString() throws Exception {
        final double[] dd = { 129.85,239.855,294.85,29.785,239.85,29.185 };
        final StringBuilder stringBuilder = new StringBuilder();
        for (double d : dd) {
            final NumberFormat nf = NumberFormat.getInstance(new Locale("en"));
            System.out.println(nf.format(d));
            stringBuilder.append(d).append('\t');
        }
        System.out.println(stringBuilder.toString());
    }

    public void testVperfMonitorWPB() throws Exception {
        JProgressBar[] bars = { new JProgressBar(), new JProgressBar(), new JProgressBar() };
        final VPerformanceMonitor perfmon =
                new VPerformanceMonitor(JDBC_URL, 5000, "In-Time", bars, new DbLoaderPanel.RunningProcessesMonitor[0], 120);
        runMonitor(perfmon);
    }

    public void testFullMonitorManager() throws Exception {
        final IMonitor monitor = new MonitorManager(JDBC_DRIVER, JDBC_URL1, "", 0, 0, TOP_X, MONITOR_SLEEP_MS, AGE_SECONDS,
                EXPENSIVE_STATEMENTS_LENGTH_MS, new JProgressBar[0], new DbLoaderPanel.RunningProcessesMonitor[2], "RT");
        runMonitor(monitor);
    }

    private void runMonitor(IMonitor monitor) throws Exception {
        ExecutorService service = Executors.newFixedThreadPool(1);
        service.submit(monitor);
        if (!(monitor instanceof MonitorManager))
            startGate.countDown();
        Thread.sleep(30000);
        long start = System.currentTimeMillis();
        System.out.println("gathering results");
        monitor.shutdown();
        List<String> data = monitor.getResults();
        long end = System.currentTimeMillis();
        for (String line : data)
            System.out.println(line);
        System.out.println(end - start);
    }
}
