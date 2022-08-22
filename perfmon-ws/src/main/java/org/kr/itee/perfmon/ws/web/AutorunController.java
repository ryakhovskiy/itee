package org.kr.itee.perfmon.ws.web;

import org.kr.itee.perfmon.ws.monitor.MonitorManager;
import org.kr.itee.perfmon.ws.notification.MessageType;
import org.kr.itee.perfmon.ws.notification.NotificationService;
import org.kr.itee.perfmon.ws.os.ProcessController;
import org.kr.itee.perfmon.ws.pojo.*;
import org.kr.itee.perfmon.ws.utils.IOUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class AutorunController {

    private final Logger log = Logger.getLogger(AutorunController.class);
    private final ScheduledExecutorService itProcessLauncher = Executors.newSingleThreadScheduledExecutor();
    private final ScheduledExecutorService rtProcessLauncher = Executors.newSingleThreadScheduledExecutor();
    private final MonitorManager[] monitorManagers = new MonitorManager[2];
    private final ExecutorService monitorExecutor = Executors.newFixedThreadPool(2);
    private final RequestPOJO request;
    private final ProcessController rtProcessController;
    private final ProcessController itProcessController;
    private final AtomicInteger rtQueryProcessCounter = new AtomicInteger(0);
    private final AtomicInteger itQueryProcessCounter = new AtomicInteger(0);
    private final AtomicInteger rtUpdateProcessCounter = new AtomicInteger(0);
    private final AtomicInteger itUpdateProcessCounter = new AtomicInteger(0);

    public AutorunController(RequestPOJO request) throws IOException {
        this.request = request;
        this.rtProcessController = new ProcessController();
        this.itProcessController = new ProcessController();
    }

    public void start() throws Exception {
        if (null == request) {
            log.warn("null request received, skipping");
            return;
        }
        startMonitors();
        startItUpdates();
        startRtUpdates();
        startItQueries();
        startRtQueries();
        startRTstopper();
        startITstopper();
    }

    private void startITstopper() {
        final long timeoutMS = request.getItAutorunSpecification().getTimeoutMS();
        if (timeoutMS <= 0)
            return;
        log.debug("scheduling stopping thread for IT, timeout (ms): " + timeoutMS);
        final String threadName = "IT-STOPPER";
        Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(timeoutMS);
                    log.debug("stopping IT by timeout");
                    IOUtils.getInstance().cleanUpWorkingDir();
                } catch (InterruptedException e) {
                    log.debug(threadName + " has been interrupted");
                } catch (IOException e) {
                    log.error("Error while cleaning up working directory: " + e.getMessage(), e);
                } finally {
                    stopIT();
                }
            }
        };
        Thread t = new Thread(r, threadName);
        t.setDaemon(true);
        t.start();
    }

    private void startRTstopper() {
        final long timeoutMS = request.getRtAutorunSpecification().getTimeoutMS();
        if (timeoutMS <= 0)
            return;
        log.debug("scheduling stopping thread for RT, timeout (ms): " + timeoutMS);
        final String threadName = "RT-STOPPER";
        Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(timeoutMS);
                    NotificationService.getInstance().sendMessageType(MessageType.STOP);
                    log.debug("stopping RT by timeout");
                    IOUtils.getInstance().cleanUpWorkingDir();
                } catch (InterruptedException e) {
                    log.debug(threadName + " has been interrupted");
                } catch (IOException e) {
                    log.error("Error while cleaning up working directory: " + e.getMessage(), e);
                } finally {
                    stopRT();
                }
            }
        };
        Thread t = new Thread(r, threadName);
        t.setDaemon(true);
        t.start();
    }

    private void startMonitors() throws Exception {
        NotificationService.getInstance().sendMessageType(MessageType.START);
        log.debug("starting rt monitor...");
        monitorManagers[0] = startMonitor(request.getRtMonitorSpecification(), "REAL-TIME");
        log.debug("starting it monitor...");
        monitorManagers[1] = startMonitor(request.getItMonitorSpecification(), "IN-TIME");
    }

    private MonitorManager startMonitor(final MonitorPOJO monitorPOJO, final String colName) throws Exception {
        if (null == monitorPOJO) {
            log.warn("cannot start monitor, no specification received");
            return null;
        }
        JdbcPOJO jdbcPOJO = monitorPOJO.getJdbcPOJO();
        if (null == jdbcPOJO) {
            log.warn("cannot start monitor, no jdbc specification for monitor received");
            return null;
        }
        String monitorFile = monitorPOJO.getOutfile();
        String driver = jdbcPOJO.getDriver();
        String url = jdbcPOJO.getUrl();
        String server = monitorPOJO.getServer();
        int port = monitorPOJO.getPowerConcumtionMonitorPort();
        long pcduration = monitorPOJO.getPowerConsumtionMonitorDurationMS();
        int topx = monitorPOJO.getTopXValues();
        long sleepInterval = monitorPOJO.getMonitorQueryIntervalMS();
        int age = monitorPOJO.getMonitorAgeSeconds();
        long expensiveStatementsDuration = monitorPOJO.getExpensiveStatementsDurationMS();
        MonitorManager monitorManager = new MonitorManager(driver, url, server, port, pcduration, topx, sleepInterval, age,
                expensiveStatementsDuration, colName, monitorFile);
        monitorExecutor.submit(monitorManager);
        return monitorManager;
    }

    private void startItUpdates() throws Exception {
        AutorunPOJO autorun = request.getItAutorunSpecification();
        if (null == autorun) {
            log.warn("no autorun for IT");
            return;
        }
        startLoad(itProcessLauncher, itUpdateProcessCounter, itProcessController,
                request.getItUpdateLoadUpdateSpecification(),
                autorun.getUpdateProcessesBatchSize(),
                autorun.getUpdateProcessesLimit(),
                autorun.getUpdateProcessAddPeriodMS());
    }

    private void startRtUpdates() throws Exception {
        AutorunPOJO autorun = request.getRtAutorunSpecification();
        if (null == autorun) {
            log.warn("no autorun for RT");
            return;
        }
        startLoad(rtProcessLauncher, rtUpdateProcessCounter, rtProcessController,
                request.getRtUpdateLoadSpecification(),
                autorun.getUpdateProcessesBatchSize(),
                autorun.getUpdateProcessesLimit(),
                autorun.getUpdateProcessAddPeriodMS());
    }

    private void startItQueries() throws Exception {
        AutorunPOJO autorun = request.getItAutorunSpecification();
        if (null == autorun) {
            log.warn("no autorun for IT");
            return;
        }
        startLoad(itProcessLauncher, itQueryProcessCounter, itProcessController,
                request.getItQueryLoadSpecification(),
                autorun.getQueryProcessesBatchSize(),
                autorun.getQueryProcessesLimit(),
                autorun.getQueryProcessAddPeriodMS());
    }

    private void startRtQueries() throws Exception {
        AutorunPOJO autorun = request.getRtAutorunSpecification();
        if (null == autorun) {
            log.warn("no autorun for RT");
            return;
        }
        startLoad(rtProcessLauncher, rtQueryProcessCounter, rtProcessController,
                request.getRtQueryLoadSpecification(),
                autorun.getQueryProcessesBatchSize(),
                autorun.getQueryProcessesLimit(),
                autorun.getQueryProcessAddPeriodMS());
    }

    private void startLoad(final ScheduledExecutorService processLauncher, final AtomicInteger processCounter,
                           final ProcessController processController, final LoadPOJO loadPOJO,
                           final int batch, final int limit, final long addTime) throws Exception {
        if (null == loadPOJO) {
            log.warn("null load received, skipping");
            return;
        }
        final Properties loadProps = new Properties();
        loadProps.setProperty("query.file", loadPOJO.getFile());
        loadProps.setProperty("round.robin", String.valueOf(loadPOJO.isRoundRobin()));
        loadProps.setProperty("query.file.encoding", loadPOJO.getFileEncoding());
        loadProps.setProperty("jdbc.pass", loadPOJO.getJdbcPOJO().getPassword());
        loadProps.setProperty("jdbc.server", loadPOJO.getJdbcPOJO().getHost());
        loadProps.setProperty("connection.pooling.enabled", "false");
        loadProps.setProperty("query.type", loadPOJO.getQueryType());
        loadProps.setProperty("exec.time", String.valueOf(loadPOJO.getExecutionTimeSeconds()));
        loadProps.setProperty("jdbc.url", loadPOJO.getJdbcPOJO().getUrl());
        loadProps.setProperty("jdbc.user", loadPOJO.getJdbcPOJO().getUser());
        loadProps.setProperty("scheduler.interval.ms", String.valueOf(loadPOJO.getSchedulerIntervalMS()));
        loadProps.setProperty("jdbc.driver", loadPOJO.getJdbcPOJO().getDriver());
        loadProps.setProperty("concurrent.executors", String.valueOf(loadPOJO.getConcurrentExecutors()));
        loadProps.setProperty("queries.per.interval", String.valueOf(loadPOJO.getQueriesPerInterval()));

        Runnable addProcess = new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < batch; i++) {
                        int id = processCounter.incrementAndGet();
                        if (id > limit)
                            return;
                        processController.startProcess(loadProps);
                    }
                } catch (IOException e) {
                    log.error("cannot start RT process: " + e.getMessage(), e);
                }
            }
        };
        processLauncher.scheduleAtFixedRate(addProcess, addTime, addTime, TimeUnit.MILLISECONDS);
    }

    public void stop() throws Exception {
        log.info("stopping launcher");
        stopRT();
        stopIT();
    }

    private void stopRT() {
        try {
            log.info("stopping RT...");
            stop(rtProcessLauncher, rtProcessController, monitorManagers[0], rtQueryProcessCounter, rtUpdateProcessCounter);
        } catch (Exception e) {
            log.error("Error while stopping real-time scenario: " + e.getMessage(), e);
        }
    }

    private void stopIT() {
        try {
            log.info("stopping IT...");
            stop(itProcessLauncher, itProcessController, monitorManagers[1], itQueryProcessCounter, itUpdateProcessCounter);
        } catch (Exception e) {
            log.error("Error while stopping in-time scenario: " + e.getMessage(), e);
        }
    }

    private void stop(ExecutorService processLauncher, ProcessController processController,
                      MonitorManager mm, AtomicInteger... processCounters) throws Exception {
        processLauncher.shutdownNow();
        processController.stopAllProcesses();
        for (AtomicInteger counter : processCounters)
            counter.set(0);
        if (null == mm)
            return;
        mm.shutdown();
        mm.saveResultsToFile();
    }
}
