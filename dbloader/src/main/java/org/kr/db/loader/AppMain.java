package org.kr.db.loader;


import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.*;

/**
 * Created by kr on 21.01.14.
 */
public class AppMain {

    private static final long offset = getOffset();

    public static long currentTimeMillis() {
        return System.currentTimeMillis() + offset;
    }

    private static long getOffset() {
        final TimeZone tz = TimeZone.getDefault();
        return (-1) * tz.getOffset(System.currentTimeMillis());
    }

    private static final Logger log = Logger.getLogger(AppMain.class);

    public static void main(String... args) {
        try {
            loadLog4jConfig();
            String host = InetAddress.getLocalHost().getHostName();
            //can be used in log4j.properties with key %X{host}
            MDC.put("host", host);
            final String brokerUrl = args.length > 0 ? args[0] : "";
            start(brokerUrl);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            System.exit(-1);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            System.exit(-2);
        }
    }

    private static void start(String brokerUrl) throws IOException, SQLException {
        log.debug("configuring");
        final AppConfig config = AppConfig.getInstance();
        final int concurrentExecutors = config.getConcurrentExecutors();
        log.debug("concurrent executors: " + concurrentExecutors);
        final long interval = config.getSchedulerInterval();
        log.debug("query launch interval: " + interval);
        final int queriesPerInterval = config.getQueriesPerInterval();
        final boolean roundRobinEnabled = config.isRoundRobinEnabled();
        final long stopTime = config.getExecutionTime();
        final int threadsCount = stopTime > 0 ? 2 : 1;

        final MqLogger mqLogger = brokerUrl.length() > 0 ? new MqLogger(brokerUrl) : null;
        if (null != mqLogger)
            mqLogger.start();
        final List<Future> futures = new ArrayList<Future>(concurrentExecutors);
        final BlockingQueue<String> queryQueue = new ArrayBlockingQueue<String>(concurrentExecutors * queriesPerInterval * 2);
        final ExecutorService executorService = Executors.newFixedThreadPool(concurrentExecutors, NamedThreadFactory.newInstance("SE"));
        final ConnectionManager connectionManager = ConnectionManager.newInstance(concurrentExecutors, concurrentExecutors);

        for (int i = 0; i < concurrentExecutors; i++) {
            Future f = executorService.submit(new SqlExecutor(queryQueue, connectionManager, mqLogger));
            futures.add(f);
        }
        log.debug("executors launched");
        final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(threadsCount, NamedThreadFactory.newInstance("TR"));
        final Object stopNotifier = new Object();
        Future f = scheduledExecutorService.scheduleAtFixedRate(new TaskLauncher(queryQueue, stopNotifier), 10, interval, TimeUnit.MILLISECONDS);
        futures.add(f);
        log.debug("launcher scheduled");

        Runnable stopTask = new Runnable() {
            @Override
            public void run() {
                log.debug("stopping tasks by timer...");
                close(futures, executorService, scheduledExecutorService, connectionManager, mqLogger);
                log.debug("tasks stopped.");
            }
        };

        if (stopTime > 0) {
            scheduledExecutorService.schedule(stopTask, stopTime, TimeUnit.SECONDS);
            log.debug("stop thread by timer scheduled");
        } else if (!roundRobinEnabled) {
            Thread stopThread = new Thread(stopTask, "STOP_THREAD");
            stopThread.setDaemon(true);
            synchronized (stopNotifier) {
                try {
                    log.debug("awaiting for all data processed...");
                    stopNotifier.wait();
                    log.debug("all data processed, stopping");
                } catch (InterruptedException e) {
                    log.debug("interrupted");
                }
                stopThread.start();
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread("SHUTDOWN_THREAD") {
            public void run() {
                close(futures, executorService, scheduledExecutorService, connectionManager, mqLogger);
            }
        });
    }

    private static void close(List<Future> futures, ExecutorService executorService,
                              ScheduledExecutorService scheduledExecutorService, ConnectionManager connectionManager,
                              MqLogger mqLogger) {
        mqLogger.interrupt();
        for (Future f : futures)
            f.cancel(true);
        executorService.shutdownNow();
        scheduledExecutorService.shutdownNow();
        connectionManager.close();
    }

    /**
     * loads log4j.properties file located in resources directory
     * @throws java.io.IOException
     */
    private static void loadLog4jConfig() throws IOException {
        if (new File("log4j.properties").exists())
            PropertyConfigurator.configure("log4j.properties");
        else
            loadLog4jConfigFromRecourse();
    }

    private static void loadLog4jConfigFromRecourse() throws IOException {
        InputStream inputStream = null;
        try {
            inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("log4j.properties");
            if (null != inputStream)
                PropertyConfigurator.configure(inputStream);
            else
                System.out.println("Cannot find log4j.properties resource");
        } finally {
            if (null != inputStream)
                inputStream.close();
        }
    }
}
