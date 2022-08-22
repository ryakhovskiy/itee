package org.kr.intp.application.agent;

import org.kr.intp.IntpMessages;
import org.kr.intp.IntpServerInfo;
import org.kr.intp.application.AppContext;
import org.kr.intp.application.job.JobInitializator;
import org.kr.intp.application.job.hooks.HookLogger;
import org.kr.intp.application.job.lifecycle.LifecycleJobMonitor;
import org.kr.intp.application.job.scheduler.CustomCalendar;
import org.kr.intp.application.manager.ApplicationReader;
import org.kr.intp.application.manager.DbApplicationManager;
import org.kr.intp.application.monitor.ActiveConnectionsMonitor;
import org.kr.intp.application.monitor.HwResourceMonitor;
import org.kr.intp.application.pojo.job.Application;
import org.kr.intp.client.db.DatabaseRequestHandler;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.*;
import org.kr.intp.util.db.meta.IntpMetadataManager;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import org.kr.intp.util.db.project.IntpProjectManager;
import org.kr.intp.util.license.LicenseManager;
import org.kr.intp.util.thread.NamedThreadFactory;
import org.kr.intp.util.thread.ThreadUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 9/14/13
 * Time: 3:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class IntpServer implements IServer {

    private final IntpServerInfo intpServerInfo;
    private final Logger log = LoggerFactory.getLogger(IntpServer.class);
    private final ExecutorService serviceExecutorService = Executors.newCachedThreadPool(NamedThreadFactory.newInstance("ST"));
    private final ScheduledExecutorService serviceScheduledExecutorService = Executors.newScheduledThreadPool(1, NamedThreadFactory.newInstance("STS"));
    private final HwResourceMonitor hwResourceMonitor;
    private final ActiveConnectionsMonitor acMonitor;
    private final DbApplicationManager dbApplicationManager;
    private final DatabaseRequestHandler databaseRequestHandler;
    private final JobInitializator jobInitializator;
    private final DbLogger dbLogger;
    private int tables;

    private int currentApplicationsCount = 0;
    private volatile boolean running;

    private final IntpConfig config;
    private final IntpDbInitializer initializer;

    public IntpServer(IntpServerInfo info, IntpConfig config) {
        log.debug("creating a Server instance");
        ServiceConnectionPool.restart();
        dbLogger = new DbLogger(config.isDbLoggingEnabled());
        AppContext.instance().setServer(this); //leaking reference
        AppContext.instance().setIntpMessage(new IntpMessages(config.getLocale()));
        this.config = config;
        this.initializer = new IntpDbInitializer(config);
        this.dbApplicationManager = new DbApplicationManager();
        this.databaseRequestHandler = new DatabaseRequestHandler(dbApplicationManager);
        this.jobInitializator = new JobInitializator();
        this.intpServerInfo = info;
        this.hwResourceMonitor = HwResourceMonitor.getHanaHwResourceMonitor();
        this.acMonitor = ActiveConnectionsMonitor.getHanaActiveConnectionsMonitor();
        log.debug("server instance was successfully created");
    }

    public IntpConfig getConfiguration() {
        return config;
    }

    private void initialize() {
        log.debug("initializing the instance");
        dbLogger.start();
        initializer.checkHANAConfigData();
        IntpMetadataManager.checkIntpMetadata();
        setStarted();
        IntpProjectManager.checkIntpProjects();
        initializer.checkIntpAdminUserPrivelegesForIntpSchema();
        initializer.checkIntpAdminUserPrivelegesForIntpGenSchema();
        final LicenseManager licenseManager = LicenseManager.newInstance(this, intpServerInfo);
        this.serviceExecutorService.submit(licenseManager);
        this.serviceExecutorService.submit(HookLogger.getInstance());
        this.tables = licenseManager.getTablesCount();
        initializer.persistConfigurationToDatabase();
        dbApplicationManager.init();
        log.debug("the instance was successfully initialized");
    }

    public void start() throws Exception {
        initialize();
        log.info("INTP SERVER is being started...");
        serviceExecutorService.submit(databaseRequestHandler);
        serviceExecutorService.submit(LifecycleJobMonitor.newInstance(jobInitializator, dbApplicationManager));
        serviceExecutorService.submit(hwResourceMonitor);
        serviceExecutorService.submit(acMonitor);
        serviceScheduledExecutorService.scheduleAtFixedRate(CustomCalendar.getInstance(), 1000, 1, TimeUnit.DAYS);
    }

    public boolean startApplication(String projectId) throws SQLException, IOException {
        if (!running)
            throw new UnsupportedOperationException("INTP is not running");
        final Application app = ApplicationReader.getInstance().getActiveApplication(projectId);
        if (currentApplicationsCount >= tables)
            return false;
        final boolean started = jobInitializator.scheduleJob(app);
        if (started)
            currentApplicationsCount++;
        return started;
    }

    public boolean stopApplication(String projectId) throws SQLException {
        if (!running)
            throw new UnsupportedOperationException("INTP is not running");
        final boolean stopped = jobInitializator.interruptJob(projectId);
        if (stopped)
            currentApplicationsCount--;
        return stopped;
    }

    public void close() throws IOException {
        if (!running) {
            log.warn("IN-TIME SERVER IS NOT RUNNING");
            ThreadUtils.logStackTrace(log, Level.INFO);
            return;
        }
        log.warn("STOPPING IN-TIME SERVER");
        setStopped();
        hwResourceMonitor.stop();
        acMonitor.stop();
        jobInitializator.stop();
        ApplicationReader.getInstance().close();
        log.warn("shutting down service executors");
        serviceExecutorService.shutdownNow();
        serviceScheduledExecutorService.shutdownNow();
        dbLogger.interrupt();
        ServiceConnectionPool.instance().close();
        AppContext.instance().setServer(null);
    }

    private void setStarted() {
        IntpStatusModifier.getInstance().setIntpStarted(intpServerInfo.getInstanceId());
        this.running = true;
    }

    private void setStopped() throws IOException {
        try {
            this.running = false;
            log.warn("setting stop status...");
            IntpStatusModifier.getInstance().setIntpStopped(intpServerInfo.getInstanceId());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public boolean isRunning() {
        return running;
    }

    public int[] getCpuMemStats() {
        return hwResourceMonitor.getCpuMemStats();
    }

    public Map<String, Integer> getActiveConnectionsStats() {
        return acMonitor.getActiveConnections();
    }

    public BaseLogger getDbLogger() {
        return dbLogger;
    }

}
