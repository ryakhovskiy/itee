package org.kr.intp.application.job;

import org.kr.intp.IntpMessages;
import org.kr.intp.application.AppContext;
import org.kr.intp.application.job.hooks.HookManager;
import org.kr.intp.application.job.lifecycle.ProjectLcManager;
import org.kr.intp.application.job.scheduler.CalendarExecutor;
import org.kr.intp.application.job.scheduler.CustomExecutor;
import org.kr.intp.application.job.scheduler.PeriodicalExecutor;
import org.kr.intp.application.manager.DbApplicationManager;
import org.kr.intp.application.manager.EREManager;
import org.kr.intp.application.pojo.job.Application;
import org.kr.intp.application.pojo.job.ApplicationJob;
import org.kr.intp.application.pojo.job.JobProperties;
import org.kr.intp.application.pojo.job.SpecialProcedureMapping;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.CloseableResource;
import org.kr.intp.util.db.TimeController;
import org.kr.intp.util.db.pool.DataSourceFactory;
import org.kr.intp.util.db.pool.IDataSource;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class JobInitializator {

    private static final String INTP_SCHEMA = AppContext.instance().getConfiguration().getIntpSchema();
    private static final String UPDATE_SCHEDULER_TIME_BASE_QUERY = String.format("update %s.RT_ACTIVE_PROJECTS set"
            + " SCHEDULER_TIME_BASE = case when (SCHEDULER_TIME_BASE is null) then CURRENT_UTCTIMESTAMP"
            + " else SCHEDULER_TIME_BASE end where PROJECT_ID = ?", INTP_SCHEMA);
    private static final String GET_SCHEDULER_TIME_BASE_QUERY = String.format("select ap.SCHEDULER_TIME_BASE,"
            + " CURRENT_UTCTIMESTAMP from %s.RT_ACTIVE_PROJECTS AS ap where ap.PROJECT_ID = ?", INTP_SCHEMA);
    private final Logger log = LoggerFactory.getLogger(JobInitializator.class);
    private final Map<String, List<CustomExecutor>> applicationExecutors = new HashMap<String, List<CustomExecutor>>();
    private final Map<String, List<Future>> applicationTasks = new HashMap<String, List<Future>>();
    private final Map<String, List<IDataSource>> applicationConnectionPools = new HashMap<>();
    private final Map<String, List<CloseableResource>> applicationCloseableResources = new HashMap<String, List<CloseableResource>>();
    private final DbApplicationManager dbApplicationManager = new DbApplicationManager();
    private final IntpMessages intpMessages = AppContext.instance().getIntpMessages();
    private final IntpConfig config = AppContext.instance().getConfiguration();

    public JobInitializator() { }

    public boolean scheduleJob(final Application application) {
        if (applicationTasks.containsKey(application.getProjectId())) {
            log.warn("application [" + application.getProjectId() + "] is already running");
            return false;
        }
        log.debug("Starting application: " + application);
        Runnable runnable = new Runnable() {
            public void run() {
                final String projectId = application.getProjectId();
                try {
                    log.info("scheduling new application: " + projectId);
                    final List<CustomExecutor> executors = new ArrayList<CustomExecutor>();
                    applicationTasks.put(projectId, new ArrayList<Future>());
                    applicationConnectionPools.put(projectId, new ArrayList<IDataSource>());
                    applicationCloseableResources.put(projectId, new ArrayList<CloseableResource>());
                    applicationExecutors.put(projectId, executors);
                    final long timeBase = getSchedulerTimeBase(projectId);
                    final ApplicationJob[] jobs = application.getApplicationJobs();
                    final JobObserver observer = JobObserver.getInstance(projectId);
                    applicationCloseableResources.get(projectId).add(observer);
                    final EREManager ereManager = EREManager.getInstance(projectId);
                    ereManager.runMonitors();
                    applicationCloseableResources.get(projectId).add(ereManager);
                    final HookManager hookManager = new HookManager(JobInitializator.this, dbApplicationManager, projectId, application.getVersion());
                    for (ApplicationJob job : jobs) {
                        if(job.getInitialProperties().getProcessExecutors()>0){
                            final CustomExecutor initialExecutor = scheduleInitialJob(projectId, job, observer, hookManager, timeBase, application.getClientInfo());

                            executors.add(initialExecutor);
                        }
                        if(job.getDeltaProperties().getProcessExecutors()>0){
                            final CustomExecutor deltaExecutor = scheduleDeltaJob(projectId, job, observer, hookManager, timeBase, application.getClientInfo());
                            executors.add(deltaExecutor);
                        }
                    }
                    scheduleLifecycleJobs(projectId);
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    if (interruptJob(projectId)){
                        dbApplicationManager.stopOnError(application.getProjectId(), e.getMessage());
                    }
                }
            }
        };
        new Thread(runnable, "STARTER").start();
        return true;
    }

    private long getSchedulerTimeBase(String projectId) throws SQLException {
        long schedulerTimeBaseFromDb = 0;
        long intimeUTCTime;
        long HANASystemUTCTime = 0;

        Connection connection = null;
        PreparedStatement getSchedulerTimeBaseStatement = null;
        PreparedStatement updateSchedulerTimeBaseStatement = null;
        ResultSet set = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            updateSchedulerTimeBaseStatement = connection.prepareStatement(UPDATE_SCHEDULER_TIME_BASE_QUERY);
            updateSchedulerTimeBaseStatement.setString(1, projectId);
            int countOfUpdatedRows = updateSchedulerTimeBaseStatement.executeUpdate();
            if(countOfUpdatedRows != 1){
                return 0;
            }
            getSchedulerTimeBaseStatement = connection.prepareStatement(GET_SCHEDULER_TIME_BASE_QUERY);
            getSchedulerTimeBaseStatement.setString(1, projectId);
            set = getSchedulerTimeBaseStatement.executeQuery();
            if (!set.next()) {
                return 0;
            } else {
                Timestamp tempTimestamp = set.getTimestamp(1);
                if(tempTimestamp != null)
                    schedulerTimeBaseFromDb = tempTimestamp.getTime();
                tempTimestamp = set.getTimestamp(2);
                if(tempTimestamp != null)
                    HANASystemUTCTime = tempTimestamp.getTime();
                intimeUTCTime = TimeController.getInstance().getServerUtcTimeMillis();
                if(HANASystemUTCTime != 0 && schedulerTimeBaseFromDb != 0) {
                    long deltaMilliseconds = intimeUTCTime - HANASystemUTCTime;
                    schedulerTimeBaseFromDb = schedulerTimeBaseFromDb + deltaMilliseconds;
                }
            }
        } finally {
            if (null != set)
                set.close();
            if (null != updateSchedulerTimeBaseStatement)
                updateSchedulerTimeBaseStatement.close();
            if (null != getSchedulerTimeBaseStatement)
                getSchedulerTimeBaseStatement.close();
            if (null != connection)
                connection.close();
        }
        return schedulerTimeBaseFromDb;
    }

    private CustomExecutor scheduleDeltaJob(String projectId, ApplicationJob job, JobObserver observer,
                     HookManager hookManager, long schedulerTimeBase, Properties clientInfo) throws SQLException {
        final ApplicationJob.JobType type = ApplicationJob.JobType.DELTA;

        //read job properties
        final JobProperties.ConnectionType connectionType = job.getDeltaProperties().getConnectionType();
        final long startDate = job.getDeltaProperties().getStartdate();
        final long endDate = job.getDeltaProperties().getEnddate();
        final int processors = job.getDeltaProperties().getProcessExecutors();
        final String keysSqlStatement = job.getDeltaKeysSqlStatement();
        final String processSqlStatement = job.getDeltaSqlStatement();
        final int keysCount = job.getKeys().length;
        final long period = job.getDeltaProperties().getPeriod();
        final int version = job.getVersion();
        final JobProperties.SchedulingMethod schedulingMethod = job.getDeltaProperties().getSchedulingMethod();
        final JobProperties.PeriodChoice periodChoice = job.getDeltaProperties().getPeriodChoice();

        final long keyProcessingExecutionMaxDurationMS = job.getDeltaProperties().getKeyProcessingExecutionMaxDurationMS();
        final long keyProcessingTotalTimeoutMS = job.getDeltaProperties().getKeyProcessingTotalTimeoutMS();
        final long keyProcessingRetryPauseMS = job.getDeltaProperties().getKeyProcessingRetryPauseMS();

        final long connectionTimeoutMS = job.getDeltaProperties().getConnectionEstablishingMaxDurationMS();
        final long connectionTotalTimeoutMS = job.getDeltaProperties().getConnectionTotalTimeoutMS();
        final long connectionRetryPauseMS = job.getDeltaProperties().getConnectionRetryPauseMS();

        //create executor
        int threadsCount = 2; //one for EventLogger, one for DeltaJobExecutor, 2 for DeltaKeysReaders
        final boolean scheduleStopThread = endDate > 0;
        if (scheduleStopThread)
            threadsCount++;
        CustomExecutor executor = null;
        if (schedulingMethod == JobProperties.SchedulingMethod.PERIOD)
            executor = new PeriodicalExecutor(projectId + "_D", threadsCount, periodChoice);
        else
            executor = new CalendarExecutor(projectId + "_D", threadsCount, periodChoice);

        final SpecialProcedureMapping spMapping = getMapping(projectId, version, JobType.DELTA, keysCount);

        //submit EventLogger
        final EventLogger eventLogger = EventLogger.getEventLogger(job, type);
        Future future = executor.submit(eventLogger);
        applicationTasks.get(projectId).add(future);
        //submit DeltaJobExecutor
        IDataSource dataSource = DataSourceFactory.newDataSource(processors, clientInfo, connectionType,
                connectionTimeoutMS, connectionRetryPauseMS, connectionTotalTimeoutMS);
        applicationConnectionPools.get(projectId).add(dataSource);
        final DeltaKeyProcessor keyProcessor = new DeltaKeyProcessor(this, dbApplicationManager, projectId,
                processSqlStatement, keysCount, eventLogger, processors, observer, dataSource, hookManager,
                job.getKeys(), spMapping, keyProcessingTotalTimeoutMS, keyProcessingRetryPauseMS,
                keyProcessingExecutionMaxDurationMS);

        final DeltaJobExecutor jobExecutor = new DeltaJobExecutor(keyProcessor, job.getKeys(), projectId,
                eventLogger, observer, keysCount, keysSqlStatement, dataSource);
        applicationCloseableResources.get(projectId).add(jobExecutor);
        long initialDelay = TimeUnit.SECONDS.toMillis(3);

        long periodInMilliseconds;
        switch(periodChoice) {
            case SECONDS:
                periodInMilliseconds = TimeUnit.SECONDS.toMillis(period);
                break;
            case HOUR:
                periodInMilliseconds = TimeUnit.HOURS.toMillis(period);
                break;
            case DAY:
                periodInMilliseconds = TimeUnit.DAYS.toMillis(period);
                break;
            case WEEK:
                periodInMilliseconds = TimeUnit.DAYS.toMillis(period) * 7;
                break;
            case MONTH:
                periodInMilliseconds = TimeUnit.DAYS.toMillis(period) * 30;
                break;
            case YEAR:
                periodInMilliseconds = TimeUnit.DAYS.toMillis(period) * 365;
                break;
            default:
                String message = intpMessages.getString("org.kr.intp.application.job.jobinitializator.001",
                        "Wrong PeriodChoice");
                log.warn(message);
                periodInMilliseconds = TimeUnit.SECONDS.toMillis(period);
                break;
        }

        if(periodInMilliseconds != 0) {
            if (startDate > 0) {
                initialDelay = startDate - TimeController.getInstance().getServerUtcTimeMillis();
                if (initialDelay < 0) {
                    initialDelay = periodInMilliseconds - (-initialDelay) % periodInMilliseconds;
                }
            } else {
                initialDelay = schedulerTimeBase + TimeUnit.SECONDS.toMillis(30) -
                        TimeController.getInstance().getServerUtcTimeMillis();
                if (initialDelay < 0) {
                    initialDelay = periodInMilliseconds - (-initialDelay) % periodInMilliseconds;
                }
            }
        }

        if (schedulingMethod == JobProperties.SchedulingMethod.PERIOD) {
            if (period > 0)
                future = executor.schedule(jobExecutor, initialDelay, periodInMilliseconds);
            else
                future = executor.schedule(jobExecutor, initialDelay);
        } else {
            future = ((CalendarExecutor)executor).schedule(job.getDeltaProperties().getCalendarInfo(), jobExecutor);
        }

        applicationTasks.get(projectId).add(future);

        //schedule DeltaKeysReader
        //connectionManager = ConnectionManagerFactory.newConnectionManager(2, 1);
        //applicationConnectionPools.get(projectId).add(connectionManager);
        //final DeltaKeysReader keysReader = new DeltaKeysReader(projectId, connectionManager, keysSqlStatement, keysCount, jobExecutor);
        //final long keyReaderPeriod = periodInMilliseconds <= TimeUnit.SECONDS.toMillis(1) ? periodInMilliseconds : TimeUnit.SECONDS.toMillis(1);
        //future = executor.schedule(keysReader, initialDelay, keyReaderPeriod);
        //applicationTasks.get(projectId).add(future);

        log.info(String.format("%s - DeltaJobExecutor scheduled, period: %d ms, delay: %d ms", projectId, periodInMilliseconds, initialDelay));
        return executor;
    }

    private CustomExecutor scheduleInitialJob(String projectId, ApplicationJob job, JobObserver observer,
                                                        HookManager hookManager, long schedulerTimeBase, Properties clientInfo) throws SQLException {
        final ApplicationJob.JobType type = ApplicationJob.JobType.INITIAL;

        //read job properties
        final JobProperties.ConnectionType connectionType = job.getInitialProperties().getConnectionType();
        final long startDate = job.getInitialProperties().getStartdate();
        final long endDate = job.getInitialProperties().getEnddate();
        final int processors = job.getInitialProperties().getProcessExecutors();
        final String keysSqlStatement = job.getInitialKeysSqlStatement();
        final String processSqlStatement = job.getInitialSqlStatement();
        final int keysCount = job.getKeys().length;
        final long period = job.getInitialProperties().getPeriod();
        final int version = job.getVersion();
        final JobProperties.SchedulingMethod schedulingMethod = job.getInitialProperties().getSchedulingMethod();
        final JobProperties.PeriodChoice periodChoice = job.getInitialProperties().getPeriodChoice();

        final long keyProcessingExecutionMaxDurationMS = job.getDeltaProperties().getKeyProcessingExecutionMaxDurationMS();
        final long keyProcessingTotalTimeoutMS = job.getDeltaProperties().getKeyProcessingTotalTimeoutMS();
        final long keyProcessingRetryPauseMS = job.getDeltaProperties().getKeyProcessingRetryPauseMS();

        final long connectionTimeoutMS = job.getDeltaProperties().getConnectionEstablishingMaxDurationMS();
        final long connectionTotalTimeoutMS = job.getDeltaProperties().getConnectionTotalTimeoutMS();
        final long connectionRetryPauseMS = job.getDeltaProperties().getConnectionRetryPauseMS();

        //create executor
        int threadsCount = 2; //one for EventLogger and one for InitialJobExecutor
        final boolean scheduleStopThread = endDate > 0;
        if (scheduleStopThread)
            threadsCount++;
        CustomExecutor executor = null;
        if (schedulingMethod == JobProperties.SchedulingMethod.PERIOD)
            executor = new PeriodicalExecutor(projectId + "_I", threadsCount, periodChoice);
        else
            executor = new CalendarExecutor(projectId + "_I", threadsCount, periodChoice);

        //schedule EventLogger
        final EventLogger eventLogger = EventLogger.getEventLogger(job, type);
        Future future = executor.submit(eventLogger);
        applicationTasks.get(projectId).add(future);

        //schedule StopThread
        if (scheduleStopThread)
            scheduleStopThread(executor, endDate, projectId);

        final SpecialProcedureMapping spMapping = getMapping(projectId, version, JobType.INITIAL, keysCount);
        //schedule InitialJobExecutor
        final IDataSource dataSource = DataSourceFactory.newDataSource(processors, clientInfo, connectionType, connectionTimeoutMS, connectionRetryPauseMS, connectionTotalTimeoutMS);
        applicationConnectionPools.get(projectId).add(dataSource);
        final KeysReader keysReader = new KeysReader(projectId, dataSource, keysSqlStatement, keysCount);
        final InitialJobExecutor jobExecutor = new InitialJobExecutor(this, dbApplicationManager, projectId,
                keysReader, processSqlStatement,  processors, keysCount, eventLogger, observer, hookManager, dataSource,
                job.getKeys(), spMapping, keyProcessingTotalTimeoutMS, keyProcessingRetryPauseMS,
                keyProcessingExecutionMaxDurationMS);
        applicationCloseableResources.get(projectId).add(jobExecutor);
        long initialDelay = TimeUnit.SECONDS.toMillis(3);

        long periodInMilliseconds;
        switch(periodChoice) {
            case SECONDS:
                periodInMilliseconds = TimeUnit.SECONDS.toMillis(period);
                break;
            case HOUR:
                periodInMilliseconds = TimeUnit.HOURS.toMillis(period);
                break;
            case DAY:
                periodInMilliseconds = TimeUnit.DAYS.toMillis(period);
                break;
            case WEEK:
                periodInMilliseconds = TimeUnit.DAYS.toMillis(period) * 7;
                break;
            case MONTH:
                periodInMilliseconds = TimeUnit.DAYS.toMillis(period) * 30;
                break;
            case YEAR:
                periodInMilliseconds = TimeUnit.DAYS.toMillis(period) * 365;
                break;
            default:
                String message = intpMessages.getString("org.kr.intp.application.job.initialjobinitializator.001",
                        "wrong PeriodChoice");
                log.warn(message);
                periodInMilliseconds = TimeUnit.SECONDS.toMillis(period);
                break;
        }

        if(periodInMilliseconds != 0) {
            if (startDate > 0) {
                initialDelay = startDate - TimeController.getInstance().getServerUtcTimeMillis();
                if (initialDelay < 0) {
                    initialDelay = periodInMilliseconds - (-initialDelay) % periodInMilliseconds;
                }
            } else {
                initialDelay = schedulerTimeBase + TimeUnit.SECONDS.toMillis(30) -
                        TimeController.getInstance().getServerUtcTimeMillis();
                if (initialDelay < 0) {
                    initialDelay = periodInMilliseconds - (-initialDelay) % periodInMilliseconds;
                }
            }
        }

        if (schedulingMethod == JobProperties.SchedulingMethod.PERIOD) {
            if (period > 0)
                future = executor.schedule(jobExecutor, initialDelay, periodInMilliseconds);
            else
                future = executor.schedule(jobExecutor, initialDelay);
        } else {
            future = ((CalendarExecutor)executor).schedule(job.getInitialProperties().getCalendarInfo(), jobExecutor);
        }

        applicationTasks.get(projectId).add(future);
        log.info(String.format("%s - InitialJobExecutor scheduled, period: %d ms, delay: %d ms", projectId, periodInMilliseconds, initialDelay));
        return executor;
    }

    private void scheduleStopThread(final CustomExecutor executorService,
                                    final long endDate, final String projectId) {
        String message = intpMessages.getString("org.kr.intp.application.job.jobinitializator.002",
                "%s - scheduling stop thread: %s");
        log.debug(String.format(message, projectId, new Date(endDate)));
        Runnable stopExecution = new Runnable() {
            public void run() {
                log.info(projectId + " shutting down...");
                JobInitializator.this.interruptJob(projectId);
                Thread.currentThread().interrupt();
            }
        };
        Future future = executorService.schedule(stopExecution, endDate -
                TimeController.getInstance().getServerUtcTimeMillis());
        applicationTasks.get(projectId).add(future);
    }

    private void scheduleLifecycleJobs(String projectId) throws SQLException {
        ProjectLcManager lcManager = new ProjectLcManager(this, dbApplicationManager, projectId);
        applicationCloseableResources.get(projectId).add(lcManager);
        lcManager.runLC();
    }

    public boolean interruptJob(String projectId) {
        log.info(projectId + " - interrupting application: ");
        if (!applicationTasks.containsKey(projectId)) {
            log.warn("application [" + projectId + "] is not running");
            return false;
        }
        final boolean interrupt = true;
        //interrupt all jobs assigned to the application.
        final List<Future> tasks = applicationTasks.remove(projectId);
        for (Future task : tasks)
            task.cancel(interrupt);
        tasks.clear();
        //stop the executor.
        final List<CustomExecutor> executors = applicationExecutors.remove(projectId);
        for (CustomExecutor executorService : executors)
            executorService.shutdownNow();

        for (CloseableResource c : applicationCloseableResources.get(projectId))
            c.close();

        //close connection pools
        for (IDataSource dataSource : applicationConnectionPools.get(projectId))
            dataSource.close();
        return true;
    }

    private SpecialProcedureMapping getMapping(String projectId, int version, JobType jobType, int keysCount)
            throws SQLException {
        log.debug("looking for special-procedure mapping...");
        final String sql = "select key_number, key_special_value, procedure_schema, procedure_name from " +
                config.getIntpSchema() + ".RT_SPECIAL_PROCEDURES where " +
                "project_id = ? and project_version = ? and type = ?";
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.prepareStatement(sql);
            statement.setString(1, projectId);
            statement.setInt(2, version);
            statement.setString(3, jobType == JobType.INITIAL ? "I" : "D");
            resultSet = statement.executeQuery();
            Map<String, String> mapping = new HashMap<String, String>();
            int id = -1;
            while (resultSet.next()) {
                log.debug("mapping found");
                id = resultSet.getInt(1);
                String value = resultSet.getString(2);
                String schema = resultSet.getString(3);
                String proc = resultSet.getString(4);
                StringBuilder sb = new StringBuilder();
                sb.append("call \"").append(schema).append("\".\"").append(proc).append("\"(?,?,");
                for (int i = 0; i < keysCount; i++)
                    sb.append("?,");
                sb.deleteCharAt(sb.length() - 1);
                sb.append(')');
                mapping.put(value, sb.toString());
            }
            if (id > 0) {
                SpecialProcedureMapping spMapping = new SpecialProcedureMapping(id, mapping);
                log.debug("special-procedure mapping found: " + spMapping);
                return spMapping;
            }
            else {
                log.debug("special-procedure mapping not found");
                return null;
            }
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    public void stop() {
        log.warn("stopping Job Scheduler...");
        Set<String> projectIds = new HashSet<String>(applicationExecutors.keySet());
        for (String projectId : projectIds)
            interruptJob(projectId);
    }
}