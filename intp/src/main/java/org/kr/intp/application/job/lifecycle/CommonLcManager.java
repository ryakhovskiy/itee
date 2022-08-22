package org.kr.intp.application.job.lifecycle;

import org.kr.intp.application.job.JobInitializator;
import org.kr.intp.application.manager.DbApplicationManager;
import org.kr.intp.application.pojo.job.LifecycleJob;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.TimeController;
import org.kr.intp.util.db.pool.DataSourceFactory;
import org.kr.intp.util.db.pool.IDataSource;
import org.kr.intp.util.thread.NamedThreadFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * Created by kr on 29.01.14.
 */
public class CommonLcManager {

    private final Logger log = LoggerFactory.getLogger(CommonLcManager.class);
    private final Set<LifecycleJob> activeJobs = new HashSet<LifecycleJob>();
    private final Map<LifecycleJob, List<Future>> jobFutures = new HashMap<LifecycleJob, List<Future>>();
    private final Map<LifecycleJob, IDataSource> jobConnectionPool = new HashMap<LifecycleJob, IDataSource>();
    private final Map<LifecycleJob, List<ExecutorService>> jobExecutorServices = new HashMap<LifecycleJob, List<ExecutorService>>();
    private final JobInitializator jobInitializator;
    private final DbApplicationManager dbApplicationManager;

    public CommonLcManager(JobInitializator jobInitializator, DbApplicationManager dbApplicationManager) {
        this.jobInitializator = jobInitializator;
        this.dbApplicationManager = dbApplicationManager;
    }

    public void updateJobs(LifecycleJob[] activeJobs) {
        startNewJobs(activeJobs);
        stopOldJobs(activeJobs);
    }

    private void startNewJobs(LifecycleJob[] lifecycleJobs) {
        for (LifecycleJob job : lifecycleJobs) {
            if (activeJobs.contains(job))
                continue;
            start(job);
        }
    }

    private void stopOldJobs(LifecycleJob[] lifecycleJobs) {
        final Set<LifecycleJob> currentActiveJobs = new HashSet<LifecycleJob>(activeJobs);
        final Set<LifecycleJob> newActiveJobs = new HashSet<LifecycleJob>(Arrays.asList(lifecycleJobs));
        for (LifecycleJob job : currentActiveJobs) {
            if (newActiveJobs.contains(job))
                continue;
            stop(job);
        }
    }

    public void start(LifecycleJob job) {
        log.trace("starting LC: " + job);
        String name = job.getProjectId().length() > 0 ? job.getProjectId() + '_' + job.getName() : job.getName();
        if (jobFutures.containsKey(job) || jobConnectionPool.containsKey(job) || jobExecutorServices.containsKey(job))
            interruptJob(job);
        if (job.getEndDate() > 0 && job.getEndDate() <= TimeController.getInstance().getServerUtcTimeMillis()) {
            if (log.isTraceEnabled()) {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                String endDate = simpleDateFormat.format(new Date(job.getEndDate()));
                String currentDate = simpleDateFormat.format(TimeController.getInstance().getServerUtcTimeMillis());
                log.trace(String.format("job [%s] will not be scheduled due to obsolescence%nCurrent Date: %s%nEndDate: %s", name, currentDate, endDate));
            }
            return;
        }
        log.info("initializing - " + name);
        try {
            IDataSource dataSource = DataSourceFactory.newDataSource(job.getExecutorsCount());
            jobConnectionPool.put(job, dataSource);

            ThreadFactory threadFactory = NamedThreadFactory.newInstance(name);
            int threadsCount = 2;
            if (job.getEndDate() > 0)
                threadsCount++;
            ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(threadsCount, threadFactory);
            ExecutorService executorService = Executors.newFixedThreadPool(job.getExecutorsCount(), threadFactory);
            List<ExecutorService> executorServiceList = new ArrayList<ExecutorService>();
            jobExecutorServices.put(job, executorServiceList);
            executorServiceList.add(scheduledExecutorService);
            executorServiceList.add(executorService);

            EventLogger eventLogger = EventLogger.newInstnce(job.getProjectId(), job.getName());
            LifecycleJobExecutor jobExecutor =
                    new LifecycleJobExecutor(jobInitializator, dbApplicationManager, job.getProjectId(), job.getProcedure(), dataSource,
                            eventLogger, executorService);
            Future future =
                    scheduledExecutorService.scheduleAtFixedRate(jobExecutor, job.getStartDate(),
                            job.getPeriodMS(), TimeUnit.MILLISECONDS);

            ArrayList<Future> futures = new ArrayList<Future>();
            jobFutures.put(job, futures);
            futures.add(future);

            Future loggerFuture = scheduledExecutorService.submit(eventLogger);
            futures.add(loggerFuture);

            if (job.getEndDate() > 0 && job.getEndDate() > TimeController.getInstance().getServerUtcTimeMillis()) {
                Future stopThreadFuture = scheduleStopThread(scheduledExecutorService, job.getEndDate(), job);
                futures.add(stopThreadFuture);
            }
            activeJobs.add(job);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            interruptJob(job);
        }
    }

    public void close() {
        log.debug("stopping...");
        for (LifecycleJob job : activeJobs)
            stop(job);
    }

    public void stop(LifecycleJob job) {
        interruptJob(job);
    }

    private void interruptJob(LifecycleJob job) {
        log.trace("stopping LC: " + job);
        String name = job.getProjectId().length() > 0 ? job.getProjectId() + '_' + job.getName() : job.getName();
        log.info("disposing - " + name);
        List<Future> futures = jobFutures.remove(job);
        if (null != futures)
            for (Future future : futures)
                future.cancel(true);

        List<ExecutorService> executors = jobExecutorServices.remove(job);
        if (null != executors)
            for (ExecutorService executorService : executors)
                executorService.shutdownNow();

        IDataSource dataSource = jobConnectionPool.remove(job);
        if (null != dataSource)
            dataSource.close();
        activeJobs.remove(job);
    }

    private Future scheduleStopThread(final ScheduledExecutorService executorService, final long endDate, final LifecycleJob lifecycleJob) {
        Runnable stopExecution = new Runnable() {
            @Override
            public void run() {
                interruptJob(lifecycleJob);
                Thread.currentThread().interrupt();
            }
        };
        return executorService.schedule(stopExecution, endDate - TimeController.getInstance().getServerUtcTimeMillis(),
                TimeUnit.MILLISECONDS);
    }

}
