package org.kr.intp.application.job.lifecycle;

import org.kr.intp.application.job.JobInitializator;
import org.kr.intp.application.manager.DbApplicationManager;
import org.kr.intp.application.pojo.job.LifecycleJob;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.CloseableResource;
import org.kr.intp.util.db.pool.DataSourceFactory;
import org.kr.intp.util.db.pool.IDataSource;
import org.kr.intp.util.thread.NamedThreadFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created bykron 09.09.2014.
 */
public class ProjectLcManager implements CloseableResource {

    private final Logger log = LoggerFactory.getLogger(ProjectLcManager.class);
    private final String projectId;
    private final List<ExecutorService> executors = new ArrayList<ExecutorService>();
    private final List<IDataSource> connectionPools = new ArrayList<IDataSource>();
    private final JobInitializator jobInitializator;
    private final DbApplicationManager dbAppManager;

    public ProjectLcManager(JobInitializator jobInitializator, DbApplicationManager dbAppManager, String projectId) throws SQLException {
        this.jobInitializator = jobInitializator;
        this.dbAppManager = dbAppManager;
        this.projectId = projectId;
    }

    public void runLC() throws SQLException {
        final LifecycleJob[] jobs = LifecycleJobReader.getInstance().getActiveLifecycleJobs(projectId);
        for (LifecycleJob job : jobs) {
            final ScheduledExecutorService executor = Executors.newScheduledThreadPool(job.getExecutorsCount(),
                    NamedThreadFactory.newInstance(projectId + "_LC"));
            final ExecutorService executorService = Executors.newFixedThreadPool(2,
                    NamedThreadFactory.newInstance(projectId + "_LC"));
            final IDataSource dataSource = DataSourceFactory.newDataSource(job.getExecutorsCount());
            final EventLogger logger = EventLogger.newInstnce(projectId, job.getName());
            final LifecycleJobExecutor jobExecutor = new LifecycleJobExecutor(jobInitializator, dbAppManager, job.getProjectId(),
                    job.getProcedure(), dataSource, logger, executor);

            executorService.submit(logger);
            executorService.submit(jobExecutor);

            executors.add(executor);
            executors.add(executorService);
            connectionPools.add(dataSource);
        }
    }


    @Override
    public void close() {
        log.debug("closing LC Manager: " + projectId);

        for (ExecutorService es : executors)
            es.shutdownNow();

        for (IDataSource cp : connectionPools)
            cp.close();
    }

}
