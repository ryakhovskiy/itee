package org.kr.intp.application.job;

import org.kr.intp.application.job.hooks.HookManager;
import org.kr.intp.application.job.hooks.HookType;
import org.kr.intp.application.manager.DbApplicationManager;
import org.kr.intp.application.pojo.event.EventFactory;
import org.kr.intp.application.pojo.job.ProjectTableKey;
import org.kr.intp.application.pojo.job.SpecialProcedureMapping;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.TimeController;
import org.kr.intp.util.db.pool.IDataSource;
import org.kr.intp.util.db.pool.SAPHistoricalDataSource;
import org.kr.intp.util.thread.NamedThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by kr on 1/16/14.
 */
public class DeltaKeyProcessor {

    private final Logger log = LoggerFactory.getLogger(KeysReader.class);
    private final IDataSource dataSource;
    private final String processSqlStatement;
    private final int keysCount;
    private final String projectId;
    private final EventFactory eventFactory = EventFactory.newInstance();
    private final EventLogger eventLogger;
    private final ExecutorService executor;
    private final ExecutorService keyProcessorExecutorService;
    private final JobObserver jobObserver;
    private final KeysSorter keysSorter;
    private final HookManager hookManager;
    private final List<Integer> sequentialKeyIDs = new ArrayList<Integer>();
    private final SpecialProcedureMapping spMapping;
    private final long keyProcessingTotalTimeoutMS;
    private final long keyProcessingRetryPauseMS;
    private final long keyProcessingExecutionMaxDurationMS;
    private final JobInitializator jobInitializator;
    private final DbApplicationManager dbApplicationManager;

    public DeltaKeyProcessor(JobInitializator jobInitializator, DbApplicationManager dbApplicationManager,
                             String projectId, String processSqlStatement, int keysCount, EventLogger eventLogger,
                             int processors, JobObserver jobObserver, IDataSource dataSource,
                             HookManager hookManager, ProjectTableKey[] keysDefinitions,
                             SpecialProcedureMapping spMapping, long keyProcessingTotalTimeoutMS, long keyProcessingRetryPauseMS, long keyProcessingExecutionMaxDurationMS) {
        this.jobInitializator = jobInitializator;
        this.dbApplicationManager = dbApplicationManager;
        this.projectId = projectId;
        this.processSqlStatement = processSqlStatement;
        this.dataSource = dataSource;
        this.keysCount = keysCount;
        this.eventLogger = eventLogger;
        this.keyProcessorExecutorService = Executors.newFixedThreadPool(processors, NamedThreadFactory.newInstance(projectId + "_DKP"));
        this.executor = Executors.newCachedThreadPool(NamedThreadFactory.newInstance(projectId + "_DKPS"));
        this.jobObserver = jobObserver;
        this.hookManager = hookManager;
        this.keysSorter = new KeysSorter(keysDefinitions);
        for (ProjectTableKey ptk : keysDefinitions)
            if (ptk.isSequential())
                sequentialKeyIDs.add(ptk.getNumber());
        this.spMapping = spMapping;
        this.keyProcessingExecutionMaxDurationMS = keyProcessingExecutionMaxDurationMS;
        this.keyProcessingRetryPauseMS = keyProcessingRetryPauseMS;
        this.keyProcessingTotalTimeoutMS = keyProcessingTotalTimeoutMS;
    }

    public void process(final String uuid, final List<Object[]> keys) {
        log.info(String.format("%s - Processing %d keys; UUID: %s", projectId, keys.size(), uuid));
        final long start = TimeController.getInstance().getServerUtcTimeMillis();
        eventLogger.log(eventFactory.newStartedEvent(uuid, start));
        Runnable processor = getProcessor(uuid, keys);
        if (dataSource instanceof SAPHistoricalDataSource)
            ((SAPHistoricalDataSource)dataSource).reset(start);
        executor.execute(processor);
    }

    private Runnable getProcessor(final String uuid, final List<Object[]> keys) {
        return new Runnable() {
            @Override
            public void run() {
                boolean keysProcessingHasErrors = false;
                String keysProcessingErrorMessage = "";
                try {

                    final long start = TimeController.getInstance().getServerUtcTimeMillis();

                    hookManager.invokeHooks(HookType.PRE_DELTA, uuid, start);
                    final List<KeysProcessor> processors = new ArrayList<KeysProcessor>(keys.size());
                    final Map<String, List<Object[]>> sorted = keysSorter.sort(keys);
                    for (String key : sorted.keySet()) {
                        log.debug("processing sorted: " + key);
                        final List<Object[]> skeys = sorted.get(key);
                        List<Object> sequentialKeys = new ArrayList<Object>();
                        if (skeys.size() > 0) {
                            Object[] k = skeys.get(0);
                            for (Integer sequentialKeyID : sequentialKeyIDs)
                                sequentialKeys.add(k[sequentialKeyID - 1]);
                        }
                        hookManager.invokeHooks(HookType.PRE_DELTA_SEQUENTIAL, uuid, start, sequentialKeys.toArray());
                        processors.clear();
                        for (Object[] message : skeys) {
                            KeysProcessor kp =
                                    new KeysProcessor(processSqlStatement, dataSource, eventLogger, keysCount,
                                            projectId, uuid, message, start, spMapping,
                                            keyProcessingExecutionMaxDurationMS,
                                            keyProcessingTotalTimeoutMS,
                                            keyProcessingRetryPauseMS);
                            processors.add(kp);
                        }
                        List<Future<KeysProcessor.KeysProcessorResult>> futures = keyProcessorExecutorService.invokeAll(processors);
                        for (Future<KeysProcessor.KeysProcessorResult> f : futures) {
                            KeysProcessor.KeysProcessorResult result = f.get();
                            if (result.getProcessResult() == KeysProcessor.ProcessResult.ERROR) {
                                keysProcessingHasErrors = true;
                                log.warn("Delta job has errors, skipping hooks: " + result.getInfo());
                                keysProcessingErrorMessage ="Delta job has errors: " + result.getInfo();
                                break;
                            }
                        }
                        if (!keysProcessingHasErrors)
                            hookManager.invokeHooks(HookType.POST_DELTA_SEQUENTIAL, uuid, start, sequentialKeys.toArray());
                        else {
                            log.warn("Delta job has errors, breaking.");
                            break;
                        }
                    }
                    if (!keysProcessingHasErrors)
                        hookManager.invokeHooks(HookType.POST_DELTA, uuid, start);
                    final long duration = TimeController.getInstance().getServerUtcTimeMillis() - start;
                    log.info(String.format("All job processing took %d ms, UUID: %s", duration, uuid));
                } catch (InterruptedException e) {
                    log.debug("interrupted, stopping...");
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    log.error("Job FAILED: " + projectId, e);
                    eventLogger.log(eventFactory.newErrorEvent(uuid, e));
                    log.warn("stopping project due to errors");
                    dbApplicationManager.stopOnError(projectId, e.getMessage());
                    jobInitializator.interruptJob(projectId);
                } finally {
                    eventLogger.log(eventFactory.newFinishedEvent(uuid,
                            TimeController.getInstance().getServerUtcTimeMillis()));
                    jobObserver.setDeltaFinished(uuid);
                }
                if (keysProcessingHasErrors) {
                    log.warn("stopping project due to errors");
                    dbApplicationManager.stopOnError(projectId, keysProcessingErrorMessage);
                    jobInitializator.interruptJob(projectId);
                }

            }
        };
    }

    public void close() {
        executor.shutdown();
        keyProcessorExecutorService.shutdown();
    }
}
