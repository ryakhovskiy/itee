package org.kr.intp.application.job;

import org.kr.intp.IntpMessages;
import org.kr.intp.application.AppContext;
import org.kr.intp.application.job.hooks.HookManager;
import org.kr.intp.application.job.hooks.HookType;
import org.kr.intp.application.job.optimization.binpacking.Bin;
import org.kr.intp.application.job.optimization.binpacking.BinItem;
import org.kr.intp.application.job.optimization.binpacking.ItemExceedsCapacityException;
import org.kr.intp.application.job.optimization.binpacking.algorithms.BinPackingAlgorithm;
import org.kr.intp.application.job.optimization.binpacking.algorithms.FirstFitDecreasing;
import org.kr.intp.application.manager.DbApplicationManager;
import org.kr.intp.application.pojo.event.EventFactory;
import org.kr.intp.application.pojo.job.ProjectTableKey;
import org.kr.intp.application.pojo.job.SpecialProcedureMapping;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.CloseableResource;
import org.kr.intp.util.db.TimeController;
import org.kr.intp.util.db.pool.IDataSource;
import org.kr.intp.util.db.pool.SAPHistoricalDataSource;
import org.kr.intp.util.thread.NamedThreadFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class InitialJobExecutor implements Runnable, CloseableResource {

    private final IntpConfig config = AppContext.instance().getConfiguration();
    private final boolean clearEmptyLogActivities;
    private final IntpMessages intpMessages = AppContext.instance().getIntpMessages();
    private final Logger log = LoggerFactory.getLogger(InitialJobExecutor.class);
    private final boolean isDebugEnabled = log.isDebugEnabled();
    private final JobObserver jobObserver;
    private final EventFactory eventFactory = EventFactory.newInstance();
    private final EventLogger eventLogger;
    private KeysReader keysReader;
    private final IDataSource dataSource;
    private final String processSqlStatement;
    private final String projectId;
    private final int keysCount;
    private final ActionLogger actionLogger;
    private final HookManager hookManager;
    private final KeysSorter keysSorter;
    private final List<Integer> sequentialKeyIDs = new ArrayList<Integer>();
    private final SpecialProcedureMapping spMapping;
    private final long keyProcessingTotalTimeoutMS;
    private final long keyProcessingRetryPauseMS;
    private final long keyProcessingExecutionMaxDurationMS;
    private final JobInitializator jobInitializator;
    private final DbApplicationManager dbApplicationManager;

    // Executor Optimization
    private final int maxExecutors;
    private final String executorOptimizationStrategy;
    private ExecutorService[] executorPool;

    public InitialJobExecutor(JobInitializator jobInitializator, DbApplicationManager dbApplicationManager,
                              String projectId, KeysReader keysReader, String processSqlStatement, int processors,
                              int keysCount, EventLogger eventLogger, JobObserver jobObserver, HookManager hookManager,
                              IDataSource dataSource, ProjectTableKey[] keysDefinition,
                              SpecialProcedureMapping spMapping, long keyProcessingTotalTimeoutMS,
                              long keyProcessingRetryPauseMS, long keyProcessingExecutionMaxDurationMS) {
        this.clearEmptyLogActivities = config.isClearEmptyLogActivitiesEnabled();
        this.jobInitializator = jobInitializator;
        this.dbApplicationManager = dbApplicationManager;
        this.projectId = projectId;
        this.actionLogger = ActionLogger.getInstance(projectId);
        this.keysCount = keysCount;
        this.eventLogger = eventLogger;
        this.jobObserver = jobObserver;
        this.dataSource = dataSource;
        this.keysReader = keysReader;
        this.hookManager = hookManager;
        this.processSqlStatement = processSqlStatement;
        this.keysSorter = new KeysSorter(keysDefinition);
        for (ProjectTableKey ptk : keysDefinition)
            if (ptk.isSequential())
                sequentialKeyIDs.add(ptk.getNumber());
        this.spMapping = spMapping;
        this.maxExecutors = processors;
        this.keyProcessingExecutionMaxDurationMS = keyProcessingExecutionMaxDurationMS;
        this.keyProcessingRetryPauseMS = keyProcessingRetryPauseMS;
        this.keyProcessingTotalTimeoutMS = keyProcessingTotalTimeoutMS;
        this.executorOptimizationStrategy = config.getExecutorOptimizationStrategy();
    }

    //TODO: refactor
    public void run() {
        final long triggeredTime = TimeController.getInstance().getServerUtcTimeMillis();
        final boolean startAllowed = jobObserver.tryStartInitial();
        String message = "";
        if (!startAllowed)
            return;
        final String uuid = UUID.randomUUID().toString();
        int keysCountToBeProcessed = 0;
        boolean hasErrorsGlobal = false;
        String keysProcessingErrorMessage = "";
        Throwable executionException = null;
        try {
            prepareRun(triggeredTime, uuid);
            final long start = TimeController.getInstance().getServerUtcTimeMillis();
            if (dataSource instanceof SAPHistoricalDataSource)
                ((SAPHistoricalDataSource) dataSource).reset(start);

            //================================================================================
            // PRE_INITIAL phase
            //================================================================================

            hookManager.invokeHooks(HookType.PRE_INITIAL, uuid, start);

            //================================================================================
            // PRE_MAIN phase
            //================================================================================

            final List<Object[]> keys = keysReader.getKeys(uuid);
            keysCountToBeProcessed = keys.size();
            if (isDebugEnabled)
                log.debug(projectId + " Keys to be processed: " + keysCountToBeProcessed);
            if (keysCountToBeProcessed == 0)
                return;
            final List<KeysProcessor> processors = new ArrayList<KeysProcessor>(keysCountToBeProcessed);
            final Map<String, List<Object[]>> sorted = keysSorter.sort(keys);
            eventLogger.log(eventFactory.newStartedEvent(uuid, TimeController.getInstance().getServerUtcTimeMillis()));
            for (String key : sorted.keySet()) {
                final long sstart = TimeController.getInstance().getServerUtcTimeMillis();
                log.debug("processing sorted: " + key);

                //================================================================================
                // PRE_INITIAL_SEQUENTIAL phase
                //================================================================================

                // Retrieve sequential keys in order to pass them as parameters
                final List<Object[]> skeys = sorted.get(key);
                final List<Object> sequentialKeys = new ArrayList<Object>();
                if (skeys.size() > 0) {
                    Object[] k = skeys.get(0);
                    for (Integer sequentialKeyID : sequentialKeyIDs)
                        sequentialKeys.add(k[sequentialKeyID - 1]);
                }

                // Invoke PRE_INITIAL_SEQUENTIAL hooks
                hookManager.invokeHooks(HookType.PRE_INITIAL_SEQUENTIAL, uuid, start, sequentialKeys.toArray());
                processors.clear();

                //================================================================================
                // MAIN phase
                //================================================================================
                boolean useAutoMode = false;
                List<Future<KeysProcessor.KeysProcessorResult>> futures = new ArrayList<Future<KeysProcessor.KeysProcessorResult>>();

                // Check executor optimzation mode
                if (executorOptimizationStrategy.equals("ffd") || executorOptimizationStrategy.equals("ffd-auto")) {
                    // Initialize bins
                    List<BinItem<Object[]>> items = new ArrayList<BinItem<Object[]>>();

                    // Convert keys to BinItems
                    for (Object[] k : skeys) {
                        if (k.length <= keysCount) {
                            useAutoMode = true;
                            log.trace("Cannot use executor optimization as no effort column is given.");
                            break;
                        }
                        Object[] sKey = new Object[k.length - 1];
                        System.arraycopy(k, 0, sKey, 0, k.length - 1);
                        Object weightObject = k[k.length - 1];
                        int weight = 0;
                        if (weightObject instanceof Integer)
                            weight = (Integer) weightObject;
                        else if (weightObject instanceof Double)
                            weight = ((Double) weightObject).intValue();
                        else if (weightObject instanceof Long)
                            weight = ((Long) weightObject).intValue();
                        else {
                            useAutoMode = true;
                            break;
                        }
                        items.add(new BinItem<Object[]>(weight, sKey));
                    }
                    if (!useAutoMode) {
                        // Initialize Algorithm
                        BinPackingAlgorithm<Object[]> algorithm = new FirstFitDecreasing<Object[]>();
                        List<Bin<Object[]>> binResult = null;
                        try {
                            binResult = algorithm.execute(-1, items);
                        } catch (ItemExceedsCapacityException e) {
                            log.error("FFD could not be executed." + e.getMessage());
                            useAutoMode = true;
                            break;
                        }
                        log.info("optimization: optimal number of executors: " + binResult.size());
                        if (binResult.size() > maxExecutors) {
                            useAutoMode = true;
                        } else {
                            if (executorOptimizationStrategy.equals("ffd-auto")) {
                                executeFFDAuto(uuid, start, processors, futures, binResult);
                            } else if (executorOptimizationStrategy.equals("ffd")) {
                                executeFFD(uuid, start, processors, futures, binResult);
                            }
                        }
                    }
                }

                // Retrieve Executor Optimization Strategy
                if (executorOptimizationStrategy.equals("auto") || useAutoMode) {
                    log.trace("executor optimization: using auto mode.");
                    this.executorPool = new ExecutorService[1];
                    this.executorPool[0] = Executors.newFixedThreadPool(maxExecutors, NamedThreadFactory.newInstance(projectId + "_IJE"));

                    // Create KeysProcessor for each key
                    for (Object[] messages : skeys) {
                        Object[] keyWithoutEffort;
                        if (messages.length <= keysCount) {
                            keyWithoutEffort = messages;
                        } else {
                            keyWithoutEffort = new Object[messages.length - 1];
                            System.arraycopy(messages, 0, keyWithoutEffort, 0, messages.length - 1);
                        }
                        KeysProcessor kp = new KeysProcessor(
                                processSqlStatement, dataSource, eventLogger, keysCount,
                                projectId, uuid, keyWithoutEffort, start, spMapping,
                                keyProcessingExecutionMaxDurationMS,
                                keyProcessingTotalTimeoutMS,
                                keyProcessingRetryPauseMS);
                        processors.add(kp);
                    }

                    // Assign KeysProcessor to executors
                    futures = executorPool[0].invokeAll(processors);
                }

                // Retrieve results from KeysProcessors
                boolean hasErrors = false;
                for (Future<KeysProcessor.KeysProcessorResult> f : futures) {
                    KeysProcessor.KeysProcessorResult result = f.get();
                    if (result.getProcessResult() == KeysProcessor.ProcessResult.ERROR) {
                        hasErrors = true;
                        hasErrorsGlobal = true;
                        message = intpMessages.getString("org.kr.intp.application.job.initialjobexecutor.001",
                                "Initial job has errors, skipping hooks: ");
                        log.warn(message + result.getInfo());
                        keysProcessingErrorMessage = "Initial job has errors: " + result.getInfo();
                        break;
                    }
                }
                if (!hasErrors)
                    //================================================================================
                    // POST_INITIAL_SEQUENTIAL phase
                    //================================================================================
                    hookManager.invokeHooks(HookType.POST_INITIAL_SEQUENTIAL, uuid, start, sequentialKeys.toArray());
                else {
                    message = intpMessages.getString("org.kr.intp.application.job.initialjobexecutor.002",
                            "Initial job has errors, breaking.");
                    log.warn(message);
                    break;
                }
                if (isDebugEnabled) {
                    final long sduration = TimeController.getInstance().getServerUtcTimeMillis() - sstart;
                    log.debug("done processing sorted: " + key + " in " + sduration + " ms");
                }
            }
            //================================================================================
            // POST_INITIAL phase
            //================================================================================
            if (!hasErrorsGlobal)
                hookManager.invokeHooks(HookType.POST_INITIAL, uuid, start);
            final long duration = TimeController.getInstance().getServerUtcTimeMillis() - start;
            log.debug(String.format("%s All job processing took %d ms, UUID: %s", projectId, duration, uuid));
        } catch (InterruptedException e) {
            hasErrorsGlobal = true;
            message = intpMessages.getString("org.kr.intp.application.job.initialjobexecutor.003",
                    "Initial has been interrupted.");
            log.info(projectId + " " + message);
            executionException = e;
        } catch (ExecutionException e) {
            hasErrorsGlobal = true;
            message = intpMessages.getString("org.kr.intp.application.job.initialjobexecutor.004",
                    " Initial has errors: ") + e.getMessage();
            executionException = e;
        } catch (Throwable e) {
            hasErrorsGlobal = true;
            message = e.getMessage();
            executionException = e;
        } finally {
            handleErrors(hasErrorsGlobal, executionException, message, keysProcessingErrorMessage, uuid);
            if (keysCountToBeProcessed > 0) {
                eventLogger.log(eventFactory.newFinishedEvent(uuid,
                        TimeController.getInstance().getServerUtcTimeMillis()));
            } else if (clearEmptyLogActivities) {
                eventLogger.log(eventFactory.newClearEvent(uuid));
            }
            jobObserver.setInitialFinished(uuid, hasErrorsGlobal);
            shutdownExecutors();
        }
    }

    private void handleErrors(boolean hasError, Throwable e, String message, String keysProcessingErrorMessage, String uuid) {
        if (!hasError)
            return;
        try {
            if (null == e)
                e = new Exception(keysProcessingErrorMessage + "; " + message);
            message = message + "; " + e.getMessage();
            eventLogger.log(eventFactory.newErrorEvent(uuid, e));
            log.error(message, e);
            dbApplicationManager.stopOnError(projectId, keysProcessingErrorMessage);
        } finally {
            jobInitializator.interruptJob(projectId);
        }
    }

    private void executeFFD(String uuid, long start, List<KeysProcessor> processors,
                            List<Future<KeysProcessor.KeysProcessorResult>> futures,
                            List<Bin<Object[]>> binResult) {
        // Initialize ExecutorService
        this.executorPool = new ExecutorService[binResult.size()];
        for (int i = 0; i < executorPool.length; i++) {
            executorPool[i] = Executors.newSingleThreadExecutor(NamedThreadFactory.newInstance(projectId + "_IJE" + i));
        }

        for (int i = 0; i < binResult.size(); i++) {
            Bin<Object[]> currentBin = binResult.get(i);
            for (BinItem<Object[]> k : currentBin.getItems()) {
                KeysProcessor kp = new KeysProcessor(
                        processSqlStatement, dataSource, eventLogger, keysCount,
                        projectId, uuid, k.getItem(), start, spMapping,
                        keyProcessingExecutionMaxDurationMS,
                        keyProcessingTotalTimeoutMS,
                        keyProcessingRetryPauseMS);
                processors.add(kp);
            }
            for (KeysProcessor kp : processors) {
                futures.add(executorPool[i].submit(kp));
            }
            processors.clear();
        }
    }

    private void executeFFDAuto(String uuid, long start, List<KeysProcessor> processors,
                                List<Future<KeysProcessor.KeysProcessorResult>> futures,
                                List<Bin<Object[]>> binResult) {
        this.executorPool = new ExecutorService[binResult.size()];
        this.executorPool[0] = Executors.newFixedThreadPool(binResult.size(), NamedThreadFactory.newInstance(projectId + "_IJE"));

        for (int i = 0; i < binResult.size(); i++) {
            Bin<Object[]> currentBin = binResult.get(i);
            if (i == 0) {
                for (BinItem<Object[]> k : currentBin.getItems()) {
                    KeysProcessor kp = new KeysProcessor(
                            processSqlStatement, dataSource, eventLogger, keysCount,
                            projectId, uuid, k.getItem(), start, spMapping,
                            keyProcessingExecutionMaxDurationMS,
                            keyProcessingTotalTimeoutMS,
                            keyProcessingRetryPauseMS);
                    processors.add(kp);
                }
                for (KeysProcessor kp : processors) {
                    futures.add(executorPool[0].submit(kp));
                }
                processors.clear();
            } else {
                for (int j = currentBin.getItems().size() - 1; j >= 0; j--) {
                    BinItem<Object[]> k = currentBin.getItems().get(j);
                    KeysProcessor kp = new KeysProcessor(
                            processSqlStatement, dataSource, eventLogger, keysCount,
                            projectId, uuid, k.getItem(), start, spMapping,
                            keyProcessingExecutionMaxDurationMS,
                            keyProcessingTotalTimeoutMS,
                            keyProcessingRetryPauseMS);
                    processors.add(kp);
                }
                for (KeysProcessor kp : processors) {
                    futures.add(executorPool[0].submit(kp));
                }
                processors.clear();
            }
        }
    }

    private void prepareRun(long triggeredTime, String uuid) throws InterruptedException {
        actionLogger.log("initial running: " + uuid);
        log.info(projectId + " Initial started: " + uuid);
        eventLogger.log(eventFactory.newTriggeredEvent(uuid, triggeredTime));
        eventLogger.log(eventFactory.newWaitingEvent(uuid));
        jobObserver.awaitForDeltasCompletion();
    }

    private void shutdownExecutors() {
        if (null == executorPool) {
            return;
        }
        for (ExecutorService e : executorPool) {
            if (null == e)
                continue;
            e.shutdownNow();
        }
    }

    public void close() {
        actionLogger.close();
    }
}