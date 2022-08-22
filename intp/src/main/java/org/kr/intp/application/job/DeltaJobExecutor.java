package org.kr.intp.application.job;

import org.kr.intp.IntpMessages;
import org.kr.intp.application.AppContext;
import org.kr.intp.application.pojo.event.EventFactory;
import org.kr.intp.application.pojo.job.ProjectTableKey;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.CloseableResource;
import org.kr.intp.util.db.TimeController;
import org.kr.intp.util.db.pool.IDataSource;
import org.kr.intp.util.thread.NamedThreadFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by kr on 25.12.13.
 */
public class DeltaJobExecutor implements Runnable, CloseableResource {

    private final IntpMessages intpMessages = AppContext.instance().getIntpMessages();
    private final Logger log = LoggerFactory.getLogger(DeltaJobExecutor.class);
    private final boolean isTraceEnabled = log.isTraceEnabled();
    private final DeltaKeyProcessor keyProcessor;
    private final ProjectTableKey[] keysDefinition;
    private final ExecutorService deltaJobExecutor;
    private final String projectId;
    private final Lock deltaExecutionLock = new ReentrantLock();
    private final Set<Key> keysSet = new HashSet<>();
    private final EventFactory eventFactory = EventFactory.newInstance();
    private final EventLogger eventLogger;
    private final JobObserver jobObserver;
    private final ActionLogger actionLogger;
    private KeysReader keysReader;

    private long firstKeyAddedTime;

    public DeltaJobExecutor(DeltaKeyProcessor keyProcessor, ProjectTableKey[] keysDefinition, String projectId,
                            EventLogger eventLogger, JobObserver jobObserver, int keysCount, String keysSqlStatement,
                            IDataSource dataSource) {
        this.projectId = projectId;
        this.actionLogger = ActionLogger.getInstance(projectId);
        this.eventLogger = eventLogger;
        this.jobObserver = jobObserver;
        this.keyProcessor = keyProcessor;
        this.keysDefinition = keysDefinition;
        this.deltaJobExecutor = Executors.newCachedThreadPool(NamedThreadFactory.newInstance(projectId + "_DJE"));
        this.keysReader = new KeysReader(projectId, dataSource, keysSqlStatement, keysCount);
    }

    public void run() {
        if (Thread.currentThread().isInterrupted())
            close();
        executeDeltaJob();
    }

    public boolean addKeys(Collection<Object[]> keys) {
        try {
            deltaExecutionLock.lock();
            if (keysSet.size() == 0)
                firstKeyAddedTime = TimeController.getInstance().getServerUtcTimeMillis();
            boolean added = false;
            for (Object[] key : keys)
                added |= keysSet.add(new Key(key));
            return added;
        } finally {
            deltaExecutionLock.unlock();
        }
    }

    private void executeDeltaJob() {
        try {
            deltaExecutionLock.lock();
            final String uuid = UUID.randomUUID().toString();
            try {
                List<Object[]> keys = keysReader.getKeys(uuid);
                this.addKeys(keys);
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }

            if (0 == keysSet.size()) {
                if (isTraceEnabled)
                    log.trace(String.format("%s no keys to be processed, skipping delta.", projectId));
                return;
            }
            final boolean startAllowed = jobObserver.tryStartDelta();
            if (!startAllowed)
                return;
            log.info(String.format("%s delta triggered: %s", projectId, uuid));
            actionLogger.log(String.format("delta running: %s", uuid));
            final long firstKeyTriggered = firstKeyAddedTime;
            eventLogger.log(eventFactory.newTriggeredEvent(uuid, TimeController.getInstance().getServerUtcTimeMillis(),
                    firstKeyTriggered));
            eventLogger.log(eventFactory.newWaitingEvent(uuid));
            final List<Key> keysList = new ArrayList<Key>();
            keysList.addAll(keysSet);
            keysSet.clear();
            Runnable worker = new Runnable() {
                @Override
                public void run() {
                    DeltaJobExecutor.this.executeDeltaJob(uuid, keysList);
                }
            };
            deltaJobExecutor.submit(worker);
        } finally {
            deltaExecutionLock.unlock();
        }
    }

    private void executeDeltaJob(String uuid, List<Key> keysList) {
        log.debug(String.format("%s Delta Execution started [%s], %d keys to be processed...", projectId, uuid, keysList.size()));
        if (isTraceEnabled)
            log.trace(projectId + " [" + uuid + "] Keys to be processed: " + keysList);
        final List<Object[]> input = new ArrayList<Object[]>();
        for (Key key : keysList)
            input.add(key.getData());

        keyProcessor.process(uuid, input);
    }

    public void close() {
        String message = intpMessages.getString("org.kr.intp.application.job.deltajobexecutor.001",
                "%s stopping...");
        log.info(String.format(message, projectId));
        actionLogger.close();
        keyProcessor.close();
        deltaJobExecutor.shutdownNow();
    }

    private class Key {

        private final Object[] o;

        public Key(Object[] o) {
            this.o = o;
        }

        public boolean equals(Object other) {
            if (!(other instanceof Key))
                return false;
            Object[] k = ((Key)other).getData();
            return Arrays.equals(o, k);
        }

        public int hashCode() {
            return Arrays.hashCode(o);
        }

        public Object[] getData() {
            return o;
        }

        public String toString() {
            return Arrays.toString(o);
        }
    }
}
