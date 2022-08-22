package org.kr.intp.application.job;

import org.kr.intp.application.job.retry.RetryingSapCallableStatementInvoker;
import org.kr.intp.application.pojo.event.EventFactory;
import org.kr.intp.application.pojo.job.SpecialProcedureMapping;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.TimeController;
import org.kr.intp.util.db.pool.SAPHistoricalDataSource;
import org.kr.intp.util.retry.RetryingInvokable;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.concurrent.Callable;

public class KeysProcessor implements Callable<KeysProcessor.KeysProcessorResult> {

    private final Logger log = LoggerFactory.getLogger(KeysProcessor.class);
    private final boolean isTraceEnabled = log.isTraceEnabled();
    private final EventLogger eventLogger;
    private final EventFactory eventFactory = EventFactory.newInstance();
    private final String projectId;
    private final int keysCount;
    private final String uuid;
    private final Object[] message;
    private final DataSource dataSource;
    private final String processSqlStatement;
    private final Timestamp starttime;
    private final SpecialProcedureMapping spMapping;
    private final boolean mappingEnabled;
    private final RetryingSapCallableStatementInvoker invoker;
    private final long keyProcessingTotalTimeoutMS;

    public KeysProcessor(String processSqlStatement, DataSource dataSource, EventLogger eventLogger,
                         int keysCount, String projectId, String uuid, Object[] message, long time,
                         long keyProcessingExecutionMaxDurationMS,
                         long keyProcessingTotalTimeoutMS,
                         long keyProcessingRetryPauseMS) {
        this(processSqlStatement, dataSource, eventLogger, keysCount, projectId,
                uuid, message, time, null, keyProcessingExecutionMaxDurationMS, keyProcessingTotalTimeoutMS,
                keyProcessingRetryPauseMS);
    }

    public KeysProcessor(String processSqlStatement, DataSource dataSource, EventLogger eventLogger,
                         int keysCount, String projectId, String uuid, Object[] message, long time,
                         SpecialProcedureMapping spMapping,
                         long keyProcessingExecutionMaxDurationMS,
                         long keyProcessingTotalTimeoutMS,
                         long keyProcessingRetryPauseMS) {
        this.processSqlStatement = processSqlStatement;
        this.dataSource = dataSource;
        this.eventLogger = eventLogger;
        this.keysCount = keysCount;
        this.projectId = projectId;
        this.uuid = uuid;
        this.message = message;
        this.starttime = new Timestamp(time);
        this.spMapping = spMapping;
        this.mappingEnabled = spMapping != null;
        this.keyProcessingTotalTimeoutMS = keyProcessingTotalTimeoutMS;
        final boolean retriesEnabled = keyProcessingExecutionMaxDurationMS == 0;
        if (retriesEnabled)
            invoker = new RetryingSapCallableStatementInvoker(
                    keyProcessingRetryPauseMS, keyProcessingExecutionMaxDurationMS, "KP");
        else
            invoker = null;
    }

    public KeysProcessorResult call() {
        final long start = TimeController.getInstance().getServerUtcTimeMillis();
        try {
            eventLogger.log(eventFactory.newProcessingEvent(uuid, start, message));
            if (isTraceEnabled)
                log.trace(projectId + "[" + uuid + "] starting processing keys: " + Arrays.toString(message));
            executeQuery(message);
            final long end = TimeController.getInstance().getServerUtcTimeMillis();
            eventLogger.log(eventFactory.newProcessedEvent(uuid, start, end, message));
            if (isTraceEnabled)
                log.trace(projectId + "[" + uuid + "] calling procedure for keys " + Arrays.toString(message) + " took (ms): " + (end - start));
            final long time = TimeController.getInstance().getServerUtcTimeMillis() - start;
            return new KeysProcessorResult(ProcessResult.OK, "succeed", time);
        } catch (Exception e) {
            String errorMessage = "Error while processing keys: " + Arrays.toString(message) + "; " + e.getMessage();
            if (e.getCause() instanceof SQLException) {
                e = (SQLException) e.getCause();
            }
            if (e instanceof SQLException) {
                errorMessage += "; SQL error code: " + ((SQLException) e).getErrorCode()
                        + "; SQL State: " + ((SQLException) e).getSQLState();
            }
            log.error(errorMessage, e);
            eventLogger.log(eventFactory.newErrorEvent(uuid, e));
            final long time = TimeController.getInstance().getServerUtcTimeMillis() - start;
            return new KeysProcessorResult(ProcessResult.ERROR, errorMessage, time);
        }
    }

    private void executeQuery(final Object[] keys) throws SQLException {
        if (null != invoker) {
            RetryingInvokable invokable = new RetryingInvokable() {
                @Override
                public Object invoke() throws Exception {
                    KeysProcessor.this.invoke(keys);
                    return null;
                }
            };
            invoker.invokeWithRetries(invokable, keyProcessingTotalTimeoutMS);
        } else {
            invoke(keys);
        }
    }

    private void invoke(Object[] keys) throws SQLException {
        Connection connection = null;
        CallableStatement statement = null;
        try {
            connection = getConnection();
            statement = getStatement(connection);
            statement.setString(1, uuid);
            statement.setTimestamp(2, starttime);
            for (int i = 0; i < keysCount; i++)
                statement.setObject(i + 3, keys[i]);
            statement.execute();
            if (dataSource instanceof SAPHistoricalDataSource)
                connection.commit();
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private Connection getConnection() throws SQLException {
        long start = System.currentTimeMillis();
        Connection connection = dataSource.getConnection();
        long end = System.currentTimeMillis();
        if (isTraceEnabled)
            log.trace(projectId + " Retrieving connection took (ms): " + (end - start));
        return connection;
    }

    private CallableStatement getStatement(Connection connection) throws SQLException {
        long start = System.currentTimeMillis();
        String sql = processSqlStatement;
        if (mappingEnabled) {
            for (String value : spMapping.getMapping().keySet()) {
                if (message[spMapping.getKeyId() - 1].equals(value)) {
                    sql = spMapping.getMapping().get(value);
                    break;
                }
            }
        }
        CallableStatement statement = connection.prepareCall(sql); //statement should be pooled
        long end = System.currentTimeMillis();
        if (isTraceEnabled)
            log.trace(projectId + " Preparing statement took (ms): " + (end - start) + "; " + sql);
        return statement;
    }

    public enum ProcessResult {
        OK,
        ERROR
    }

    public class KeysProcessorResult {
        private final ProcessResult processResult;
        private final String info;
        private final long time;
        private KeysProcessorResult(ProcessResult processResult, String info, long time) {
            this.processResult = processResult;
            this.info = info;
            this.time = time;
        }
        public ProcessResult getProcessResult() { return processResult; }
        public long getTime() { return time; }
        public String getInfo() { return info; }
    }
}