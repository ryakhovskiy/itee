package org.kr.intp.application.job;

import org.kr.intp.application.AppContext;
import org.kr.intp.application.pojo.event.*;
import org.kr.intp.application.pojo.job.ApplicationJob;
import org.kr.intp.application.pojo.job.ProjectTableKey;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 9/15/13
 * Time: 8:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class EventLogger implements Runnable {

    public static EventLogger getEventLogger(ApplicationJob job, ApplicationJob.JobType type) throws SQLException {
        return new EventLogger(job.getProjectId(), job.getTableName(), type, job.getKeys());
    }

    private final Logger log = LoggerFactory.getLogger(EventLogger.class);
    private final EventFactory eventFactory = EventFactory.newInstance();
    private final EventStatus eventStatus = EventStatus.getInstance();
    private final BlockingQueue<Event> logQueue = new LinkedBlockingQueue<Event>();
    private final String logTableName;
    private final char jobType;
    private final ProjectTableKey[] ptKeys;
    private final String projectId;
    private final IntpConfig config = AppContext.instance().getConfiguration();

    private EventLogger(String projectId, String tableName, ApplicationJob.JobType type, ProjectTableKey[] keys) throws SQLException {
        this.logTableName = String.format("%s_%s_RUN_LOG", projectId, tableName);
        if (log.isDebugEnabled())
            log.debug(projectId + " - preparing EventLogger; " + logTableName);
        this.projectId = projectId;
        this.jobType = type.getType().charAt(0);
        this.ptKeys = keys;
        if (log.isTraceEnabled())
            log.trace(projectId + " - EventLogger prepared; " + logTableName);
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Event event = logQueue.take();
                logEvent(event);
            } catch (InterruptedException e) {
                log.debug("EventLogger has been interrupted");
                Thread.currentThread().interrupt();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private String createUpdateOldUuidsAggregatedStatement() {
        final StringBuilder query = new StringBuilder();
        query.append("update ");
        query.append(config.getIntpGenObjectsSchema());
        query.append('.');
        query.append(logTableName);
        query.append(" set COMMENT = ifnull(COMMENT, '') || ? where UUID = ? and STATUS <> (select id from ");
        query.append(config.getIntpSchema());
        query.append(".RT_STATUS_DESC where NAME = 'A')");
        return query.toString();
    }

    private String createAggregatedEventStatement() {
        final StringBuilder query = new StringBuilder();
        query.append("insert into ");
        query.append(config.getIntpGenObjectsSchema());
        query.append('.');
        query.append(logTableName);
        query.append(" (TYPE, UPD_TIMESTAMP, UUID, STATUS, JOB_NAME, COMMENT) values ('");
        query.append(jobType);
        query.append("',current_utctimestamp,?,");
        query.append(EventType.AGGREGATION.getId());
        query.append(",'");
        query.append(this.projectId);
        query.append("',?)");
        return query.toString();
    }

    private String createClearEmptyActivitiesQuery() {
        final StringBuilder query = new StringBuilder();
        query.append("delete from ");
        query.append(config.getIntpGenObjectsSchema());
        query.append('.');
        query.append(logTableName);
        query.append(" where UUID = ?");
        return query.toString();
    }

    private String createTriggeredEventQuery() {
        //TODO: update query
        final StringBuilder query = new StringBuilder();
        query.append("insert into ");
        query.append(config.getIntpGenObjectsSchema());
        query.append('.');
        query.append(logTableName);
        query.append(" (TYPE, UPD_TIMESTAMP, UUID, STATUS, JOB_NAME, FK_TRIGGERED, TRIGGERED) values ('");
        query.append(jobType);
        query.append("',current_utctimestamp,?,");
        query.append(EventType.TRIGGERED.getId());
        query.append(",'");
        query.append(this.projectId);
        query.append("',?,?)");
        return query.toString();
    }

    private String createErrorEventQuery() {
        final StringBuilder query = new StringBuilder();
        query.append("insert into ");
        query.append(config.getIntpGenObjectsSchema());
        query.append('.');
        query.append(logTableName);
        query.append(" (TYPE, UPD_TIMESTAMP, UUID, STATUS, JOB_NAME, COMMENT) values('");
        query.append(jobType);
        query.append("',current_utctimestamp,?,");
        query.append(EventType.ERROR.getId());
        query.append(",'");
        query.append(this.projectId);
        query.append("',?)");
        return query.toString();
    }

    private String createWaitingEventQuery() {
        final StringBuilder query = new StringBuilder();
        query.append("insert into ");
        query.append(config.getIntpGenObjectsSchema());
        query.append('.');
        query.append(logTableName);
        query.append(" (TYPE, UPD_TIMESTAMP, UUID, STATUS, JOB_NAME) values('");
        query.append(jobType);
        query.append("',current_utctimestamp,?,");
        query.append(EventType.WAITING.getId());
        query.append(",'");
        query.append(this.projectId);
        query.append("')");
        return query.toString();
    }

    private String createStartedEventQuery() {
        final StringBuilder query = new StringBuilder();
        query.append("insert into ");
        query.append(config.getIntpGenObjectsSchema());
        query.append('.');
        query.append(logTableName);
        query.append(" (TYPE, UPD_TIMESTAMP, UUID, STATUS, JOB_NAME, STARTED) values ('");
        query.append(jobType);
        query.append("',current_utctimestamp,?,");
        query.append(EventType.STARTED.getId());
        query.append(",'");
        query.append(this.projectId);
        query.append("',?)");
        return query.toString();
    }

    private String createProcessingEventQuery(ProjectTableKey[] keys) {
        final StringBuilder query = new StringBuilder();
        query.append("insert into ");
        query.append(config.getIntpGenObjectsSchema());
        query.append('.');
        query.append(logTableName);
        query.append(" (UPD_TIMESTAMP, UUID, STARTED, JOB_NAME, TYPE, STATUS ");
        for (ProjectTableKey k : keys) {
            query.append(',');
            query.append(k.getKeyName());
        }
        query.append(") values (current_utctimestamp, ?, ?, '");
        query.append(projectId);
        query.append("', '");
        query.append(jobType);
        query.append("', ");
        query.append(EventType.PROCESSING.getId());
        for (int i = 0; i < keys.length; i++)
            query.append(", ?");
        query.append(')');
        return query.toString();
    }

    private String createProcessedEventQuery(ProjectTableKey[] keys) {
        final StringBuilder query = new StringBuilder();
        query.append("insert into ");
        query.append(config.getIntpGenObjectsSchema());
        query.append('.');
        query.append(logTableName);
        query.append(" (UPD_TIMESTAMP, UUID, STARTED, FINISHED, JOB_NAME, TYPE, STATUS ");
        for (ProjectTableKey k : keys) {
            query.append(',');
            query.append(k.getKeyName());
        }
        query.append(") values (current_utctimestamp, ?, ?, ?, '");
        query.append(projectId);
        query.append("', '");
        query.append(jobType);
        query.append("', ");
        query.append(EventType.PROCESSED.getId());
        for (int i = 0; i < keys.length; i++)
            query.append(", ?");
        query.append(')');
        return query.toString();
    }

    private String createFinishedEventQuery() {
        final StringBuilder query = new StringBuilder();
        query.append("insert into ");
        query.append(config.getIntpGenObjectsSchema());
        query.append('.');
        query.append(logTableName);
        query.append(" (TYPE, UPD_TIMESTAMP, UUID, STATUS, JOB_NAME, FINISHED) values ('");
        query.append(jobType);
        query.append("',current_utctimestamp,?,");
        query.append(EventType.FINISHED.getId());
        query.append(",'");
        query.append(this.projectId);
        query.append("',?)");
        return query.toString();
    }

    public void log(Event event) {
        try {
            logQueue.put(event);
        } catch (InterruptedException e) {
            log.debug("EventLogger has been interrupted");
            Thread.currentThread().interrupt();
        }
    }

    private void logEvent(Event event) throws SQLException {
        switch (event.getEventType()) {
            case TRIGGERED:
                logTriggeredEvent((TriggeredEvent)event);
                break;
            case WAITING:
                logWaitingEvent((WaitingEvent)event);
                break;
            case STARTED:
                logStartedEvent((StartedEvent)event);
                break;
            case PROCESSING:
                logProcessingEvent((ProcessingEvent)event);
                break;
            case PROCESSED:
                logProcessedEvent((ProcessedEvent)event);
                break;
            case ERROR:
                logErrorEvent((ErrorEvent)event);
                break;
            case FINISHED:
                logFinishedEvent((FinishedEvent)event);
                break;
            case CLEAR:
                clearEmptyLogActivities((ClearEvent)event);
                break;
            case AGGREGATION:
                logAggregationEvent((AggregationEvent)event);
                break;
            default:
                log.warn("Unsupported EventType: " + event.getEventType());
                break;
        }
    }

    private void logAggregationEvent(AggregationEvent event) throws SQLException {
        Connection connection = null;
        PreparedStatement logAggregatedEventStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            logAggregatedEventStatement = connection.prepareStatement(createAggregatedEventStatement());
            for (String uuid : event.getOldUuids()) {
                logAggregatedEventStatement.setString(1, uuid);
                logAggregatedEventStatement.setString(2, event.getUuid());
                logAggregatedEventStatement.execute();
            }
        } finally {
            if (null != logAggregatedEventStatement)
                logAggregatedEventStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void clearEmptyLogActivities(ClearEvent event) throws SQLException {
        Connection connection = null;
        PreparedStatement logClearEmptyActivitiesStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            logClearEmptyActivitiesStatement = connection.prepareStatement(createClearEmptyActivitiesQuery());
            logClearEmptyActivitiesStatement.setString(1, event.getUuid());
            logClearEmptyActivitiesStatement.execute();
        } finally {
            if (null != logClearEmptyActivitiesStatement)
                logClearEmptyActivitiesStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void logErrorEvent(ErrorEvent event) throws SQLException {
        Connection connection = null;
        PreparedStatement logErrorEventStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            logErrorEventStatement = connection.prepareStatement(createErrorEventQuery());
            logErrorEventStatement.setString(1, event.getUuid());
            logErrorEventStatement.setString(2, event.getError());
            logErrorEventStatement.execute();
        } finally {
            if (null != logErrorEventStatement)
                logErrorEventStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void logTriggeredEvent(TriggeredEvent event) throws SQLException {
        cleanupRunLog(event);
        Connection connection = null;
        PreparedStatement logTriggerEventStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            logTriggerEventStatement = connection.prepareStatement(createTriggeredEventQuery());
            logTriggerEventStatement.setString(1, event.getUuid());
            if (event.getFirstKeyTriggeredTime().getTime() == 0)
                logTriggerEventStatement.setTimestamp(2, event.getTriggeredTime());
            else
                logTriggerEventStatement.setTimestamp(2, event.getFirstKeyTriggeredTime());
            logTriggerEventStatement.setTimestamp(3, event.getTriggeredTime());
            logTriggerEventStatement.execute();
        } finally {
            if (null != logTriggerEventStatement)
                logTriggerEventStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void logWaitingEvent(WaitingEvent event) throws SQLException {
        Connection connection = null;
        PreparedStatement logWaitingEventStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            logWaitingEventStatement = connection.prepareStatement(createWaitingEventQuery());
            logWaitingEventStatement.setString(1, event.getUuid());
            logWaitingEventStatement.execute();
        } finally {
            if (null != logWaitingEventStatement)
                logWaitingEventStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void logStartedEvent(StartedEvent event) throws SQLException {
        Connection connection = null;
        PreparedStatement logStartedEventStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            logStartedEventStatement = connection.prepareStatement(createStartedEventQuery());
            logStartedEventStatement.setString(1, event.getUuid());
            logStartedEventStatement.setTimestamp(2, event.getStartedTime());
            logStartedEventStatement.execute();
        } finally {
            if (null != logStartedEventStatement)
                logStartedEventStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    /**
     * Cleans up the run log table if a job is started.
     * @throws SQLException
     */
    private void cleanupRunLog(TriggeredEvent event) throws SQLException{
        String cleanupStatement = String.format(
                "DELETE FROM \"%s\".\"%s\" WHERE TYPE = '%s' AND UUID != '%s';",
                config.getIntpGenObjectsSchema(), logTableName, jobType, event.getUuid());
        Connection connection = null;
        Statement stmt = null;
        try{
            connection = ServiceConnectionPool.instance().getConnection();
            stmt = connection.createStatement();
            stmt.execute(cleanupStatement);
        }
        finally{
            if(null!=stmt){
                stmt.close();
            }
            if(null!=connection){
                connection.close();
            }
        }
    }

    private void logProcessingEvent(ProcessingEvent event) throws SQLException {
        final Object[] keys = event.getKeys();
        Connection connection = null;
        PreparedStatement logProcessingEventStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            logProcessingEventStatement = connection.prepareStatement(createProcessingEventQuery(ptKeys));
            logProcessingEventStatement.setString(1, event.getUuid());
            logProcessingEventStatement.setTimestamp(2, event.getStartedTime());
            for (int i = 0; i < keys.length; i++) {
                //if (keys[i].equals(AGGREGATION_PLACEHOLDER))
                //    logProcessingEventStatement.setNull(i + 3, Types.JAVA_OBJECT);
                //else
                    logProcessingEventStatement.setObject(i + 3, keys[i]);
            }
            logProcessingEventStatement.execute();
        } finally {
            if (null != logProcessingEventStatement)
                logProcessingEventStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void logProcessedEvent(ProcessedEvent event) throws SQLException {
        Connection connection = null;
        PreparedStatement logProcessedEventStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            logProcessedEventStatement = connection.prepareStatement(createProcessedEventQuery(ptKeys));
            logProcessedEventStatement.setString(1, event.getUuid());
            logProcessedEventStatement.setTimestamp(2, event.getStartedTime());
            logProcessedEventStatement.setTimestamp(3, event.getFinishedTime());
            Object[] keys = event.getKeys();
            for (int i = 0; i < keys.length; i++) {
                //if (keys[i].equals(AGGREGATION_PLACEHOLDER))
                //    logProcessedEventStatement.setNull(i + 4, Types.JAVA_OBJECT);
                //else
                    logProcessedEventStatement.setObject(i + 4, keys[i]);
            }
            logProcessedEventStatement.execute();
        } finally {
            if (null != logProcessedEventStatement)
                logProcessedEventStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void logFinishedEvent(FinishedEvent event) throws SQLException {
        Connection connection = null;
        PreparedStatement logFinishedEventStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            logFinishedEventStatement = connection.prepareStatement(createFinishedEventQuery());
            logFinishedEventStatement.setString(1, event.getUuid());
            logFinishedEventStatement.setTimestamp(2, event.getFinishedTime());
            logFinishedEventStatement.execute();
        } finally {
            if (null != logFinishedEventStatement)
                logFinishedEventStatement.close();
            if (null != connection)
                connection.close();
        }
    }
}
