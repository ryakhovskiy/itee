package org.kr.intp.application.job.lifecycle;

import org.kr.intp.application.AppContext;
import org.kr.intp.application.pojo.event.EventType;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.TimeController;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by kr on 30.01.14.
 */
public class EventLogger implements Runnable {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    public static EventLogger newInstnce(String projectId, String name) throws SQLException {
        return new EventLogger(projectId, name);
    }

    private final Logger log = LoggerFactory.getLogger(EventLogger.class);
    private final BlockingQueue<Event> queue = new LinkedBlockingQueue<Event>();
    private final String projectId;
    private final String name;
    private final LogWriter logWriter;

    private EventLogger(String projectId, String name) throws SQLException {
        this.projectId = projectId == null ? "NONE" : projectId;
        this.name = name;
        this.logWriter = new LogWriter();
    }

    public void logTriggered(String uuid) {
        long time = TimeController.getInstance().getServerUtcTimeMillis();
        Event event = new TriggeredEvent(uuid, time);
        try {
            queue.put(event);
        } catch (InterruptedException e) {
            log.debug("interrupted");
            Thread.currentThread().interrupt();
        }
    }

    public void logStarted(String uuid) {
        long time = TimeController.getInstance().getServerUtcTimeMillis();
        Event event = new StartedEvent(uuid, time);
        try {
            queue.put(event);
        } catch (InterruptedException e) {
            log.debug("interrupted");
            Thread.currentThread().interrupt();
        }
    }

    public void logFinished(String uuid) {
        long time = TimeController.getInstance().getServerUtcTimeMillis();
        Event event = new FinishedEvent(uuid, time);
        try {
            queue.put(event);
        } catch (InterruptedException e) {
            log.debug("interrupted");
            Thread.currentThread().interrupt();
        }
    }

    public void logError(String uuid, Throwable e) {
        long time = TimeController.getInstance().getServerUtcTimeMillis();
        Event event = new ErrorEvent(uuid, time, e);
        try {
            queue.put(event);
        } catch (InterruptedException ie) {
            log.debug("interrupted");
            Thread.currentThread().interrupt();
        }
    }

    public void run() {
        while (!Thread.currentThread().isInterrupted())
            log();
    }

    private void log() {
        try {
            Event event = queue.take();
            log(event);
        } catch (InterruptedException e) {
            log.debug("interrupted");
            Thread.currentThread().interrupt();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }

    private void log(Event event) throws SQLException {
        EventType type = event.getType();
        switch (type) {
            case ERROR:
                logWriter.logError((ErrorEvent) event);
                break;
            case TRIGGERED:
                logWriter.logTriggered((TriggeredEvent) event);
                break;
            case STARTED:
                logWriter.logStarted((StartedEvent) event);
                break;
            case FINISHED:
                logWriter.logFinished((FinishedEvent) event);
                break;
            default:
                log.error("WRONG EVENT TYPE: " + event.getType());
                break;
        }
    }

    private abstract class Event {

        private final String uuid;

        private final long time;

        private Event(String uuid, long time) {
            this.uuid = uuid;
            this.time = time;
        }

        public String getUuid() { return uuid; }

        public long getTime() {
            return time;
        }

        public abstract EventType getType();
    }

    private class TriggeredEvent extends Event {

        private TriggeredEvent(String uuid, long time) {
            super(uuid, time);
        }

        @Override
        public EventType getType() {
            return EventType.TRIGGERED;
        }
    }

    private class StartedEvent extends Event {

        private StartedEvent(String uuid, long time) {
            super(uuid, time);
        }

        @Override
        public EventType getType() {
            return EventType.STARTED;
        }
    }

    private class FinishedEvent extends Event {

        private FinishedEvent(String uuid, long time) {
            super(uuid, time);
        }

        @Override
        public EventType getType() {
            return EventType.FINISHED;
        }
    }

    private class ErrorEvent extends Event {

        private final Throwable e;

        private ErrorEvent(String uuid, long time, Throwable e) {
            super(uuid, time);
            this.e = e;
        }

        @Override
        public EventType getType() {
            return EventType.ERROR;
        }

        public String getErrorMessage() {
            StringBuilder sb = new StringBuilder();
            Throwable error = e;
            while (null != error) {
                sb.append(error.getMessage());
                sb.append(LINE_SEPARATOR);
                for (StackTraceElement stackTraceElement : error.getStackTrace()) {
                    sb.append(stackTraceElement.toString());
                    sb.append(LINE_SEPARATOR);
                }
                sb.append(LINE_SEPARATOR);
                error = error.getCause();
            }
            return sb.toString();
        }
    }

    private class LogWriter {
        private final String schema = AppContext.instance().getConfiguration().getIntpSchema();
        private LogWriter() { }

        private void logError(ErrorEvent event) throws SQLException {
            int index = 0;
            Connection connection = null;
            PreparedStatement errorStatement = null;
            try {
                connection = ServiceConnectionPool.instance().getConnection();
                errorStatement = connection.prepareStatement(String.format("insert into %s.RT_LIFECYCLE_LOG (UUID, PROJECT_ID, UPDATE_TS, NAME, STATUS, COMMENT) values (?, ?, ?, ?, ?, ?)", schema));
                errorStatement.setString(++index, event.getUuid());
                errorStatement.setString(++index, projectId);
                errorStatement.setTimestamp(++index, new Timestamp(event.getTime()));
                errorStatement.setString(++index, name);
                errorStatement.setInt(++index, event.getType().getId());
                errorStatement.setString(++index, event.getErrorMessage());
                errorStatement.execute();
            } finally {
                if (null != errorStatement)
                    errorStatement.close();
                if (null != connection)
                    connection.close();
            }
        }

        private void logTriggered(TriggeredEvent event) throws SQLException {
            int index = 0;
            Connection connection = null;
            PreparedStatement triggeredStatement = null;
            try {
                connection = ServiceConnectionPool.instance().getConnection();
                triggeredStatement = connection.prepareStatement(String.format("insert into %s.RT_LIFECYCLE_LOG (UUID, PROJECT_ID, UPDATE_TS, NAME, STATUS) values (?, ?, ?, ?, ?)", schema));
                triggeredStatement.setString(++index, event.getUuid());
                triggeredStatement.setString(++index, projectId);
                triggeredStatement.setTimestamp(++index, new Timestamp(event.getTime()));
                triggeredStatement.setString(++index, name);
                triggeredStatement.setInt(++index, event.getType().getId());
                triggeredStatement.execute();
            } finally {
                if (null != triggeredStatement)
                    triggeredStatement.close();
                if (null != connection)
                    connection.close();
            }
        }

        private void logStarted(StartedEvent event) throws SQLException {
            int index = 0;
            Connection connection = null;
            PreparedStatement startedStatement = null;
            try {
                connection = ServiceConnectionPool.instance().getConnection();
                startedStatement = connection.prepareStatement(String.format("insert into %s.RT_LIFECYCLE_LOG (UUID, PROJECT_ID, UPDATE_TS, NAME, STATUS) values (?, ?, ?, ?, ?)", schema));
                startedStatement.setString(++index, event.getUuid());
                startedStatement.setString(++index, projectId);
                startedStatement.setTimestamp(++index, new Timestamp(event.getTime()));
                startedStatement.setString(++index, name);
                startedStatement.setInt(++index, event.getType().getId());
                startedStatement.execute();
            } finally {
                if (null != startedStatement)
                    startedStatement.close();
                if (null != connection)
                    connection.close();
            }
        }

        private void logFinished(FinishedEvent event) throws SQLException {
            Connection connection = null;
            PreparedStatement finishedStatement = null;
            try {
                int index = 0;
                connection = ServiceConnectionPool.instance().getConnection();
                finishedStatement = connection.prepareStatement(String.format("insert into %s.RT_LIFECYCLE_LOG (UUID, PROJECT_ID, UPDATE_TS, NAME, STATUS) values (?, ?, ?, ?, ?)", schema));
                finishedStatement.setString(++index, event.getUuid());
                finishedStatement.setString(++index, projectId);
                finishedStatement.setTimestamp(++index, new Timestamp(event.getTime()));
                finishedStatement.setString(++index, name);
                finishedStatement.setInt(++index, event.getType().getId());
                finishedStatement.execute();
            } finally {
                if (null != finishedStatement)
                    finishedStatement.close();
                if (null != connection)
                    connection.close();
            }
        }
    }
}
