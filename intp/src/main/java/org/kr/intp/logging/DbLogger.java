package org.kr.intp.logging;

import org.kr.intp.application.AppContext;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

/**
 * Created bykron 23.11.2016.
 */
public class DbLogger extends BaseLogger {

    private final org.slf4j.Logger slf4jLogger = org.slf4j.LoggerFactory.getLogger(DbLogger.class);
    private final boolean enabled;
    private final String schema = AppContext.instance().getConfiguration().getIntpSchema();
    private final String sql = "INSERT INTO " + schema + ".INTP_LOG VALUES (?,?,?,?,?)";

    public DbLogger(boolean enabled) {
        super(false);
        this.enabled = enabled;
    }

    @Override
    public void run() {
        slf4jLogger.info(getClass() + " enabled: " + enabled);
        if (!enabled)
            return;
        while (!Thread.currentThread().isInterrupted())
            log();
    }

    public void message(Level level, String name, String message) {
        if (!enabled)
            return;
        message(new LogMessage(level, name, message, Thread.currentThread().getName(), null));
    }

    public void message(Level level, String name, String format, Object arg) {
        if (!enabled)
            return;
        message(new LogMessage(level, name, String.format(format, arg), Thread.currentThread().getName(), null));
    }

    public void message(Level level, String name, String format, Object arg1, Object arg2) {
        if (!enabled)
            return;
        message(new LogMessage(level, name, String.format(format, arg1, arg2), Thread.currentThread().getName(), null));
    }

    public void message(Level level, String name, String format, Object... arguments) {
        if (!enabled)
            return;
        message(new LogMessage(level, name, String.format(format, arguments), Thread.currentThread().getName(), null));
    }

    public void message(Level level, String name, String message, Throwable t) {
        if (!enabled)
            return;
        message(new LogMessage(level, name, message, Thread.currentThread().getName(), t));
    }

    private void log() {
        try {
            final LogMessage message = getNextMessage();
            log(message);
        } catch (InterruptedException e) {
            slf4jLogger.info("DbLogger has been interrupted, not-logged-messages (queue size): " + queue.size());
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            slf4jLogger.error("Error while storing log-message to the database: " + e.getMessage(), e);
        }
    }

    private LogMessage getNextMessage() throws InterruptedException {
        LogMessage message = null;
        do {
            message = queue.poll(100L, TimeUnit.MILLISECONDS);
            if (slf4jLogger.isTraceEnabled())
                slf4jLogger.trace("message received: %b; current queue size: %d", null != message, queue.size());
        } while (null == message);
        return message;
    }

    private void log(LogMessage message) throws SQLException {
        if (!isLogLevelEnabled(message))
            return;

        try (Connection connection = ServiceConnectionPool.instance().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, new Timestamp(message.timestamp));
            statement.setString(2, message.thread);
            statement.setString(3, message.level.toString());
            final String msg = prepareMessageString(message);
            statement.setString(4, message.name);
            statement.setString(5, msg);
            statement.executeUpdate();
        } catch (SQLException e) {
            slf4jLogger.error("Database error: " + e.getMessage(), e);
        }
    }

    private boolean isLogLevelEnabled(LogMessage message) {
        final Level messageLevel = message.level;
        org.slf4j.Logger messageLogger = org.slf4j.LoggerFactory.getLogger(message.name);
        if (messageLogger.isTraceEnabled()) {
            return messageLevel.getId() >= Level.TRACE.getId();
        } else if (messageLogger.isDebugEnabled()) {
            return messageLevel.getId() >= Level.DEBUG.getId();
        } else if (messageLogger.isInfoEnabled()) {
            return messageLevel.getId() >= Level.INFO.getId();
        } else if (messageLogger.isErrorEnabled())
            return messageLevel.getId() >= Level.ERROR.getId();
        else
            return true;
    }

    private String prepareMessageString(LogMessage message) {
        if (null == message)
            return "";
        if (null == message.throwable)
            return message.message;
        final StackTraceElement[] stackTraceElements = message.throwable.getStackTrace();
        if (stackTraceElements == null || stackTraceElements.length == 0)
            return message.message;
        final StringBuilder builder = new StringBuilder();
        builder.append(message.message);
        int i = 0;
        StackTraceElement stackTraceElement;
        do {
            stackTraceElement = stackTraceElements[i++];
            builder.append(stackTraceElement.toString()).append("\n");
        } while (i < stackTraceElements.length);
        return builder.toString();
    }
}
