package org.kr.intp.application.job;

import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class InitialJobDBController {

    private static final String UPDATE_SQL_TEMPLATE =
            "update %s.RT_ACTIVE_PROJECTS set IS_PROJECT_INITIALIZED = ? where PROJECT_ID = ?";
    private static final String SELECT_SQL_TEMPLATE =
            "select IS_PROJECT_INITIALIZED from %s.RT_ACTIVE_PROJECTS where PROJECT_ID = ?";

    private final Logger log = LoggerFactory.getLogger(InitialJobDBController.class);
    private final boolean isTraceEnabled = log.isTraceEnabled();
    private final String updateSql;
    private final String selectSql;

    public InitialJobDBController(String serverSchema) {
        this.updateSql = String.format(UPDATE_SQL_TEMPLATE, serverSchema);
        this.selectSql = String.format(SELECT_SQL_TEMPLATE, serverSchema);
    }

    public boolean notifyStarted(String projectId) {
        try (Connection connection = ServiceConnectionPool.instance().getConnection();
             PreparedStatement statement = connection.prepareStatement(updateSql)) {
            statement.setBoolean(1, false);
            statement.setString(2, projectId);
            final int rowsAffected = statement.executeUpdate();
            if (isTraceEnabled)
                log.trace("notifying started project [" + projectId + "]; rows affected: " + rowsAffected);
            return rowsAffected > 0;
        } catch (SQLException e) {
            log.error("Cannot notify server that job has been started: " + e.getMessage(), e);
            return false;
        }
    }

    public boolean notifyFinished(String projectId) {
        try (Connection connection = ServiceConnectionPool.instance().getConnection();
             PreparedStatement statement = connection.prepareStatement(updateSql)) {
            statement.setBoolean(1, true);
            statement.setString(2, projectId);
            final int rowsAffected = statement.executeUpdate();
            if (isTraceEnabled)
                log.trace("notifying finished project [" + projectId + "]; rows affected: " + rowsAffected);            return rowsAffected > 0;
        } catch (SQLException e) {
            log.error("Cannot notify server that job has been started: " + e.getMessage(), e);
            return false;
        }
    }

    public boolean isProjectInitialized(String projectId) {
        try (Connection connection = ServiceConnectionPool.instance().getConnection();
             PreparedStatement statement = connection.prepareStatement(selectSql)) {
            statement.setString(1, projectId);
            ResultSet resultSet = statement.executeQuery();
            if (!resultSet.next()) {
                log.warn("no rows found for project [" + projectId + "]");
                return false;
            }
            final boolean initialized = resultSet.getBoolean(1);
            if (isTraceEnabled)
                log.trace("project [" + projectId + "]; is initialized: " + initialized);
            return initialized;
        } catch (SQLException e) {
            log.error("Cannot notify server that job has been started: " + e.getMessage(), e);
            return false;
        }
    }
}
