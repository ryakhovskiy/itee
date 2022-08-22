package org.kr.intp.application.manager;

import org.kr.intp.application.AppContext;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.*;

/**
 * Created bykron 19.09.2014.
 */
public class DbRequestStatusManager {

    private static final IntpConfig CONFIG = AppContext.instance().getConfiguration();
    private static final String SCHEMA = CONFIG.getIntpSchema();
    private static final Logger log = LoggerFactory.getLogger(DbRequestStatusManager.class);

    private static final String FINISH_QUERY = String.format("update %s.RT_RR set RESPONSE = 'SUCCESS'", SCHEMA);
    private static final String ERROR_QUERY = String.format("update %s.RT_RR set RESPONSE = 'ERROR', STATUS = ?", SCHEMA);
    private static final String STATUS_QEURY = String.format("update %s.RT_RR set STATUS = ?", SCHEMA);
    private static final String RCOUNT_QUERY = String.format("select count(*) from %s.RT_RR", SCHEMA);

    public DbRequestStatusManager() { }

    protected void reportFinish() throws SQLException {
        Connection connection = null;
        PreparedStatement finishStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            finishStatement = connection.prepareStatement(FINISH_QUERY);
            int updated = finishStatement.executeUpdate();
            if (0 == updated)
                throw new SQLException("Cannot report FINISH: no data to update");
            awaitFinishing();
        } finally {
            closeStatement(finishStatement);
            closeConnection(connection);
        }
    }

    protected void reportError(String error) {
        Connection connection = null;
        PreparedStatement errorStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            errorStatement = connection.prepareStatement(ERROR_QUERY);
            errorStatement.setString(1, error);
            int updated = errorStatement.executeUpdate();
            if (0 == updated)
                throw new SQLException("Cannot report ERROR: " + error);
            awaitFinishing();
        } catch (SQLException e) {
            log.error("Error while reporting error: " + e.getMessage(), e);
        } finally {
            closeStatement(errorStatement);
            closeConnection(connection);
        }
    }

    private void awaitFinishing() throws SQLException {
        Connection connection = null;
        PreparedStatement rcountStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            rcountStatement = connection.prepareStatement(RCOUNT_QUERY);
            int count = 0;
            do {
                ResultSet resultSet = null;
                try {
                    resultSet = rcountStatement.executeQuery();
                    if (!resultSet.next())
                        continue;
                    count = resultSet.getInt(1);
                } finally {
                    if (null != resultSet)
                        resultSet.close();
                }
            } while (count > 0);
        } finally {
            closeStatement(rcountStatement);
            closeConnection(connection);
        }
    }

    protected void reportProgressStatus(String status) throws SQLException {
        log.debug(status);
        Connection connection = null;
        PreparedStatement statusStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statusStatement = connection.prepareStatement(STATUS_QEURY);
            statusStatement.setString(1, status);
            int updated = statusStatement.executeUpdate();
            if (0 == updated)
                throw new SQLException("Cannot report STATUS: " + status);
        } finally {
            closeStatement(statusStatement);
            closeConnection(connection);
        }
    }

    private void closeStatement(Statement statement) {
        if (null == statement)
            return;
        try {
            statement.close();
        } catch (SQLException e) {
            log.error("Error while closing resources: " + e.getMessage(), e);
        }
    }

    private void closeConnection(Connection connection) {
        if (null == connection)
            return;
        try {
            connection.close();
        } catch (SQLException e) {
            log.error("Error while closing resources: " + e.getMessage(), e);
        }
    }

    protected void close() {

    }

}
