package org.kr.intp.application.manager;

import org.kr.intp.application.AppContext;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public abstract class ApplicationManagerBase {

    protected static final int APPLICATION_ACTIVATION_STATUS_INACTIVE = 0;
    protected static final int APPLICATION_ACTIVATION_STATUS_ACTIVE = 1;

    protected static final int APPLICATION_RUNNING_STATUS_STOPPED = 0;
    protected static final int APPLICATION_RUNNING_STATUS_STARTED = 1;
    protected static final int APPLICATION_RUNNING_STATUS_FAILED = 2;

    private static final String INTP_SCHEMA = AppContext.instance().getConfiguration().getIntpSchema();

    private static final String IS_ACTIVE_QUERY = String.format("select STATUS from %s.RT_PROJECT_DEFINITIONS where PROJECT_ID = ? AND VERSION = ?", INTP_SCHEMA);
    private static final String APP_ACTIVATION_STATUS_QUERY = String.format("update %s.RT_PROJECT_DEFINITIONS set STATUS = ?, ACTIVATION_DATE = CURRENT_UTCTIMESTAMP where PROJECT_ID = ? and version = ?", INTP_SCHEMA);
    private static final String COPY_INACTIVE_APP_QUERY = String.format("insert into %s.RT_ACTIVE_PROJECTS (PROJECT_ID, VERSION, STATUS, ACTION, STATUS_MESSAGE) select top 1 PROJECT_ID, VERSION, 0, '', 'Stopped' from %s.RT_PROJECT_DEFINITIONS where PROJECT_ID = ? and version = ? and STATUS = 0 ", INTP_SCHEMA, INTP_SCHEMA);
    private static final String APP_RUNNING_STATUS_QUERY = String.format("update %s.RT_ACTIVE_PROJECTS set STATUS = ?, STATUS_MESSAGE = ?, ACTION = '' where PROJECT_ID = ?", INTP_SCHEMA);
    private static final String REMOVE_ACTIVE_APP_QUERY = String.format("delete from %s.RT_ACTIVE_PROJECTS where PROJECT_ID = ?", INTP_SCHEMA);
    private static final String GET_STARTED_APPS_QUERY = String.format("select PROJECT_ID from %s.RT_ACTIVE_PROJECTS where status = 1", INTP_SCHEMA);
    private static final String GET_ACTIVE_VERSION_QUERY = String.format("select VERSION from %s.RT_PROJECT_DEFINITIONS where PROJECT_ID = ? and status = 1", INTP_SCHEMA);

    private final Logger log = LoggerFactory.getLogger(ApplicationManagerBase.class);

    protected ApplicationManagerBase() { }

    protected boolean isApplicationActive(String projectId, int version) throws SQLException {
        Connection connection = null;
        PreparedStatement isActiveStatement = null;
        ResultSet set = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            isActiveStatement = connection.prepareStatement(IS_ACTIVE_QUERY);
            isActiveStatement.setString(1, projectId);
            isActiveStatement.setInt(2, version);
            set = isActiveStatement.executeQuery();
            if (!set.next()) {
                final String error = String.format("Project [%s] v. %d does not exists", projectId, version);
                throw new SQLException(error);
            }
            return set.getInt(1) == 1;
        } finally {
            if (null != set)
                set.close();
            if (null != isActiveStatement)
                isActiveStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    protected void setApplicationActive(String projectId, int version)
            throws IOException, SQLException, CreateMetadataException {
        Connection connection = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            connection.setAutoCommit(false);
            ApplicationMetadataManager.generateMetadata(connection, projectId, version);
            copyApplication(projectId, version);
            setApplicationActivationStatus(projectId, version, APPLICATION_ACTIVATION_STATUS_ACTIVE);
            connection.commit();
        } catch (Exception e) {
            if (null != connection)
                connection.rollback();
            throw e;
        } finally {
            if (null != connection)
                connection.close();
        }
    }

    protected void setApplicationInactive(String projectId, int version)
            throws SQLException, IOException, RemoveMetadataException {
        Connection connection = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            connection.setAutoCommit(false);
            setApplicationActivationStatus(projectId, version, APPLICATION_ACTIVATION_STATUS_INACTIVE);
            removeActiveApplication(projectId);
            ApplicationMetadataManager.removeMetadata(connection, projectId, version);
            connection.commit();
        } catch (Exception e) {
            if (null != connection)
                connection.rollback();
            throw e;
        } finally {
            if (null != connection)
                connection.close();
        }
    }

    private void setApplicationActivationStatus(String projectId, int version, int status) throws SQLException {
        Connection connection = null;
        PreparedStatement appActivationStatusStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            appActivationStatusStatement = connection.prepareStatement(APP_ACTIVATION_STATUS_QUERY);
            appActivationStatusStatement.setInt(1, status);
            appActivationStatusStatement.setString(2, projectId);
            appActivationStatusStatement.setInt(3, version);
            int updated = appActivationStatusStatement.executeUpdate();
            if (0 == updated)
                log.error("appActivationStatusStatement - update affected 0 records");
        } finally {
            if (null != appActivationStatusStatement)
                appActivationStatusStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    protected void setApplicationStarted(String projectId) throws SQLException {
        setApplicationRunningStatus(projectId, APPLICATION_RUNNING_STATUS_STARTED, "Running");
    }

    protected void setApplicationStopped(String projectId) throws SQLException {
        setApplicationRunningStatus(projectId, APPLICATION_RUNNING_STATUS_STOPPED, "Stopped");
    }

    protected void setApplicationFailed(String projectId, String statusMessage) throws SQLException {
        setApplicationRunningStatus(projectId, APPLICATION_RUNNING_STATUS_FAILED, statusMessage);
    }

    protected String[] getStartedApplications() throws SQLException {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery(GET_STARTED_APPS_QUERY);
            final List<String> projectIds = new ArrayList<String>();
            while (resultSet.next())
                projectIds.add(resultSet.getString(1));
            return projectIds.toArray(new String[projectIds.size()]);
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    protected int getAppActiveVersion(String projectId) throws SQLException {
        Connection connection = null;
        PreparedStatement getActiveVersionStatement = null;
        ResultSet resultSet = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            getActiveVersionStatement = connection.prepareStatement(GET_ACTIVE_VERSION_QUERY);
            getActiveVersionStatement.setString(1, projectId);
            resultSet = getActiveVersionStatement.executeQuery();
            if (!resultSet.next())
                return Integer.MIN_VALUE;
            return resultSet.getInt(1);
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != getActiveVersionStatement)
                getActiveVersionStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void setApplicationRunningStatus(String projectId, int status, String statusMessage) throws SQLException {
        Connection connection = null;
        PreparedStatement appRunningStatusStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            appRunningStatusStatement = connection.prepareStatement(APP_RUNNING_STATUS_QUERY);
            appRunningStatusStatement.setInt(1, status);
            appRunningStatusStatement.setString(2, statusMessage);
            appRunningStatusStatement.setString(3, projectId);
            int updated = appRunningStatusStatement.executeUpdate();
            if (0 == updated)
                log.error("appRunningStatusStatement - update affected 0 records");
        } finally {
            if (null != appRunningStatusStatement)
                appRunningStatusStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void copyApplication(String projectId, int version) throws SQLException {
        Connection connection = null;
        PreparedStatement copyInactiveAppStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            copyInactiveAppStatement = connection.prepareStatement(COPY_INACTIVE_APP_QUERY);
            copyInactiveAppStatement.setString(1, projectId);
            copyInactiveAppStatement.setInt(2, version);
            int updated = copyInactiveAppStatement.executeUpdate();
            if (0 == updated)
                log.error("copyInactiveAppStatement - update affected 0 records");
        } finally {
            if (null != copyInactiveAppStatement)
                copyInactiveAppStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void removeActiveApplication(String projectId) throws SQLException {
        Connection connection = null;
        PreparedStatement removeActiveAppStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            removeActiveAppStatement = connection.prepareStatement(REMOVE_ACTIVE_APP_QUERY);
            removeActiveAppStatement.setString(1, projectId);
            int updated = removeActiveAppStatement.executeUpdate();
            if (0 == updated) {
                log.error("removeActiveAppStatement - update affected 0 records");
            }
        } finally {
            if (null != removeActiveAppStatement)
                removeActiveAppStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    protected void close() {

    }
}
