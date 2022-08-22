package org.kr.intp.application.manager;

import org.kr.intp.application.AppContext;
import org.kr.intp.application.agent.IServer;
import org.kr.intp.application.pojo.job.ApplicationJob;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

public class DbApplicationManager extends ApplicationManagerBase implements IProjectManager {

    private final Logger log = LoggerFactory.getLogger(DbApplicationManager.class);
    private final DbRequestStatusManager dbRequestStatusManager = new DbRequestStatusManager();
    private final IServer server = AppContext.instance().getServer();
    private final IntpConfig config = AppContext.instance().getConfiguration();

    public DbApplicationManager() {
        super();
    }

    public void init() {
        try {
            //start started applications
            final String[] projectIds = getStartedApplications();
            log.info("Jobs to be initialized: " + Arrays.toString(projectIds));
            for (String projectId : projectIds){
                if(!isInterruptedApplication(projectId)){
                    startApplication(projectId, false);
                }
            }
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * This method checks whether a project has been interrupted by an In-Time server shutdown.
     * @param projectId the project id.
     * @return <tt>true</tt> if the project has been interrupted.
     * @throws SQLException
     */
    private boolean isInterruptedApplication(String projectId) throws SQLException {
        String uuids = "";
        String interruptedQuery = String.format("SELECT UUID FROM \"%s\".\"%s_RUNTIME_INFO\" WHERE STATUS != 'DONE' " +
                "AND STATUS != 'ERROR' GROUP BY UUID;", config.getIntpGenObjectsSchema(), projectId);
        Connection connection = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            stmt = connection.createStatement();
            rs = stmt.executeQuery(interruptedQuery);
            if (rs.next()) {
                uuids += rs.getString(1);
            }
            while (rs.next()) {
                uuids += ", " + rs.getString(1);
            }
        } finally {
            if (null != rs)
                rs.close();
            if (null != stmt)
                stmt.close();
            if (null != connection)
                connection.close();
        }
        if (uuids.length() == 0) {
            return false;
        } else {
            setProjectInterrupted(projectId, uuids);
            return true;
        }
    }

    /**
     * This methods sets a given project's status to "ERROR". It should be called if the In-Time server has shutdown
     * while a project was running. The status message displays all non-finished UUIDs of the given project.
     * @param projectId the interrupted project.
     * @param uuids the uuids found in the run log table which have not been finished.
     * @throws SQLException
     */
    private void setProjectInterrupted(String projectId, String uuids) throws SQLException {
        String interruptedStatement = String.format("UPDATE \"%s\".\"RT_ACTIVE_PROJECTS\" SET STATUS = 2, " +
                "STATUS_MESSAGE = 'Project %s has been interrupted due to In-Time server shutdown. Please restart " +
                "the project. For further information see the Troubleshooting section in the In-Time Server User Guide. " +
                "Affected UUID(s): %s.' WHERE PROJECT_ID = '%s';",
                config.getIntpSchema(), projectId, uuids, projectId);

        Connection connection = null;
        Statement stmt = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            stmt = connection.createStatement();
            stmt.execute(interruptedStatement);
        } finally {
            if (null != stmt)
                stmt.close();
            if (null != connection)
                connection.close();
        }
    }

    public void start(String projectId) throws SQLException {
        log.debug("starting application: " + projectId);
        startApplication(projectId, true);
    }

    private void startApplication(final String projectId, final boolean reportProgress) throws SQLException {
        try {
            if (reportProgress)
                dbRequestStatusManager.reportProgressStatus("Starting application " + projectId);
            if (!server.startApplication(projectId))
                throw new Exception("Application already started: " + projectId);
            setApplicationStarted(projectId);
            if (reportProgress)
                dbRequestStatusManager.reportFinish();
            if (log.isTraceEnabled())
                log.trace("application started: " + projectId);
        } catch (Exception e) {
            log.error("Error while starting application [" + projectId + "]: " + e.getMessage(), e);
            if (reportProgress)
                dbRequestStatusManager.reportError(e.getMessage());
        }
    }

    public void stop(String projectId) throws SQLException {
        try {
            setApplicationStopped(projectId);
            dbRequestStatusManager.reportProgressStatus("Stopping application " + projectId);
            if (!server.stopApplication(projectId))
                throw new Exception("Application is not running: " + projectId);
            dbRequestStatusManager.reportFinish();
        } catch (Exception e) {
            log.error("Error while stopping project [" + projectId + "]: " + e.getMessage(), e);
            dbRequestStatusManager.reportError(e.getMessage());
        }
    }

    public void stopOnError(String projectId, String errMsg) {
        try {
            setApplicationFailed(projectId, errMsg);
            if (!server.stopApplication(projectId))
                throw new Exception("Application is not running: " + projectId);
        } catch (Exception e) {
            dbRequestStatusManager.reportError(e.getMessage());
        }
    }

    public void activate(String projectId, int version) throws Exception {
        char intpType = config.getIntpType();
        if(intpType == 'T' || intpType == 'P' || intpType == 'Q'){
            dbRequestStatusManager.reportError(
                    "An activation on T, P or Q system is not allowed!"
            );
            return;
        }

        dbRequestStatusManager.reportProgressStatus("Activating application " + projectId + "; v. " + version);
        boolean active = false;
        try {
            active = isApplicationActive(projectId, version);
        } catch (SQLException e) {
            dbRequestStatusManager.reportError(e.getMessage());
        }
        if (active) {
            dbRequestStatusManager.reportProgressStatus("Activating application; " + projectId + "; v. " + version + " is already active");
            dbRequestStatusManager.reportFinish();
            return;
        }

        final int currentActiveVersion = ApplicationReader.getInstance().getActiveApplicationVersion(projectId);
        if (currentActiveVersion == version) {
            dbRequestStatusManager.reportProgressStatus("Current version is already active: " + currentActiveVersion);
            dbRequestStatusManager.reportFinish();
            return;
        } else if (currentActiveVersion > 0) {
            dbRequestStatusManager.reportError(String.format("There is another active version of [%s] v. %d", projectId, currentActiveVersion));
            return;
        }
        try {
            final ApplicationJob[] jobs = ApplicationReader.getInstance().getApplication(projectId, version).getApplicationJobs();
            if (0 == jobs.length)
                throw new RuntimeException(String.format("No ApplicationJobs found for application [%s], version [%d], check data consistency",
                        projectId, version));
            setApplicationActive(projectId, version);
            dbRequestStatusManager.reportFinish();
        } catch (SQLException | CreateMetadataException e) {
            log.error(e.getMessage(), e);
            dbRequestStatusManager.reportError(e.getMessage());
        }
    }

    public void deactivate(String projectId) throws SQLException, IOException {
        char intpType = config.getIntpType();
        if(intpType == 'T' || intpType == 'P' || intpType == 'Q'){
            dbRequestStatusManager.reportError(
                    "An deactivation on T, P or Q system is not allowed!"
            );
            return;
        }

        dbRequestStatusManager.reportProgressStatus("Deactivating application " + projectId);
        int currentActiveVersion = getAppActiveVersion(projectId);
        if (currentActiveVersion < 0) {
            dbRequestStatusManager.reportError("There is no active applications: " + projectId);
            return;
        }

        final String[] startedApps = getStartedApplications();
        for (String startedId : startedApps) {
            if (startedId.equals(projectId)) {
                dbRequestStatusManager.reportError("Cannot deactivate started project! " + projectId);
                return;
            }
        }

        try {
            setApplicationInactive(projectId, currentActiveVersion);
            dbRequestStatusManager.reportFinish();
        } catch (SQLException | RemoveMetadataException e) {
            log.error(e.getMessage(), e);
            dbRequestStatusManager.reportError(e.getMessage());
        }
    }

}