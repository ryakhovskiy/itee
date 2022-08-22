package org.kr.intp.client.db;

import org.kr.intp.IntpServerInfo;
import org.kr.intp.IntpTestBase;
import org.kr.intp.application.agent.IntpServer;
import org.kr.intp.application.pojo.job.*;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.config.IntpFileConfig;
import org.kr.intp.testutil.TestApplicationManager;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;

public class DatabaseRequestHandlerTest extends IntpTestBase {

    private final TestApplicationManager testApplicationManager = new TestApplicationManager();

    private final IntpConfig config = new IntpConfig(IntpFileConfig.getResourceInstance().getPropertiesMap());
    private final Logger log = LoggerFactory.getLogger(DatabaseRequestHandlerTest.class);
    private final String projectId = testApplicationManager.getProjectId();
    private final String SCHEMA = config.getIntpSchema();
    private final String WRITE_JSON_REQUEST_QUERY = "insert into " + SCHEMA + ".RT_RR (REQUEST) values (?)";
    private final String REQUEST_PROCESS_QUERY = "select top 1 RESPONSE, STATUS from " + SCHEMA + ".RT_RR";
    private Application testApp;
    private Closeable server;

    @Override
    protected void setUp() throws Exception {
        cleanUpDB();
        IntpServer server = new IntpServer(new IntpServerInfo("003", "int_dev", 'D', 4455, 10000), config);
        server.start();
        this.server = server;
        testApp = testApplicationManager.createApplication();
    }

    @Override
    protected void tearDown() throws InterruptedException, SQLException, IOException {
        cleanUpDB();
        server.close();
        ServiceConnectionPool.instance().close();
    }

    public void testStartAndActivate() throws SQLException, InterruptedException {
        if (null == testApp)
            return;

        activate(projectId, 0);

        Thread.sleep(3000);

        start(projectId);

        Thread.sleep(3000);
    }

    public void testFull() throws SQLException, InterruptedException {
        if (null == testApp)
            return;

        activate(projectId, 0);

        Thread.sleep(3000);

        start(projectId);

        Thread.sleep(3000);

        stop(projectId);

        Thread.sleep(3000);

        deactivate(projectId);

    }

    private void activate(String projectId, int version) throws SQLException, InterruptedException {
        log.debug("activating: " + projectId);
        final String json = createJson("activate", projectId, version);
        writeJsonRequest(json);
        observeRequestProcessing();
    }

    private void start(String projectId) throws SQLException, InterruptedException {
        log.debug("starting: " + projectId);
        final String json = createJson("start", projectId);
        writeJsonRequest(json);
        observeRequestProcessing();
    }

    private void stop(String projectId) throws SQLException, InterruptedException {
        log.debug("stopping: " + projectId);
        final String json = createJson("stop", projectId);
        writeJsonRequest(json);
        observeRequestProcessing();
    }

    private void deactivate(String projectId) throws SQLException, InterruptedException {
        log.debug("deactivating: " + projectId);
        final String json = createJson("deactivate", projectId);
        writeJsonRequest(json);
        observeRequestProcessing();
    }

    private String createJson(String command, String projectId) {
        return String.format("{\"command\":\"%s\", \"project_id\": \"%s\"}", command, projectId);
    }

    private String createJson(String command, String projectId, int version) {
        return String.format("{\"command\":\"%s\", \"project_id\": \"%s\", \"version\": %d}", command, projectId, version);
    }

    private void writeJsonRequest(final String json) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.prepareStatement(WRITE_JSON_REQUEST_QUERY);
            statement.setString(1, json);
            statement.executeUpdate();
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void observeRequestProcessing() throws SQLException, InterruptedException {
        RequestStatus status = null;
        do {
            status = getRequestStatus();
            log.info(status.toString());
            Thread.sleep(1000);
        } while (null == status.response || status.response.length() == 0);
    }

    private RequestStatus getRequestStatus() throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.prepareStatement(REQUEST_PROCESS_QUERY);
            resultSet = statement.executeQuery();
            if (!resultSet.next())
                throw new SQLException("No REQUEST lines found!");

            final String response = resultSet.getString(1);
            final String status = resultSet.getString(2);
            return new RequestStatus(response, status);
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void cleanUpDB() throws SQLException {
        testApplicationManager.removeAppFromDB();
        Connection connection = null;
        Statement statement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.createStatement();
            statement.executeUpdate("delete from " + config + ".RT_RR");
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private class RequestStatus {
        private final String response;
        private final String status;
        RequestStatus(String response, String status) {
            this.response = response;
            this.status = status;
        }
        public String toString() {
            return String.format("RESPONSE: [%s]; STATUS: [%s]", response, status);
        }
    }
}
