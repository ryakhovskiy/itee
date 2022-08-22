package org.kr.intp.application.manager;

import org.kr.intp.application.AppContext;
import org.kr.intp.application.pojo.job.Application;
import org.kr.intp.application.pojo.job.ApplicationJob;
import org.kr.intp.application.pojo.job.JobTriggerType;
import org.kr.intp.application.pojo.job.MetadataGenerationMode;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.config.IntpFileConfig;
import org.kr.intp.testutil.TestApplicationManager;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

public class ApplicationMetadataManagerIntegrationTest {

    private static final String T_SCHEMA = "ERP_X";
    private static final IntpConfig CONFIG = new IntpConfig(IntpFileConfig.getResourceInstance().getPropertiesMap());

    private final TestApplicationManager testApplicationManager = new TestApplicationManager();
    private final String projectId = testApplicationManager.getProjectId();
    private final String schema = AppContext.instance().getConfiguration().getIntpSchema();
    private final String genSchema = AppContext.instance().getConfiguration().getIntpGenObjectsSchema();

    @BeforeClass
    public static void setUp() {
        AppContext.instance().setConfiguration(CONFIG);
    }

    @Test
    public void testGenerateMetadataEventApp() throws Exception {
        Application application = testApplicationManager.createApplication(JobTriggerType.EVENT, T_SCHEMA, MetadataGenerationMode.FULL);
        Connection connection = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            ApplicationMetadataManager.generateMetadata(connection, projectId, 0);
            assertApplicationMdGeneratedCorrectly(application, connection);
        } finally {
            if (null != connection) {
                ApplicationMetadataManager.removeMetadata(connection, projectId, 0);
                testApplicationManager.removeAppFromDB();
                connection.close();
            }
        }
    }

    @Test
    public void testGenerateMetadataScheduleApp() throws Exception {
        Application application = testApplicationManager.createApplication(JobTriggerType.SCHEDULE, T_SCHEMA,
                MetadataGenerationMode.FULL);
        Connection connection = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            ApplicationMetadataManager.generateMetadata(connection, projectId, 0);
            assertApplicationMdGeneratedCorrectly(application, connection);
        } finally {
            if (null != connection) {
                ApplicationMetadataManager.removeMetadata(connection, projectId, 0);
                testApplicationManager.removeAppFromDB();
                connection.close();
            }
        }
    }

    @Test
    private void assertApplicationMdGeneratedCorrectly(Application application, Connection connection) throws SQLException {
        assertNotNull(application);
        assertNotNull(connection);
        assertFalse(connection.isClosed());
        final String projectId = application.getProjectId();
        final int version = application.getVersion();
        final ApplicationJob[] jobs = application.getApplicationJobs();
        assertEquals(1, jobs.length);
        final ApplicationJob job = jobs[0];
        try (Statement statement = connection.createStatement()) {

            ResultSet resultSet = statement.executeQuery("select type, table_schema, proc_schema from " + schema +
                    ".RT_PROJECT_DEFINITIONS where project_id = '" + projectId + "' and version = " + version);
            assertTrue(resultSet.next());
            final String type = resultSet.getString(1);
            final String tableSchema = resultSet.getString(2);
            final String procSchema = resultSet.getString(3);

            assertEquals(type, job.getType().toString());
            assertEquals(tableSchema, job.getTableSchema());
            assertEquals(procSchema, job.getProcSchema());
            resultSet.close();

            //objects were correctly generated
            assertTablesGeneratedCorrectly(connection, job);
            assertProceduresGeneratedCorrectly(connection, job);
            assertViewsGeneratedCorrectly(connection, job);
            assertIndexesGeneratedCorrectly(connection, job);
            assertSequencesGeneratedCorrectly(connection, job);
            assertTriggersGeneratedCorrectly(connection, job);
        }
        assert true;
    }

    private void assertTriggersGeneratedCorrectly(Connection connection, ApplicationJob job) throws SQLException {
        if (job.getMetadataGenerationMode() == MetadataGenerationMode.SKIP_TRIGGERS)
            return;
        if (job.getType() == JobTriggerType.SCHEDULE)
            return;
        final String projectId = job.getProjectId();
        final String projectTable = job.getTableName();
        final String tableSchema = job.getTableSchema();

        final String[] actions = { "INSERT", "UPDATE", "DELETE" };
        for (String action : actions) {
            final String triggerName = String.format("%s_%s_%s_TRIGGER", projectId, projectTable, action);
            assertTriggerExists(connection, triggerName, tableSchema);
        }
    }

    private void assertSequencesGeneratedCorrectly(Connection connection, ApplicationJob job) throws SQLException {
        if (job.getMetadataGenerationMode() == MetadataGenerationMode.SKIP_TRIGGERS)
            return;
        if (job.getType() == JobTriggerType.SCHEDULE)
            return;
        final String projectId = job.getProjectId();
        final String projectTable = job.getTableName();

        final String sequenceName = String.format("%s_%s_SEQ", projectId, projectTable);
        assertSequenceExists(connection, sequenceName, genSchema);
    }

    private void assertIndexesGeneratedCorrectly(Connection connection, ApplicationJob job) throws SQLException {
        final String projectId = job.getProjectId();
        final String projectTable = job.getTableName();

        final String uuidInd = String.format("%s_%s_RUN_LOG_UUID_IND", projectId, projectTable);
        assertIndexExists(connection, uuidInd, genSchema);
        final String statInd = String.format("%s_%s_RUN_LOG_STATUS_IND", projectId, projectTable);
        assertIndexExists(connection, statInd, genSchema);
    }

    private void assertViewsGeneratedCorrectly(Connection connection, ApplicationJob job) throws SQLException {
        final String projectId = job.getProjectId();

        final String rtView = String.format("%s_RUNTIME_INFO", projectId);
        assertViewExists(connection, rtView, genSchema);
        final String lcView = String.format("%s_LIFECYCLE_LOG", projectId);
        assertViewExists(connection, lcView, genSchema);
    }



    private void assertProceduresGeneratedCorrectly(Connection connection, ApplicationJob job) throws SQLException {
        final String projectId = job.getProjectId();
        final String projectTable = job.getTableName();

        final String updateKeyStatisticsProcedure = String.format("%s_UPDATE_KEY_STATISTICS", projectId);
        assertProcedureExists(connection, updateKeyStatisticsProcedure, genSchema);
        assertProcedureExists(connection, job.getDeltaKeysProcedureName(), genSchema);
        assertProcedureExists(connection, job.getInitialKeysProcedureName(), genSchema);

        if (job.getMetadataGenerationMode() == MetadataGenerationMode.FULL && job.getType() == JobTriggerType.EVENT) {
            final String cleanUpTriggerLog = String.format("%s_CLEANUP_TRIGGER_LOG", projectId);
            assertProcedureExists(connection, cleanUpTriggerLog, genSchema);
        }

        if (job.getType() == JobTriggerType.EVENT) {
            final String restartErrors = String.format("%s_%s_RESTART_ERRORS", projectId, projectTable);
            assertProcedureExists(connection, restartErrors, genSchema);
        }
        
        final String archiveRunLog = String.format("%s_ARCHIVE_RUN_LOG", projectId);
        assertProcedureExists(connection, archiveRunLog, genSchema);
    }

    private void assertTablesGeneratedCorrectly(Connection connection, ApplicationJob job) throws SQLException {
        final String projectId = job.getProjectId();
        final String projectTable = job.getTableName();

        final String keyStatisticsTableName = String.format("%s_%s_KEY_STATISTICS", projectId, projectTable);
        assertTableExists(connection, keyStatisticsTableName, genSchema);
        final String runtimeArchiveTableName = String.format("%s_%s_RUNTIME_ARCHIVE", projectId, projectTable);
        assertTableExists(connection, runtimeArchiveTableName, genSchema);
        final String runLogTableName = String.format("%s_%s_RUN_LOG", projectId, projectTable);
        assertTableExists(connection, runLogTableName, genSchema);
        if (job.getMetadataGenerationMode() == MetadataGenerationMode.FULL && job.getType() == JobTriggerType.EVENT) {
            final String triggerTableName = String.format("%s_%s_TRIGGER_LOG", projectId, projectTable);
            assertTableExists(connection, triggerTableName, genSchema);
        }
    }

    private void assertTableExists(Connection connection, String tableName, String schema) throws SQLException {
        final String sql = "select * from tables where table_name = ? and schema_name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, tableName);
            statement.setString(2, schema);
            try (ResultSet set = statement.executeQuery()) {
                assertTrue("Table not found: " + schema + "." + tableName, set.next());
            }
        }
    }

    private void assertProcedureExists(Connection connection, String procedureName, String schema) throws SQLException {
        final String sql = "select * from procedures where procedure_name = ? and schema_name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, procedureName);
            statement.setString(2, schema);
            try (ResultSet set = statement.executeQuery()) {
                assertTrue("Procedure not found: " + schema + "." + procedureName, set.next());
            }
        }
    }

    private void assertViewExists(Connection connection, String viewName, String schema) throws SQLException {
        final String sql = "select * from views where view_name = ? and schema_name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, viewName);
            statement.setString(2, schema);
            try (ResultSet set = statement.executeQuery()) {
                assertTrue("View not found: " + schema + "." + viewName, set.next());
            }
        }
    }

    private void assertIndexExists(Connection connection, String indexName, String schema) throws SQLException {
        final String sql = "select * from indexes where index_name = ? and schema_name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, indexName);
            statement.setString(2, schema);
            try (ResultSet set = statement.executeQuery()) {
                assertTrue("Index not found: " + schema + "." + indexName, set.next());
            }
        }
    }

    private void assertSequenceExists(Connection connection, String sequenceName, String schema) throws SQLException {
        final String sql = "select * from sequences where sequence_name = ? and schema_name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, sequenceName);
            statement.setString(2, schema);
            try (ResultSet set = statement.executeQuery()) {
                assertTrue("Sequence not found: " + schema + "." + sequenceName, set.next());
            }
        }
    }

    private void assertTriggerExists(Connection connection, String triggerName, String schema) throws SQLException {
        final String sql = "select * from triggers where trigger_name = ? and schema_name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, triggerName);
            statement.setString(2, schema);
            try (ResultSet set = statement.executeQuery()) {
                assertTrue("Trigger not found: " + schema + "." + triggerName, set.next());
            }
        }
    }

}