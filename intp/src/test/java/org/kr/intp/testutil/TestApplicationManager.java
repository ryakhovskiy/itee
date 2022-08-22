package org.kr.intp.testutil;

import org.kr.intp.application.pojo.job.*;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.config.IntpFileConfig;
import org.kr.intp.util.db.TimeController;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kr on 7/13/2014.
 */
public class TestApplicationManager {

    private static final IntpConfig CONFIG = new IntpConfig(IntpFileConfig.getResourceInstance().getPropertiesMap());
    private static final String SCHEMA = CONFIG.getIntpSchema();
    private static final ObjectMapper mapper = new ObjectMapper();

    private final String projectId = "AUTOTST" + TimeController.getInstance().getServerUtcTimeMillis();

    public TestApplicationManager() { }

    public String getProjectId() {
        return projectId;
    }

    public Application createApplication(JobTriggerType type, String schema, MetadataGenerationMode mdGenMode) throws SQLException, IOException {
        final Project project = new Project(projectId, type.toString(), 0, "APP", "DESC", schema, "SOURCE_IT", mdGenMode);
        final ProjectTable[] tables = { new ProjectTable(projectId, "S534", 1) };
        final ProjectTableKey[] keys = {
                new ProjectTableKey(projectId, 1, 1, "S534", "SPMON", ProjectKeyType.STRING, "NVARCHAR (16)", 0, true, false, false),
                new ProjectTableKey(projectId, 2, 1, "S534", "WERKS", ProjectKeyType.STRING, "NVARCHAR (16)", 0, true, false, false),
                new ProjectTableKey(projectId, 3, 1, "S534", "MATNR", ProjectKeyType.STRING, "NVARCHAR (18)", 10, false, true, false)
        };
        final ProjectProcedure[] procedures = {
                new ProjectProcedure(projectId, 1, 'D', "INTP_STOCK_VAL_DELTA"),
                new ProjectProcedure(projectId, 1, 'I', "INTP_STOCK_VAL_INITIAL")
        };
        return writeAppToDB(project, tables, keys, procedures);
    }

    public Application createApplication() throws SQLException, IOException {
        return createApplication(JobTriggerType.EVENT, "KR_SOURCE_ERP_SLT", MetadataGenerationMode.FULL);
    }



    private Application writeAppToDB(Project project, ProjectTable[] tables, ProjectTableKey[] keys,
                                     ProjectProcedure[] procedures) throws SQLException, IOException {

        ServiceConnectionPool cm = ServiceConnectionPool.instance();
        Connection connection = null;
        Statement statement = null;
        try {
            connection = cm.getConnection();
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            createProjectProperties(statement, projectId);
            for (ProjectTable t : tables) {
                final String query = String.format("insert into %s.RT_PROJECT_TABLES (PROJECT_ID, number, table_name, version) values ('%s', 1, '%s', 0)", SCHEMA, projectId, t.getName());
                statement.executeUpdate(query);
            }
            for (ProjectProcedure p : procedures) {
                final String query = String.format("insert into %s.RT_PROJECT_PROCEDURES values ('%s', 1, '%s', '%s', 0, 'SP', 'HD')", SCHEMA, projectId, p.getType(), p.getName());
                statement.executeUpdate(query);
            }

            for (ProjectTableKey k : keys) {
                statement.executeUpdate(String.format("insert into %s.RT_PROJECT_KEYS values ('%s', 1, %d, 0, '%s', '%s', '%s', '%s', %d, 'X', NULL)", SCHEMA,
                        projectId, k.getNumber(), k.getKeyName(), k.getKeyDbType(), k.isInitial() ? "X" : "", k.isSummable() ? "X" : "", k.getThreshold()));
            }
            statement.executeUpdate(String.format("insert into %s.RT_PROJECT_DEFINITIONS values ('%s', 0, '%s', 'APP', 'DESC', CURRENT_UTCTIMESTAMP, CURRENT_UTCTIMESTAMP, NULL, 0, '%s', '%s', NULL, 5, 0, " + project.getMetadataGenerationMode().getMode() + ")",
                    SCHEMA, projectId, project.getType().toString(), project.getTableSchema(), project.getProcSchema()));
            connection.setAutoCommit(true);
            return new Application(project, tables, keys, procedures, null);
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void createProjectProperties(Statement statement, String projectId) throws SQLException, IOException {
        Map params = new HashMap();
        params.put("sla", 20);
        Map initial = new HashMap();
        initial.put("startdate", "2014-07-09 12:00:00");
        initial.put("enddate", "2014-12-09 12:00:00");
        initial.put("period", 1000);
        initial.put("executors", 10);
        initial.put("fullload", true);
        initial.put("flrestrictions", "spmon = '201212'");
        params.put("initial", initial);
        Map delta = new HashMap();
        delta.put("startdate", "2014-07-09 12:01:00");
        delta.put("enddate", "2014-12-09 12:00:00");
        delta.put("period", 10);
        delta.put("executors", 10);
        params.put("delta", delta);
        final String sparams = mapper.writeValueAsString(params);
        final String sql = String.format("insert into %s.RT_PARAMETERS values ('%s', 0, current_utctimestamp, '%s')",
                SCHEMA, projectId, sparams.replace("'", "''"));
        statement.executeUpdate(sql);
    }

    public void removeAppFromDB() throws SQLException {
        ServiceConnectionPool cm = ServiceConnectionPool.instance();
        Connection connection = null;
        Statement statement = null;
        try {
            connection = cm.getConnection();
            statement = connection.createStatement();
            statement.executeUpdate(String.format("delete from %s.RT_ACTIVE_PROJECTS where PROJECT_ID like 'AUTOTST%%'", SCHEMA));
            statement.executeUpdate(String.format("delete from %s.RT_PARAMETERS where PROJECT_ID like 'AUTOTST%%'", SCHEMA));
            statement.executeUpdate(String.format("delete from %s.RT_PROJECT_DEFINITIONS where PROJECT_ID like 'AUTOTST%%'", SCHEMA));
            statement.executeUpdate(String.format("delete from %s.RT_PROJECT_KEYS where PROJECT_ID like 'AUTOTST%%'", SCHEMA));
            statement.executeUpdate(String.format("delete from %s.RT_PROJECT_PROCEDURES where PROJECT_ID like 'AUTOTST%%'", SCHEMA));
            statement.executeUpdate(String.format("delete from %s.RT_PROJECT_TABLES where PROJECT_ID like 'AUTOTST%%'", SCHEMA));
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

}
