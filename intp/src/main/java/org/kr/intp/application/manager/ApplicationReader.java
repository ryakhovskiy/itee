package org.kr.intp.application.manager;

import org.kr.intp.application.AppContext;
import org.kr.intp.application.pojo.job.*;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ApplicationReader {

    private static final IntpConfig CONFIG = AppContext.instance().getConfiguration();
    private static final String SCHEMA = CONFIG.getIntpSchema();

    private static final String PROJECT_QUERY = String.format(
            "select type, application, description, TABLE_SCHEMA, PROC_SCHEMA, GEN_OPTION " +
                    "from %s.RT_PROJECT_DEFINITIONS " +
                    "where PROJECT_ID = ? and version = ?", SCHEMA);

    private static final String PROJECT_TABLES_QUERY = String.format(
            "select TABLE_NAME, NUMBER " +
                    "from %s.RT_PROJECT_TABLES " +
                    "where PROJECT_ID = ? and version = ?", SCHEMA);

    private static final String PROJECT_TABLE_KEYS_QUERY = String.format(
            "select k.NUMBER, t.NUMBER, t.TABLE_NAME, k.KEY_NAME, k.KEY_DATATYPE, k.KEY_THRESHOLD, " +
                    "k.KEY_INITIAL, k.KEY_SUMMABLE, k.KEY_SEQUENTIAL " +
                    "from %s.RT_PROJECT_TABLES t " +
                    "inner join %s.RT_PROJECT_KEYS k on t.PROJECT_ID = k.PROJECT_ID " +
                    "and t.VERSION = k.VERSION and t.NUMBER = k.TABLE_NUMBER " +
                    "where t.PROJECT_ID = ? and t.VERSION = ?", SCHEMA, SCHEMA);

    private static final String PROJECT_PROCEDURES_QUERY = String.format(
            "select NUMBER, TYPE, PROCEDURE_NAME " +
                    "from %s.RT_PROJECT_PROCEDURES " +
                    "where PROJECT_ID = ? and version = ?", SCHEMA);

    private static final String APP_ACTIVATION_STATUS_QUERY = String.format(
            "select version from %s.RT_PROJECT_DEFINITIONS where PROJECT_ID = ? and status = 1", SCHEMA);

    private static final String PROJECT_PRIORITY_QUERY = String.format(
            "SELECT WORKLOAD_PRIORITY FROM %s.\"RT_PROJECT_PRIORITY\" WHERE PROJECT_ID = ? AND PROJECT_VERSION = ?", SCHEMA
    );

    private static final ApplicationReader INSTANCE = new ApplicationReader();

    public static ApplicationReader getInstance() {
        return INSTANCE;
    }

    private final Logger log = LoggerFactory.getLogger(ApplicationReader.class);

    private ApplicationReader() {
        log.debug("ApplicationReader initialized");
    }

    Application getApplication(String projectId, int version) throws SQLException, IOException {
        if (log.isDebugEnabled())
            log.debug("searching application, projectId: " + projectId + "; version: " + version);
        Connection connection = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            final Project project = getProject(projectId, version);
            final ProjectTable[] tables = getProjectTables(projectId, version);
            final ProjectTableKey[] keys = getProjectTableKeys(projectId, version);
            final ProjectProcedure[] procs = getProjectProcedures(projectId, version);
            final Properties clientInfo = getWorkloadPriority(projectId, version);
            final Application app = new Application(project, tables, keys, procs, clientInfo);
            if (log.isTraceEnabled())
                log.trace("application found: " + app);
            return app;
        } finally {
            if (null != connection)
                connection.close();
        }
    }

    public Application getActiveApplication(String projectId) throws SQLException, IOException {
        final int version = getActiveApplicationVersion(projectId);
        if (log.isDebugEnabled())
            log.debug("searching active application, projectId: " + projectId + "; version: " + version);
        Connection connection = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            final Project project = getProject(projectId, version);
            final ProjectTable[] tables = getProjectTables(projectId, version);
            final ProjectTableKey[] keys = getProjectTableKeys(projectId, version);
            final ProjectProcedure[] procs = getProjectProcedures(projectId, version);
            final Properties clientInfo = getWorkloadPriority(projectId, version);
            final Application app = new Application(project, tables, keys, procs, clientInfo);
            if (log.isTraceEnabled())
                log.trace("application found: " + app);
            return app;
        } finally {
            if (null != connection)
                connection.close();
        }
    }

    public int getActiveApplicationVersion(String projectId) throws SQLException {
        Connection connection = null;
        PreparedStatement appActiveVersionStatement = null;
        ResultSet set = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            appActiveVersionStatement = connection.prepareStatement(APP_ACTIVATION_STATUS_QUERY);
            appActiveVersionStatement.setString(1, projectId);
            set = appActiveVersionStatement.executeQuery();
            if (!set.next())
                return -1;
            return set.getInt(1);
        } finally {
            if (null != set)
                set.close();
            if (null != appActiveVersionStatement)
                appActiveVersionStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    private Project getProject(String projectId, int version) throws SQLException {
        Connection connection = null;
        PreparedStatement projectStatement = null;
        ResultSet resultSet = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            projectStatement = connection.prepareStatement(PROJECT_QUERY);
            projectStatement.setString(1, projectId);
            projectStatement.setInt(2, version);
            resultSet = projectStatement.executeQuery();
            if (!resultSet.next())
                throw new SQLException(String.format("Project does not exists: projectId [%s]; version [%d]", projectId, version));
            final String type = resultSet.getString(1);
            final String application = resultSet.getString(2);
            final String description = resultSet.getString(3);
            final String tableSchema = resultSet.getString(4);
            final String procSchema = resultSet.getString(5);
            final int mode = resultSet.getInt(6);
            final MetadataGenerationMode mdGenMode = MetadataGenerationMode.fromInt(mode);
            return new Project(projectId, type, version, application, description, tableSchema, procSchema, mdGenMode);
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != projectStatement)
                projectStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    private ProjectTable[] getProjectTables(String projectId, int version) throws SQLException {
        Connection connection = null;
        PreparedStatement projectTablesStatement = null;
        ResultSet resultSet = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            projectTablesStatement = connection.prepareStatement(PROJECT_TABLES_QUERY);
            projectTablesStatement.setString(1, projectId);
            projectTablesStatement.setInt(2, version);
            resultSet = projectTablesStatement.executeQuery();
            List<ProjectTable> ptList = new ArrayList<ProjectTable>();
            while (resultSet.next()) {
                final String name = resultSet.getString(1);
                final int number = resultSet.getInt(2);
                ptList.add(new ProjectTable(projectId, name, number));
            }
            return ptList.toArray(new ProjectTable[ptList.size()]);
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != projectTablesStatement)
                projectTablesStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    private ProjectProcedure[] getProjectProcedures(String projectId, int version) throws SQLException {
        Connection connection = null;
        PreparedStatement projectProceduresStatement = null;
        ResultSet resultSet = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            projectProceduresStatement = connection.prepareStatement(PROJECT_PROCEDURES_QUERY);
            projectProceduresStatement.setString(1, projectId);
            projectProceduresStatement.setInt(2, version);
            resultSet = projectProceduresStatement.executeQuery();
            List<ProjectProcedure> ppList = new ArrayList<ProjectProcedure>();
            while (resultSet.next()) {
                final int number = resultSet.getInt(1);
                final char type = resultSet.getString(2).charAt(0);
                final String name = resultSet.getString(3);
                ppList.add(new ProjectProcedure(projectId, number, type, name));
            }
            return ppList.toArray(new ProjectProcedure[ppList.size()]);
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != projectProceduresStatement)
                projectProceduresStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    private ProjectTableKey[] getProjectTableKeys(String projectId, int version) throws SQLException {
        Connection connection = null;
        PreparedStatement projectTableKeysStatement = null;
        ResultSet resultSet = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            projectTableKeysStatement = connection.prepareStatement(PROJECT_TABLE_KEYS_QUERY);
            projectTableKeysStatement.setString(1, projectId);
            projectTableKeysStatement.setInt(2, version);
            resultSet = projectTableKeysStatement.executeQuery();
            List<ProjectTableKey> ptkList = new ArrayList<ProjectTableKey>();
            while (resultSet.next()) {
                final int number = resultSet.getInt(1);
                final int tableNumber = resultSet.getInt(2);
                final String tableName = resultSet.getString(3);
                final String keyName = resultSet.getString(4);
                final String dbType = resultSet.getString(5);
                final ProjectKeyType pkType = ProjectKeyType.convert(dbType);
                final int threshold = resultSet.getInt(6);
                final boolean isInitial = getFlagValue(resultSet, 7);
                final boolean isSummable = getFlagValue(resultSet, 8);
                final boolean isSequential = getFlagValue(resultSet, 9);
                ptkList.add(new ProjectTableKey(projectId, number, tableNumber, tableName, keyName, pkType, dbType,
                        threshold, isInitial, isSummable, isSequential));
            }
            return ptkList.toArray(new ProjectTableKey[ptkList.size()]);
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != projectTableKeysStatement)
                projectTableKeysStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    /**
     * Retrieves the workload priority for a given project from the RT_PROJECT_PRIORITY table.
     * @param projectId the project id.
     * @param version the project version.
     * @return the workload priority.
     * @throws SQLException
     */
    private Properties getWorkloadPriority(String projectId, int version) throws SQLException{
        Connection connection = null;
        PreparedStatement workloadPriorityStatement = null;
        ResultSet resultSet = null;
        Properties clientInfo = new Properties();
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            workloadPriorityStatement = connection.prepareStatement(PROJECT_PRIORITY_QUERY);
            workloadPriorityStatement.setString(1, projectId);
            workloadPriorityStatement.setInt(2, version);
            resultSet = workloadPriorityStatement.executeQuery();
            if (resultSet.next()) {
                clientInfo.setProperty("APPLICATION", resultSet.getString(1));
            }else{
                clientInfo.setProperty("APPLICATION", CONFIG.getWorkloadPriority());
            }
            return clientInfo;
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != workloadPriorityStatement)
                workloadPriorityStatement.close();
            if (null != connection)
                connection.close();
        }
    }

    private boolean getFlagValue(ResultSet resultSet, int column) throws SQLException {
        final String value = resultSet.getString(column);
        return null != value && value.toLowerCase().equals("x");
    }

    public void close() {
        log.warn("Application Reader stopped");
    }
}
