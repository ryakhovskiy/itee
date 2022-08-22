package org.kr.intp.application.manager;

import org.kr.intp.application.AppContext;
import org.kr.intp.application.pojo.job.*;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.meta.DbMetadataManager;
import org.kr.intp.util.db.meta.DbMetadataManagerFactory;
import org.kr.intp.util.db.meta.StatementManager;
import org.kr.intp.util.db.meta.StatementsManagerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ApplicationMetadataManager {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private static final IntpConfig CONFIG = AppContext.instance().getConfiguration();
    private static final String SCHEMA = CONFIG.getIntpSchema();
    private static final String AGGR_PLACEHOLDER = CONFIG.getAggregationPlaceholder();
    private static final String INTP_GEN_SCHEMA = CONFIG.getIntpGenObjectsSchema();

    public static void generateMetadata(Connection connection, String projectId, int version) throws SQLException, IOException, CreateMetadataException {
        new ApplicationMetadataManager(connection, projectId, version).createMetadata();
    }

    public static void removeMetadata(Connection connection, String projectId, int version) throws SQLException, IOException, RemoveMetadataException {
        new ApplicationMetadataManager(connection, projectId, version).removeMetadata();
    }

    private final Logger log = LoggerFactory.getLogger(ApplicationMetadataManager.class);
    private final ApplicationJob application;
    private final String triggerTableName;
    private final String sequenceName;
    private final String insertTriggerName;
    private final String updateTriggerName;
    private final String deleteTriggerName;
    private final String runLogTableName;
    private final String deltaKeysProcedureName;
    private final String initialKeysProcedureName;
    private final DbMetadataManager dbMetadataManager;
    private final StatementManager statementManager;

    private final String keyStatisticsTableName;
    private final String updateKeyStatisticsProcedure;
    private final String runtimeViewName;
    private final String runtimeArchiveTableName;

    private ApplicationMetadataManager(Connection connection, String projectId, int version) throws SQLException, IOException {
        this.dbMetadataManager = DbMetadataManagerFactory.newHanaDbMetadataManager(connection);
        this.statementManager = StatementsManagerFactory.newHanaStatementManager(connection);
        final Application app = ApplicationReader.getInstance().getApplication(projectId, version);
        final ApplicationJob[] applicationJobs = app.getApplicationJobs();
        if (0 == applicationJobs.length)
            throw new RuntimeException(String.format("No ApplicationJobs found for application [%s], version [%d], check data consistency",
                    projectId, version));

        this.application = applicationJobs[0];
        this.triggerTableName = String.format("%s_%s_TRIGGER_LOG", application.getProjectId(), application.getTableName());
        this.sequenceName = String.format("%s_%s_SEQ", application.getProjectId(), application.getTableName());

        insertTriggerName = String.format("%s_%s_%s_TRIGGER", application.getProjectId(),
                application.getTableName(), "INSERT");
        updateTriggerName = String.format("%s_%s_%s_TRIGGER", application.getProjectId(),
                application.getTableName(), "UPDATE");
        deleteTriggerName = String.format("%s_%s_%s_TRIGGER", application.getProjectId(),
                application.getTableName(), "DELETE");

        this.runLogTableName = String.format("%s_%s_RUN_LOG", application.getProjectId(), application.getTableName());

        // Table and Procedure required to collect the key statistics
        this.keyStatisticsTableName = String.format("%s_%s_KEY_STATISTICS", application.getProjectId(), application.getTableName());
        this.updateKeyStatisticsProcedure = String.format("%s_UPDATE_KEY_STATISTICS", application.getProjectId());
        this.runtimeViewName = String.format("%s_RUNTIME_INFO", application.getProjectId());
        this.deltaKeysProcedureName = application.getDeltaKeysProcedureName();
        this.initialKeysProcedureName = application.getInitialKeysProcedureName();
        this.runtimeArchiveTableName = String.format("%s_%s_RUNTIME_ARCHIVE", application.getProjectId(), application.getTableName());
    }

    private void createMetadata() throws SQLException, CreateMetadataException {
        switch (application.getMetadataGenerationMode()) {
            case FULL:
                createTriggerTable(dbMetadataManager, statementManager);
                createSequence(dbMetadataManager, statementManager);
                createTrigger(dbMetadataManager, statementManager, "insert");
                createTrigger(dbMetadataManager, statementManager, "update");
                createTrigger(dbMetadataManager, statementManager, "delete");
                break;
            case SKIP_TRIGGERS:
            default:
                log.debug("skipping triggers creation; gen_option == 1");
                checkObjects();
                checkTriggers();
                break;
        }
        createRuntimeArchiveTable(dbMetadataManager, statementManager);
        createRunLogTable(dbMetadataManager, statementManager);
        createIndex(dbMetadataManager, statementManager, runLogTableName, Collections.singleton("UUID"));
        createIndex(dbMetadataManager, statementManager, runLogTableName, Collections.singleton("STATUS"));
        createRuntimeView(dbMetadataManager, statementManager);
        createLcView(dbMetadataManager, statementManager);
        createKeyStatisticsTable(dbMetadataManager, statementManager);
        createUpdateKeyStatisticsProcedure(dbMetadataManager, statementManager);
        createDeltaKeysStoredProcedure(dbMetadataManager, statementManager);
        createInitialKeysStoredProcedure(dbMetadataManager, statementManager);
        createDefaultStoredProcedures(dbMetadataManager, statementManager);
    }

    private void checkTriggers() throws SQLException {
        StringBuilder errorMessage = new StringBuilder();
        //check insert trigger
        if (!dbMetadataManager.isTriggerExists(this.application.getTableSchema(), insertTriggerName))
            errorMessage.append(application.getTableSchema()).append('.').append(insertTriggerName).append('\n');

        //check update trigger
        if (!dbMetadataManager.isTriggerExists(application.getTableSchema(), updateTriggerName))
            errorMessage.append(application.getTableSchema()).append('.').append(updateTriggerName).append('\n');

        //check delete trigger
        if (!dbMetadataManager.isTriggerExists(application.getTableSchema(), deleteTriggerName))
            errorMessage.append(application.getTableSchema()).append('.').append(deleteTriggerName).append('\n');

        if (errorMessage.length() > 0)
            log.warn(String.format("Missed trigger(s) for version %d of [%s]: %s",
                    application.getVersion(), application.getProjectId(), errorMessage));
    }

    private void checkObjects() throws SQLException, CreateMetadataException {
        if (application.getType() == JobTriggerType.SCHEDULE)
            return;
        StringBuilder errorMessage = new StringBuilder();
        if (!dbMetadataManager.isTableExists(INTP_GEN_SCHEMA, triggerTableName)) {
            errorMessage.append(INTP_GEN_SCHEMA).append('.').append(triggerTableName).append('\n');
        }
        //check sequence
        if (!dbMetadataManager.isSequenceExists(INTP_GEN_SCHEMA, sequenceName)) {
            errorMessage.append(INTP_GEN_SCHEMA).append('.').append(sequenceName).append('\n');
        }
        if (errorMessage.length() > 0) {
            String errorString = String.format("Missed object(s) for version %d of [%s]: %s",
                    application.getVersion(), application.getProjectId(), errorMessage);
            throw new CreateMetadataException(errorString);
        }
    }

    private void removeMetadata() throws SQLException, RemoveMetadataException {
        //remove triggers, remove table
        dropGetInitialKeysStoredProcedure(dbMetadataManager);
        dropGetDeltaKeysStoredProcedure(dbMetadataManager);
        dropUpdateKeyStatisticsProcedure(dbMetadataManager);

        dropRuntimeView(dbMetadataManager);
        dropLcView(dbMetadataManager);
        switch (application.getMetadataGenerationMode()) {
            case FULL:
                dropTrigger(dbMetadataManager, "insert");
                dropTrigger(dbMetadataManager, "update");
                dropTrigger(dbMetadataManager, "delete");
                dropSequence(dbMetadataManager);
                dropTriggerTable(dbMetadataManager);
                break;
            case SKIP_TRIGGERS:
            default:
                log.debug("skipping triggers dropping; gen_option == 1");
                break;
        }

        dropIndex(dbMetadataManager, runLogTableName, Collections.singleton("UUID"));
        dropIndex(dbMetadataManager, runLogTableName, Collections.singleton("STATUS"));
        dropRunLogTable(dbMetadataManager);
        dropKeyStatisticsTable(dbMetadataManager);
        dropDefaultProcedures(dbMetadataManager);
        dropRuntimeArchiveTable(dbMetadataManager);
        //statementManager.deleteSqlStatements(application.getProjectId(), application.getVersion());
    }

    /******************************************************************************************************************/

    private void createDefaultStoredProcedures(DbMetadataManager dbMetadataManager, StatementManager statementManager)
            throws SQLException {
        createCleanupTriggerLogProcedure(dbMetadataManager, statementManager);
        final String procedure = createArchiveRunLogProcedure(dbMetadataManager, statementManager);
        createDefaultLifecycleProcedures(procedure);
        createRestartErrorsProcedure(dbMetadataManager, statementManager);
    }

    private String createRestartErrorsProcedure(DbMetadataManager dbMetadataManager, StatementManager statementManager)
            throws SQLException {
        if (application.getType() == JobTriggerType.SCHEDULE)
            return "";
        final String name = String.format("%s_%s_RESTART_ERRORS", application.getProjectId(), application.getTableName());
        if (dbMetadataManager.isProcedureExists(INTP_GEN_SCHEMA, name))
            return name;
        log.debug("creating stored procedure: " + name);
        final String sequence = String.format("%s_%s_SEQ", application.getProjectId(), application.getTableName());
        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("create procedure ").append(INTP_GEN_SCHEMA).append('.').append(name).append(LINE_SEPARATOR);
        sqlBuilder.append("sql security invoker as\nbegin\n");
        sqlBuilder.append("\tuuids =\n\t\tselect uuid\n\t\tfrom ").append(INTP_GEN_SCHEMA).append('.').append(runLogTableName);
        sqlBuilder.append("\n\t\twhere status = (select id from ").append(SCHEMA);
        sqlBuilder.append(".RT_STATUS_DESC where name = 'E')\n\t\tfor update;\n\n");

        sqlBuilder.append("\tkeys = select distinct ");
        for (ProjectTableKey key : application.getKeys()) {
            if (key.isInitial())
                sqlBuilder.append(key.getKeyName()).append(" as ").append(key.getKeyName()).append(',');
            else {
                sqlBuilder.append("ifnull(").append(key.getKeyName()).append(",'");
                sqlBuilder.append(AGGR_PLACEHOLDER).append("')");
                sqlBuilder.append(" as ").append(key.getKeyName()).append(',');
            }
        }
        sqlBuilder.deleteCharAt(sqlBuilder.length() - 1).append("\n\t\tfrom ").append(INTP_GEN_SCHEMA).append('.');
        sqlBuilder.append(runLogTableName).append("\n\t\twhere status = (select id from ").append(SCHEMA);
        sqlBuilder.append(".RT_STATUS_DESC where name = 'B')\n\t\t\tand uuid in (select uuid from :uuids);\n\n");

        sqlBuilder.append("\tinsert into ").append(INTP_GEN_SCHEMA).append('.').append(triggerTableName).append('\n');
        sqlBuilder.append("\tselect\n\t\t").append(INTP_GEN_SCHEMA).append('.').append(sequence).append(".nextval,\n");
        sqlBuilder.append("\t\tcurrent_utctimestamp,\n\t\t'r',\n\t\t");
        for (ProjectTableKey key : application.getKeys())
            sqlBuilder.append(key.getKeyName()).append(",");
        sqlBuilder.deleteCharAt(sqlBuilder.length() - 1).append(" \n\tfrom :keys;\n\n");

        sqlBuilder.append("\tdelete from ").append(INTP_GEN_SCHEMA).append('.').append(runLogTableName);
        sqlBuilder.append(" where uuid in (select uuid from :uuids);\n\n");
        sqlBuilder.append("end\n");

        final String sql = sqlBuilder.toString();
        dbMetadataManager.executeSql(sql);
        statementManager.saveSqlStatement(application.getProjectId(), application.getVersion(), name, sql, "STORED PROCEDURE");
        return name;
    }

    private void createDefaultLifecycleProcedures(String procedure) throws SQLException {
        final long factor = CONFIG.getArchivelogPeriodFactor();
        final long periodMS = application.getInitialProperties().getPeriod() * factor;
        final long period = TimeUnit.MILLISECONDS.toSeconds(periodMS);
        dbMetadataManager.executeSql("delete from " + SCHEMA + ".RT_LIFECYCLE_JOBS where PROJECT_ID = '"
                + application.getProjectId() + '\'');

        final String sql = String.format("insert into %s.RT_LIFECYCLE_JOBS values ", SCHEMA) +
                String.format("('%s', 'ARCHIVE RUN_LOG', '\"%s\".\"%s\"', %d, NULL, NULL, 1, 'Y')",
                        application.getProjectId(), INTP_GEN_SCHEMA, procedure, period);
        dbMetadataManager.executeSql(sql);
    }

    private String createCleanupTriggerLogProcedure(
            DbMetadataManager dbMetadataManager, StatementManager statementManager) throws SQLException {
        final String name = String.format("%s_CLEANUP_TRIGGER_LOG", application.getProjectId());
        if (application.getMetadataGenerationMode() == MetadataGenerationMode.SKIP_TRIGGERS
                || application.getType() == JobTriggerType.SCHEDULE) {
            log.warn("skipping creation of procedure: " + name + " due to metadata generation mode or application type");
            return "";
        }
        if (dbMetadataManager.isProcedureExists(INTP_GEN_SCHEMA, name))
            return name;
        log.debug("creating stored procedure: " + name);
        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("create procedure ").append(INTP_GEN_SCHEMA).append('.').append(name).append("()").append(LINE_SEPARATOR);
        sqlBuilder.append("language SQLScript sql security invoker as").append(LINE_SEPARATOR);
        sqlBuilder.append("begin").append(LINE_SEPARATOR);
        sqlBuilder.append("\tdelete from ").append(INTP_GEN_SCHEMA).append('.').append(triggerTableName).append(';');
        sqlBuilder.append(LINE_SEPARATOR).append("end").append(LINE_SEPARATOR);
        final String sql = sqlBuilder.toString();
        dbMetadataManager.executeSql(sql);
        statementManager.saveSqlStatement(application.getProjectId(), application.getVersion(), name, sql, "STORED PROCEDURE");
        return name;
    }

    private String createArchiveRunLogProcedure(DbMetadataManager dbMetadataManager, StatementManager statementManager)
            throws SQLException {
        final String name = String.format("%s_ARCHIVE_RUN_LOG", application.getProjectId());
        if (dbMetadataManager.isProcedureExists(INTP_GEN_SCHEMA, name))
            return name;
        log.debug("creating stored procedure: " + name);
        final StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("create procedure ").append(INTP_GEN_SCHEMA).append('.').append(name).append(LINE_SEPARATOR);
        sqlBuilder.append("\tsql security invoker as").append(LINE_SEPARATOR);
        sqlBuilder.append("begin").append(LINE_SEPARATOR);
        sqlBuilder.append("DECLARE run_exists INTEGER;").append(LINE_SEPARATOR);
        sqlBuilder.append("SELECT COUNT(*) INTO run_exists FROM ").append(INTP_GEN_SCHEMA).append('.').append(runtimeViewName).append(";").append(LINE_SEPARATOR);
        sqlBuilder.append("IF run_exists >0 THEN").append(LINE_SEPARATOR);
        sqlBuilder.append("DELETE FROM ").append(INTP_GEN_SCHEMA).append('.').append(runtimeArchiveTableName).append(";").append(LINE_SEPARATOR);
        sqlBuilder.append("INSERT INTO ").append(INTP_GEN_SCHEMA).append('.').append(runtimeArchiveTableName);
        sqlBuilder.append(" SELECT UUID, STARTED, PROC_TIME, \"P-STARTED\", \"P-FINISHED\"");
        for (ProjectTableKey key : application.getKeys()) {
            sqlBuilder.append(", ");
            sqlBuilder.append(key.getKeyName());
        }
        sqlBuilder.append(" FROM ").append(INTP_GEN_SCHEMA).append('.').append(runtimeViewName).append(";").append(LINE_SEPARATOR);
        sqlBuilder.append("END IF;").append(LINE_SEPARATOR);
        sqlBuilder.append("\tuuids = select uuid from ").append(INTP_GEN_SCHEMA).append('.').append(runLogTableName);
        sqlBuilder.append(" where status = 50;").append(LINE_SEPARATOR).append(LINE_SEPARATOR);
        sqlBuilder.append("\tdata = select t.job_name, t.type, t.UPD_TIMESTAMP, t.uuid, t.triggered, s.started, f.finished,");
        sqlBuilder.append(application.getInitialProperties().getPeriod() * 1000).append(" as SLA_MILLIS").append(LINE_SEPARATOR);
        sqlBuilder.append("\t\tfrom ").append(INTP_GEN_SCHEMA).append('.').append(runLogTableName).append(" t").append(LINE_SEPARATOR);
        sqlBuilder.append("\t\t\tinner join ").append(INTP_GEN_SCHEMA).append('.').append(runLogTableName).append(" s ");
        sqlBuilder.append(" on  t.uuid = s.uuid and t.status = 0 and s.status = 20").append(LINE_SEPARATOR);
        sqlBuilder.append("\t\t\tinner join ").append(INTP_GEN_SCHEMA).append('.').append(runLogTableName).append(" f ");
        sqlBuilder.append(" on t.uuid = f.uuid and f.status = 50;").append(LINE_SEPARATOR).append(LINE_SEPARATOR);
        sqlBuilder.append("\tinsert into ").append(SCHEMA).append('.').append("RT_RUN_LOG_HISTORY ");
        sqlBuilder.append(" select * from :data;").append(LINE_SEPARATOR).append(LINE_SEPARATOR);
        sqlBuilder.append("\tdelete from ").append(INTP_GEN_SCHEMA).append('.').append(runLogTableName);
        sqlBuilder.append(" where uuid in (select uuid from :uuids);").append(LINE_SEPARATOR);
        sqlBuilder.append("end;").append(LINE_SEPARATOR);
        final String sql = sqlBuilder.toString();
        dbMetadataManager.executeSql(sql);
        statementManager.saveSqlStatement(application.getProjectId(), application.getVersion(), name, sql, "STORED PROCEDURE");
        return name;
    }

    private void createSequence(DbMetadataManager dbMetadataManager, StatementManager statementManager) throws SQLException {
        log.debug("creating sequence: " + sequenceName);
        if (dbMetadataManager.isSequenceExists(INTP_GEN_SCHEMA, sequenceName)) {
            log.trace("skipping creating sequence, already exists: " + sequenceName);
            return;
        }
        final String sqlString = String.format("create sequence %s.%s", INTP_GEN_SCHEMA, sequenceName);
        dbMetadataManager.executeSql(sqlString);
        statementManager.saveSqlStatement(application.getProjectId(), application.getVersion(), sequenceName, sqlString, "SEQUENCE");
    }

    private void createTriggerTable(DbMetadataManager dbMetadataManager, StatementManager statementManager) throws SQLException {
        log.debug("creating table: " + triggerTableName);
        if (application.getMetadataGenerationMode() == MetadataGenerationMode.SKIP_TRIGGERS) {
            log.debug("skipping trigger table creation; gen_option == 1");
            return;
        }
        if (dbMetadataManager.isTableExists(triggerTableName)) {
            log.trace("skipping creating table, already exists: " + triggerTableName);
            return;
        }
        final StringBuilder sql = new StringBuilder();
        sql.append("create row table ").append(INTP_GEN_SCHEMA).append('.').append(triggerTableName);
        sql.append(" (EVENT_ID BIGINT, UPD_TIMESTAMP TIMESTAMP, ACTION CHAR(1)");
        for (ProjectTableKey key : application.getKeys()) {
            sql.append(',');
            sql.append(key.getKeyName());
            sql.append(' ');
            sql.append(key.getKeyDbType());
        }
        sql.append(", primary key(EVENT_ID))");
        final String sqlString = sql.toString();
        dbMetadataManager.executeSql(sqlString);
        statementManager.saveSqlStatement(application.getProjectId(), application.getVersion(), triggerTableName, sqlString, "TABLE");
    }

    private void createRunLogTable(DbMetadataManager dbMetadataManager, StatementManager statementManager) throws SQLException {
        log.debug("creating table: " + runLogTableName);
        if (dbMetadataManager.isTableExists(runLogTableName)) {
            log.trace("skipping creating table, already exists: " + runLogTableName);
            return;
        }
        StringBuilder sql = new StringBuilder();
        sql.append("create row table ");
        sql.append(INTP_GEN_SCHEMA).append('.').append(runLogTableName).append(LINE_SEPARATOR);
        sql.append(" ( TYPE char(1), UPD_TIMESTAMP timestamp, UUID nvarchar(64), STATUS integer, ");
        sql.append(" COMMENT nvarchar(1023), JOB_NAME nvarchar(63), FK_TRIGGERED timestamp, TRIGGERED timestamp, ");
        sql.append(" STARTED timestamp, FINISHED timestamp ");
        for (ProjectTableKey key : application.getKeys()) {
            sql.append(',');
            sql.append(key.getKeyName());
            sql.append(' ');
            sql.append(key.getKeyDbType());
        }
        sql.append(')');
        final String sqlString = sql.toString();
        dbMetadataManager.executeSql(sqlString);
        statementManager.saveSqlStatement(application.getProjectId(), application.getVersion(), runLogTableName, sqlString, "TABLE");
    }

    /**
     * Creates a table to archive parts of runtime info view
     *
     * @param dbMetadataManager the Database Metadata Manager.
     * @param statementManager  the Statement Manager.
     */
    private void createRuntimeArchiveTable(DbMetadataManager dbMetadataManager, StatementManager statementManager) throws SQLException {
        log.debug("creating table: " + runtimeArchiveTableName);
        if (dbMetadataManager.isTableExists(runtimeArchiveTableName)) {
            log.trace("skipping creating table, already exists: " + runtimeArchiveTableName);
            return;
        }
        StringBuilder sql = new StringBuilder();
        sql.append("create column table ");
        sql.append(INTP_GEN_SCHEMA).append('.').append(runtimeArchiveTableName).append(LINE_SEPARATOR);
        sql.append(" ( UUID NVARCHAR(64), STARTED NVARCHAR(21), PROC_TIME DOUBLE,");
        sql.append(" \"P-STARTED\" NVARCHAR(12), \"P-FINISHED\" NVARCHAR(12) ");
        for (ProjectTableKey key : application.getKeys()) {
            sql.append(',');
            sql.append(key.getKeyName());
            sql.append(' ');
            sql.append(key.getKeyDbType());
        }
        sql.append(')');
        final String sqlString = sql.toString();
        dbMetadataManager.executeSql(sqlString);
        statementManager.saveSqlStatement(application.getProjectId(), application.getVersion(), runtimeArchiveTableName, sqlString, "TABLE");
    }

    /**
     * This method creates a statistics table for the semantic keys of the respective project.
     *
     * @param dbMetadataManager the Database Metadata Manager.
     * @param statementManager  the Statement Manager.
     */
    private void createKeyStatisticsTable(DbMetadataManager dbMetadataManager, StatementManager statementManager) throws SQLException {
        log.debug("creating table: " + keyStatisticsTableName);
        if (dbMetadataManager.isTableExists(keyStatisticsTableName)) {
            log.trace("skipping creating table, already exists: " + keyStatisticsTableName);
            return;
        }
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE ROW TABLE ");
        sql.append(INTP_GEN_SCHEMA).append('.').append(keyStatisticsTableName).append(LINE_SEPARATOR);
        sql.append(" ( N INTEGER ");
        for (ProjectTableKey key : application.getKeys()) {
            sql.append(',');
            sql.append(key.getKeyName());
            sql.append(' ');
            sql.append(key.getKeyDbType());
        }
        sql.append(",MIN_EXEC_TIME DOUBLE, AVG_EXEC_TIME DOUBLE, MAX_EXEC_TIME DOUBLE");
        sql.append(')');
        final String sqlString = sql.toString();
        dbMetadataManager.executeSql(sqlString);
        statementManager.saveSqlStatement(application.getProjectId(), application.getVersion(), keyStatisticsTableName, sqlString, "TABLE");
    }

    /**
     * This method creates a procedure to update the key statistics table.
     *
     * @param dbMetadataManager the Database Metadata Manager.
     * @param statementManager  the Statement Manager.
     */
    private void createUpdateKeyStatisticsProcedure(DbMetadataManager dbMetadataManager, StatementManager statementManager) throws SQLException {
        log.debug("creating procedure: " + updateKeyStatisticsProcedure);
        if (dbMetadataManager.isProcedureExists(INTP_GEN_SCHEMA, updateKeyStatisticsProcedure)) {
            log.trace("skipping creating stored procedure, already exists: " + updateKeyStatisticsProcedure);
            return;
        }
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE PROCEDURE ").append(INTP_GEN_SCHEMA).append('.').append(updateKeyStatisticsProcedure).append("(").append(LINE_SEPARATOR);
        sql.append("IN job_id NVARCHAR(64),").append(LINE_SEPARATOR).append("IN job_ts TIMESTAMP)").append(LINE_SEPARATOR);
        sql.append("LANGUAGE SQLSCRIPT SQL SECURITY INVOKER AS").append(LINE_SEPARATOR);
        sql.append("BEGIN").append(LINE_SEPARATOR);

        // Update keys that already exist in the key statistics table
        sql.append("\t-- Update statistics for existing keys in the statistics table").append(LINE_SEPARATOR);
        sql.append("\tUPDATE \"").append(INTP_GEN_SCHEMA).append("\".\"").append(keyStatisticsTableName).append("\" l").append(LINE_SEPARATOR);
        sql.append("\tSET N = N+1,").append(LINE_SEPARATOR);
        sql.append("\tMIN_EXEC_TIME = CASE WHEN PROC_TIME<MIN_EXEC_TIME THEN PROC_TIME ELSE MIN_EXEC_TIME END,").append(LINE_SEPARATOR);
        sql.append("\tAVG_EXEC_TIME = (N*AVG_EXEC_TIME+PROC_TIME)/(N+1),").append(LINE_SEPARATOR);
        sql.append("\tMAX_EXEC_TIME = CASE WHEN PROC_TIME>MAX_EXEC_TIME THEN PROC_TIME ELSE MAX_EXEC_TIME END").append(LINE_SEPARATOR);
        sql.append("\tFROM ").append(INTP_GEN_SCHEMA).append('.').append(keyStatisticsTableName).append(" l").append(LINE_SEPARATOR);
        sql.append("\tLEFT JOIN ").append(INTP_GEN_SCHEMA).append('.').append(runtimeViewName).append(" r").append(LINE_SEPARATOR);
        sql.append("\tON ");

        ProjectTableKey[] keys = application.getKeys();
        for (int i = 0; i <= keys.length - 1; i++) {
            sql.append("l.");
            sql.append(keys[i].getKeyName());
            sql.append("= r.");
            sql.append(keys[i].getKeyName());
            if (i < keys.length - 1) {
                sql.append(" AND ");
            }
        }
        sql.append(LINE_SEPARATOR).append("\tWHERE UUID = :job_id;").append(LINE_SEPARATOR).append(LINE_SEPARATOR);

        // Insert new statistics
        sql.append("\t-- Insert statistics for non-existing keys in the statistics table").append(LINE_SEPARATOR);
        sql.append("\tINSERT INTO ").append(INTP_GEN_SCHEMA).append('.').append(keyStatisticsTableName).append(LINE_SEPARATOR);
        sql.append("\tSELECT 1 AS N, ");
        for (ProjectTableKey key : application.getKeys()) {
            sql.append("l.");
            sql.append(key.getKeyName());
            sql.append(", ");
        }
        sql.append("PROC_TIME, PROC_TIME, PROC_TIME").append(LINE_SEPARATOR);
        sql.append("\tFROM ").append(INTP_GEN_SCHEMA).append('.').append(runtimeViewName).append(" l").append(LINE_SEPARATOR);
        sql.append("\tLEFT JOIN ").append(INTP_GEN_SCHEMA).append('.').append(keyStatisticsTableName).append(" r").append(LINE_SEPARATOR);
        sql.append("\tON ");

        for (int i = 0; i <= keys.length - 1; i++) {
            sql.append("l.");
            sql.append(keys[i].getKeyName());
            sql.append("= r.");
            sql.append(keys[i].getKeyName());
            if (i < keys.length - 1) {
                sql.append(" AND ");
            } else {
                sql.append(LINE_SEPARATOR);
                sql.append("\tWHERE r.").append(keys[i].getKeyName()).append(" IS NULL AND UUID = :job_id;");
            }
        }

        sql.append(LINE_SEPARATOR).append("END;");
        final String sqlString = sql.toString();
        dbMetadataManager.executeSql(sqlString);
        statementManager.saveSqlStatement(application.getProjectId(), application.getVersion(), updateKeyStatisticsProcedure, sqlString, "STORED PROCEDURE");
    }


    private void createTrigger(DbMetadataManager dbMetadataChecker, StatementManager statementManager, String triggerAction) throws SQLException {
        final String triggerName = String.format("%s_%s_%s_TRIGGER", application.getProjectId(),
                application.getTableName(), triggerAction.toUpperCase());
        log.debug("creating trigger: " + triggerName);
        if (dbMetadataChecker.isTriggerExists(application.getTableSchema(), triggerName)) {
            log.trace("skipping creating trigger, already exists: " + triggerName);
            return;
        }
        final String sequenceName = String.format("%s_%s_SEQ", application.getProjectId(), application.getTableName());
        final StringBuilder sql = new StringBuilder();
        sql.append("create trigger ");
        sql.append(application.getTableSchema()).append('.').append(triggerName).append(LINE_SEPARATOR);
        sql.append(" after ").append(triggerAction).append(" on ").append(application.getTableSchema()).append('.');
        sql.append(application.getTableName()).append(LINE_SEPARATOR);
        if (triggerAction.toLowerCase().equals("delete"))
            sql.append(" referencing old row as record ");
        else
            sql.append(" referencing new row as record ");
        sql.append(LINE_SEPARATOR);
        sql.append(" for each row").append(LINE_SEPARATOR);
        sql.append("begin").append(LINE_SEPARATOR);
        sql.append("insert into ").append(INTP_GEN_SCHEMA).append('.').append(triggerTableName);
        sql.append(" select ").append(INTP_GEN_SCHEMA).append('.').append(sequenceName).append(".nextval,");
        sql.append(" current_utctimestamp, '").append(triggerAction.charAt(0)).append('\'');
        for (ProjectTableKey key : application.getKeys()) {
            sql.append(",:record.");
            sql.append(key.getKeyName());
        }
        sql.append(" from dummy;").append(LINE_SEPARATOR).append("end;");
        final String sqlString = sql.toString();
        dbMetadataChecker.executeSql(sqlString);
        final String statementName = String.format("%s.%s", application.getTableSchema(), triggerName);
        statementManager.saveSqlStatement(application.getProjectId(), application.getVersion(), statementName, sqlString, "TRIGGER");
    }

    private void createRuntimeView(DbMetadataManager dbMetadataManager, StatementManager statementManager) throws SQLException {
        final String view = String.format("%s_RUNTIME_INFO", application.getProjectId());
        log.debug("creating view: " + view);
        if (dbMetadataManager.isViewExists(view)) {
            log.trace("skipping creating view, already exists: " + view);
            return;
        }
        String runlogTableWithSchema = String.format("%s.%s", INTP_GEN_SCHEMA, runLogTableName);
        StringBuilder sql = new StringBuilder();
        sql.append("create view ").append(INTP_GEN_SCHEMA).append('.').append(view).append(LINE_SEPARATOR);
        sql.append("(").append(LINE_SEPARATOR);
        sql.append("\tTYPE,").append(LINE_SEPARATOR);
        sql.append("\tSTATUS,").append(LINE_SEPARATOR);
        sql.append("\tUUID,").append(LINE_SEPARATOR);
        sql.append("\tFK_TRIGGERED,").append(LINE_SEPARATOR);
        sql.append("\tTRIGGERED,").append(LINE_SEPARATOR);
        sql.append("\tSTARTED,").append(LINE_SEPARATOR);
        //sql.append("\tFINISHED,").append(LINE_SEPARATOR);
        sql.append("\t\"P-STARTED\",").append(LINE_SEPARATOR);
        sql.append("\t\"P-FINISHED\",").append(LINE_SEPARATOR);
        sql.append("\tWAIT_TIME,").append(LINE_SEPARATOR);
        sql.append("\tPROC_TIME,").append(LINE_SEPARATOR);
        sql.append("\tALL_TIME,");
        for (ProjectTableKey key : application.getKeys())
            sql.append(LINE_SEPARATOR).append('\t').append(key.getKeyName()).append(',');
        sql.deleteCharAt(sql.length() - 1);
        sql.append(LINE_SEPARATOR).append(", \"AGGREGATED_COUNT\")").append(LINE_SEPARATOR).append(" as ").append(LINE_SEPARATOR);
        sql.append("select").append(LINE_SEPARATOR);
        sql.append("\tt.TYPE,").append(LINE_SEPARATOR);
        sql.append("\tcase").append(LINE_SEPARATOR);
        sql.append("\t\twhen e.uuid is not null then 'ERROR'").append(LINE_SEPARATOR);
        sql.append("\t\twhen f.uuid is not null then 'DONE'").append(LINE_SEPARATOR);
        sql.append("\t\twhen p.uuid is not null then 'RUN'").append(LINE_SEPARATOR);
        sql.append("\t\twhen s.uuid is not null then 'STARTED'").append(LINE_SEPARATOR);
        sql.append("\t\telse 'WAIT' end as STATE,").append(LINE_SEPARATOR);
        sql.append("\tt.UUID,").append(LINE_SEPARATOR);
        sql.append("\tTO_VARCHAR(t.FK_TRIGGERED, 'DD-MM-YYYY HH24:MI:SS' ) as FK_TRIGGERED,").append(LINE_SEPARATOR);
        sql.append("\tTO_VARCHAR(t.TRIGGERED, 'DD-MM-YYYY HH24:MI:SS' ) as TRIGGERED,").append(LINE_SEPARATOR);
        sql.append("\tTO_VARCHAR(s.STARTED , 'DD-MM-YYYY HH24:MI:SS' ) as STARTED,").append(LINE_SEPARATOR);
        //sql.append("\tTO_VARCHAR(f.FINISHED , 'HH24:MI:SS' ) as FINISHED,").append(LINE_SEPARATOR);
        sql.append("\tTO_VARCHAR(p.STARTED, 'HH24:MI:SS.FF3' ) as \"P-STARTED\",").append(LINE_SEPARATOR);
        sql.append("\tTO_VARCHAR(p.FINISHED, 'HH24:MI:SS.FF3' ) as \"P-FINISHED\",").append(LINE_SEPARATOR);
        sql.append("\tNANO100_BETWEEN(t.FK_TRIGGERED, s.STARTED) / 10000000.0 as WAIT_TIME,").append(LINE_SEPARATOR);
        sql.append("\tNANO100_BETWEEN(p.STARTED, p.FINISHED) / 10000000.0 as PROC_TIME,").append(LINE_SEPARATOR);
        sql.append("\tNANO100_BETWEEN(t.FK_TRIGGERED, f.FINISHED) / 10000000.0 as ALL_TIME,");
        for (ProjectTableKey key : application.getKeys()) {
            if (key.isInitial())
                sql.append(LINE_SEPARATOR).append("\tp.").append(key.getKeyName()).append(',');
            else {
                sql.append(LINE_SEPARATOR).append("\tcase when p.").append(key.getKeyName()).append(" is not null then p.");
                sql.append(key.getKeyName()).append(" else '").append(AGGR_PLACEHOLDER).append("' end as ").append(key.getKeyName()).append(',');
            }
        }
        sql.append("AGGREGATED_COUNT");
        sql.append(LINE_SEPARATOR).append("from ").append(runlogTableWithSchema).append(" t").append(LINE_SEPARATOR);
        sql.append("\tleft join ").append(runlogTableWithSchema).append(" s on t.uuid = s.uuid and s.status = (select id from ").append(SCHEMA).append(".RT_STATUS_DESC where name = 'S') ").append(LINE_SEPARATOR);
        sql.append("\tleft join ").append(runlogTableWithSchema).append(" p on t.uuid = p.uuid and p.status = (select id from ").append(SCHEMA).append(".RT_STATUS_DESC where name = 'P') ").append(LINE_SEPARATOR);
        sql.append("\tleft join ").append(runlogTableWithSchema).append(" f on t.uuid = f.uuid and f.status = (select id from ").append(SCHEMA).append(".RT_STATUS_DESC where name = 'F') ").append(LINE_SEPARATOR);
        sql.append("\tleft join ").append(runlogTableWithSchema).append(" e on t.uuid = e.uuid and e.status = (select id from ").append(SCHEMA).append(".RT_STATUS_DESC where name = 'E') ").append(LINE_SEPARATOR);

        sql.append("\tLEFT JOIN (").append(LINE_SEPARATOR);
        sql.append("SELECT 'D' AS TYPE");
        for (ProjectTableKey k : application.getKeys()) {
            sql.append(", ");
            sql.append(k.getKeyName());
        }
        sql.append(", COUNT(*) AS AGGREGATED_COUNT").append(LINE_SEPARATOR);
        sql.append("\t FROM (").append(LINE_SEPARATOR);
        sql.append("\t\t SELECT ");
        for (ProjectTableKey k : application.getKeys()) {
            if (k.getNumber() == 1)
                sql.append(k.getKeyName());
            else if (k.getNumber() == application.getKeys().length)
                sql.append(", '").append(AGGR_PLACEHOLDER).append("' AS \"").append(k.getKeyName()).append("\"");
            else {
                sql.append(", ");
                sql.append("CASE WHEN ").append(application.getKeys()[k.getNumber()].getKeyName()).append(" = '").append(
                        AGGR_PLACEHOLDER).append("' THEN '").append(AGGR_PLACEHOLDER).append("' ELSE ").append(k.getKeyName()).append(" END AS \"").append(k.getKeyName()).append("\"");
            }
        }
        sql.append(LINE_SEPARATOR).append("\t\tFROM ").append("\"").append(INTP_GEN_SCHEMA).append("\"").append(".").append("\"").append(runLogTableName).append("\"");
        sql.append(LINE_SEPARATOR).append("\t\t WHERE STATUS = (SELECT ID FROM ").append("\"").append(SCHEMA).append("\"").append(".")
                .append("RT_STATUS_DESC WHERE NAME = 'A')").append(LINE_SEPARATOR);
        sql.append("\t)").append(LINE_SEPARATOR);
        sql.append("\t GROUP BY ");
        for (ProjectTableKey k : application.getKeys()) {
            if (k.getNumber() != 1) sql.append(", ");
            sql.append(k.getKeyName());
        }
        sql.append(LINE_SEPARATOR).append(") a").append(LINE_SEPARATOR);
        sql.append("ON p.TYPE = a.TYPE");
        for (ProjectTableKey k : application.getKeys()) {
            sql.append(" AND ");
            sql.append("p.").append(k.getKeyName()).append(" = ").append("a.").append(k.getKeyName());
        }
        sql.append(LINE_SEPARATOR);
        sql.append("where t.status = (select id from ").append(SCHEMA).append(".RT_STATUS_DESC where name = 'T')").append(LINE_SEPARATOR);
        sql.append("order by t.TRIGGERED DESC WITH READ ONLY");
        final String sqlString = sql.toString();
        dbMetadataManager.executeSql(sqlString);
        statementManager.saveSqlStatement(application.getProjectId(), application.getVersion(), view, sqlString, "VIEW");
    }

    private void createLcView(DbMetadataManager dbMetadataManager, StatementManager statementManager) throws SQLException {
        final String view = String.format("%s_LIFECYCLE_LOG", application.getProjectId());
        log.debug("creating view: " + view);
        if (dbMetadataManager.isViewExists(view)) {
            log.trace("skipping creating view, already exists: " + view);
            return;
        }
        final StringBuilder sql = new StringBuilder();
        sql.append("create view ").append(INTP_GEN_SCHEMA).append('.').append(view).append("\n");
        sql.append("(\n\tTRIGGERED,\n\tSTATUS,\n\tNAME,\n\tWAIT_TIME,\n\tPROC_TIME,\n\tALL_TIME,\n\tCOMMENT\n)\n AS \n");
        sql.append("\tselect\n\t\tt.update_ts as TRIGGERED,\n\t\tcase\n\t\t\twhen e.uuid is not null then 'ERROR'\n");
        sql.append("\t\t\twhen f.uuid is not null then 'FINISHED'\n\t\t\twhen s.uuid is not null then 'STARTED'\n");
        sql.append("\t\telse 'TRIGGERED' end as STATUS,\n\t\tt.name,\n");
        sql.append("\t\tnano100_between(t.update_ts, s.update_ts) / 10000000.0 as WAIT_TIME,\n");
        sql.append("\t\tnano100_between(s.update_ts, f.update_ts) / 10000000.0 as PROC_TIME,\n");
        sql.append("\t\tnano100_between(t.update_ts, f.update_ts) / 10000000.0 as ALL_TIME,\n\t\te.comment\n");
        sql.append("\tfrom ").append(SCHEMA).append(".RT_LIFECYCLE_LOG t\n");
        sql.append("\t\tleft join ").append(SCHEMA);
        sql.append(".RT_LIFECYCLE_LOG s on t.uuid = s.uuid and t.status = 0 and s.status = 20\n");
        sql.append("\t\tleft join ").append(SCHEMA).append(".rt_lifecycle_log f on t.uuid = f.uuid and f.status = 50\n");
        sql.append("\t\tleft join ").append(SCHEMA).append(".rt_lifecycle_log e on t.uuid = e.uuid and e.status = 100");
        sql.append("\n\twhere t.PROJECT_ID = '").append(application.getProjectId()).append("'\n\torder by 1 desc\n");
        final String sqlString = sql.toString();
        dbMetadataManager.executeSql(sqlString);
        statementManager.saveSqlStatement(application.getProjectId(), application.getVersion(), view, sqlString, "VIEW");
    }

    private void createDeltaKeysStoredProcedure(DbMetadataManager dbMetadataManager, StatementManager statementManager) throws SQLException {
        log.debug("creating stored procedure: " + deltaKeysProcedureName);
        if (dbMetadataManager.isProcedureExists(INTP_GEN_SCHEMA, deltaKeysProcedureName)) {
            log.trace("skipping creating stored procedure, already exists: " + deltaKeysProcedureName);
            return;
        }
        String sqlString;
        if (application.getType() == JobTriggerType.EVENT)
            sqlString = createDeltaKeysStoredProcedureEventBased();
        else
            sqlString = createDeltaKeysStoredProcedureScheduledBased();

        dbMetadataManager.executeSql(sqlString);
        statementManager.saveSqlStatement(application.getProjectId(), application.getVersion(), deltaKeysProcedureName, sqlString, "STORED PROCEDURE");
    }

    private void createInitialKeysStoredProcedure(DbMetadataManager dbMetadataManager, StatementManager statementManager) throws SQLException {
        log.debug("creating stored procedure: " + initialKeysProcedureName);
        if (dbMetadataManager.isProcedureExists(INTP_GEN_SCHEMA, initialKeysProcedureName)) {
            log.trace("skipping creating stored procedure, already exists: " + initialKeysProcedureName);
            return;
        }

        String sqlString;
        if (application.getType() == JobTriggerType.EVENT)
            sqlString = createInitialKeysStoredProcedureEventBased();
        else
            sqlString = createInitialKeysStoredProcedureScheduledBased();

        dbMetadataManager.executeSql(sqlString);
        statementManager.saveSqlStatement(application.getProjectId(), application.getVersion(), initialKeysProcedureName, sqlString, "STORED PROCEDURE");
    }

    private String createDeltaKeysStoredProcedureEventBased() {
        ProjectTableKey[] projectKeys = application.getKeys();
        SortedSet<ProjectTableKey> sortedKeySet = new TreeSet<>(new Comparator<ProjectTableKey>() {
            public int compare(ProjectTableKey o1, ProjectTableKey o2) {
                return o1.getNumber() - o2.getNumber();
            }
        });
        sortedKeySet.addAll(Arrays.asList(projectKeys));
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE PROCEDURE ").append("\"").append(INTP_GEN_SCHEMA).append("\"").append(".").append("\"").append(deltaKeysProcedureName).append("\"")
                .append("(").append(LINE_SEPARATOR)
                .append("\tIN job_id NVARCHAR(64),").append(LINE_SEPARATOR)
                .append("\tIN job_ts TIMESTAMP").append(LINE_SEPARATOR)
                .append(")");
        sql.append("LANGUAGE SQLSCRIPT SQL SECURITY INVOKER AS").append(LINE_SEPARATOR);
        sql.append("BEGIN").append(LINE_SEPARATOR);
        sql.append("\tDECLARE aggregate_level CHAR(1);").append(LINE_SEPARATOR);
        sql.append("\tDECLARE n_current_level TABLE(");
        for (ProjectTableKey k : sortedKeySet) {
            if (k.getNumber() != 1)
                sql.append(", ");
            sql.append(k.getKeyName()).append(" ").append(k.getKeyDbType());
        }
        sql.append(");").append(LINE_SEPARATOR);
        sql.append("\ta = SELECT EVENT_ID, UPD_TIMESTAMP ");


        for (ProjectTableKey k : sortedKeySet) {
            sql.append(',');
            sql.append(k.getKeyName());
        }

        sql.append(" FROM ").append(INTP_GEN_SCHEMA).append('.').append(triggerTableName).append(" FOR UPDATE;");
        sql.append(LINE_SEPARATOR);
        sql.append("\tDELETE FROM ").append(INTP_GEN_SCHEMA).append('.').append(triggerTableName);
        sql.append(" WHERE EVENT_ID IN (SELECT EVENT_ID FROM :a);").append(LINE_SEPARATOR);
        sql.append("\tEXEC 'commit';").append(LINE_SEPARATOR);
        sql.append("\tdelta_keys = SELECT DISTINCT ");
        for (ProjectTableKey k : sortedKeySet) {
            sql.append(k.getKeyName());
            sql.append(',');
        }
        sql.deleteCharAt(sql.length() - 1); //delete last comma
        sql.append(" FROM :a;").append(LINE_SEPARATOR).append(LINE_SEPARATOR);
        sql.append("\t-- Get project configuration\t\n").append(
                "\tactive_project_configuration = SELECT l.PROJECT_ID, l.VERSION, KEY_NAME, NUMBER, KEY_THRESHOLD, KEY_SUMMABLE \n").append(
                "\tFROM \"").append(SCHEMA).append("\".\"RT_ACTIVE_PROJECTS\" l\n").append(
                "\tLEFT JOIN \"").append(SCHEMA).append("\".\"RT_PROJECT_KEYS\" r\n").append(
                "\tON l.PROJECT_ID = r.PROJECT_ID AND l.VERSION = r.VERSION\n").append(
                "\tWHERE l.PROJECT_ID = '").append(application.getProjectId()).append("';").append(LINE_SEPARATOR);


        // Threshold number aggregation
        for (int i = 1; i < sortedKeySet.size(); i++) {
            sql.append(LINE_SEPARATOR).append("\t-- Number Threshold Aggregation on level ").append(i).append(LINE_SEPARATOR);
            sql.append("\tSELECT KEY_SUMMABLE INTO aggregate_level FROM :active_project_configuration WHERE NUMBER = ").append((i + 1)).append(";")
                    .append(LINE_SEPARATOR);
            sql.append("\tIF aggregate_level = 'X' THEN").append(LINE_SEPARATOR);
            sql.append("\t\tn_current_level = SELECT ");
            for (ProjectTableKey k : sortedKeySet) {
                if (k.getNumber() != 1)
                    sql.append(", ");
                if (k.getNumber() <= i)
                    sql.append(k.getKeyName());
                else
                    sql.append("'").append(AGGR_PLACEHOLDER).append("' AS ").append(k.getKeyName());
            }
            sql.append(LINE_SEPARATOR);
            sql.append("\t\tFROM :delta_keys").append(LINE_SEPARATOR);
            sql.append("\t\tGROUP BY ");
            for (ProjectTableKey k : sortedKeySet) {
                if (k.getNumber() != 1 && k.getNumber() <= i)
                    sql.append(", ");
                if (k.getNumber() <= i)
                    sql.append(k.getKeyName());
            }
            sql.append(LINE_SEPARATOR);
            sql.append("\t\tHAVING COUNT(*) >= (SELECT KEY_THRESHOLD FROM :active_project_configuration WHERE NUMBER = ").append((i + 1))
                    .append(");").append(LINE_SEPARATOR).append(LINE_SEPARATOR);

            // insert aggregated from current level into run-log
            sql.append("\t\tINSERT INTO ").append("\"").append(INTP_GEN_SCHEMA).append("\"").append(".").append("\"").append(runLogTableName).append("\"").append(LINE_SEPARATOR);
            sql.append("\t\tSELECT 'D', :job_ts, :job_id, 30, null, '").append(application.getProjectId()).append("', null, null, null, null");
            for (ProjectTableKey k : sortedKeySet) {
                sql.append(", r.");
                sql.append(k.getKeyName());
            }
            sql.append(LINE_SEPARATOR).append("\t\tFROM :n_current_level l").append(LINE_SEPARATOR);
            sql.append("\t\tLEFT JOIN :delta_keys r").append(LINE_SEPARATOR);
            sql.append("\t\tON ");
            for (ProjectTableKey k : sortedKeySet) {
                if (k.getNumber() != 1 && k.getNumber() <= i)
                    sql.append(" AND ");
                if (k.getNumber() <= i)
                    sql.append("l.").append(k.getKeyName()).append(" = r.").append(k.getKeyName());
            }
            sql.append(LINE_SEPARATOR).append("\t\tWHERE r.").append(sortedKeySet.first().getKeyName()).append(" IS NOT NULL");
            sql.append(";").append(LINE_SEPARATOR).append(LINE_SEPARATOR);

            sql.append("\t\tdelta_keys = SELECT ");
            for (ProjectTableKey k : sortedKeySet) {
                if (k.getNumber() != 1)
                    sql.append(", ");
                sql.append("l.").append(k.getKeyName());
            }
            sql.append(LINE_SEPARATOR);
            sql.append("\t\t\tFROM :delta_keys l").append(LINE_SEPARATOR);
            sql.append("\t\t\tLEFT JOIN :n_current_level r").append(LINE_SEPARATOR);
            sql.append("\t\t\tON ");
            for (ProjectTableKey k : sortedKeySet) {
                if (k.getNumber() != 1 && k.getNumber() <= i)
                    sql.append(" AND ");
                if (k.getNumber() <= i)
                    sql.append("l.").append(k.getKeyName()).append(" = r.").append(k.getKeyName());
            }
            sql.append(LINE_SEPARATOR).append("\t\tWHERE r.").append(sortedKeySet.first().getKeyName()).append(" IS NULL").append(LINE_SEPARATOR);
            sql.append("\t\tUNION ALL").append(LINE_SEPARATOR);
            sql.append("\t\tSELECT ");
            for (ProjectTableKey k : sortedKeySet) {
                if (k.getNumber() != 1)
                    sql.append(", ");
                sql.append(k.getKeyName());
            }
            sql.append(LINE_SEPARATOR).append("\t\tFROM :n_current_level;").append(LINE_SEPARATOR);
            sql.append("\tEND IF;").append(LINE_SEPARATOR).append(LINE_SEPARATOR);
        }

        // Execution time aggregation
        for (int i = sortedKeySet.size(); i > 1; i--) {
            sql.append("\t-- Execution Time Aggregation on level ").append(i).append(LINE_SEPARATOR).append(LINE_SEPARATOR);
            sql.append("\tSELECT KEY_SUMMABLE INTO aggregate_level FROM :active_project_configuration WHERE NUMBER = ").append(i).append(";").append(LINE_SEPARATOR);

            sql.append("\tIF aggregate_level = 'X' THEN").append(LINE_SEPARATOR);

            // current level
            sql.append("\t\te_current_level = SELECT ");
            for (ProjectTableKey k : sortedKeySet) {
                if (k.getNumber() >= i)
                    sql.append("'").append(AGGR_PLACEHOLDER).append("' AS ");
                else
                    sql.append("l.");
                sql.append(k.getKeyName());
                sql.append(", ");
            }
            sql.append("SUM(AVG_EXEC_TIME) AS SUM_LEVEL").append(LINE_SEPARATOR);
            sql.append("\t\tFROM :delta_keys l").append(LINE_SEPARATOR);
            sql.append("\t\tLEFT JOIN \"").append(INTP_GEN_SCHEMA).append("\".\"").append(keyStatisticsTableName).append("\" r").append(LINE_SEPARATOR);
            sql.append("\t\tON");
            for (ProjectTableKey k : sortedKeySet) {
                sql.append(" l.").append(k.getKeyName()).append(" = ").append("r.").append(k.getKeyName());
                if (k.getNumber() < sortedKeySet.size())
                    sql.append(" AND");
            }
            sql.append(LINE_SEPARATOR).append("\t\tGROUP BY ");
            for (ProjectTableKey k : sortedKeySet) {
                if (k.getNumber() < i)
                    sql.append("l.").append(k.getKeyName());
                if (k.getNumber() < i - 1)
                    sql.append(", ");
            }
            sql.append(";").append(LINE_SEPARATOR).append(LINE_SEPARATOR);

            // current level to be aggregated
            sql.append("\t\te_current_level_to_be_aggregated = SELECT ");
            for (ProjectTableKey k : sortedKeySet) {
                if (k.getNumber() >= i)
                    sql.append("'").append(AGGR_PLACEHOLDER).append("' AS ");
                else
                    sql.append("l.");
                sql.append(k.getKeyName());
                sql.append(", ");
            }
            sql.append("SUM_LEVEL, AVG_EXEC_TIME").append(LINE_SEPARATOR);
            sql.append("\t\tFROM :e_current_level l").append(LINE_SEPARATOR);
            sql.append("\t\tLEFT JOIN \"").append(INTP_GEN_SCHEMA).append("\".\"").append(keyStatisticsTableName).append("\" r");
            sql.append(LINE_SEPARATOR);
            sql.append("\t\tON");
            for (ProjectTableKey k : sortedKeySet) {
                sql.append(" l.").append(k.getKeyName()).append(" = ").append("r.").append(k.getKeyName());
                if (k.getNumber() < sortedKeySet.size())
                    sql.append(" AND");
            }
            sql.append(LINE_SEPARATOR).append("\t\tWHERE SUM_LEVEL >= AVG_EXEC_TIME;").append(LINE_SEPARATOR).append(LINE_SEPARATOR);

            // insert aggregated from current level into run-log
            sql.append("\t\tINSERT INTO ").append("\"").append(INTP_GEN_SCHEMA).append("\"").append(".").append("\"").append(runLogTableName).append("\"").append(LINE_SEPARATOR);
            sql.append("\t\tSELECT 'D', :job_ts, :job_id, 30, null, '").append(application.getProjectId()).append("', null, null, null, null");
            for (ProjectTableKey k : sortedKeySet) {
                sql.append(", r.");
                sql.append(k.getKeyName());
            }
            sql.append(LINE_SEPARATOR).append("\t\tFROM :e_current_level_to_be_aggregated l").append(LINE_SEPARATOR);
            sql.append("\t\tLEFT JOIN :delta_keys r").append(LINE_SEPARATOR);
            sql.append("\t\tON");
            for (ProjectTableKey k : sortedKeySet) {
                if (k.getNumber() != 1 && k.getNumber() < i)
                    sql.append(" AND");
                if (k.getNumber() < i)
                    sql.append(" l.").append(k.getKeyName()).append(" = r.").append(k.getKeyName());
            }
            sql.append(LINE_SEPARATOR).append("\t\tWHERE r.").append(sortedKeySet.first().getKeyName()).append(" IS NOT NULL");
            sql.append(";").append(LINE_SEPARATOR).append(LINE_SEPARATOR);

            // delta keys
            sql.append("\t\tdelta_keys = SELECT ");
            for (ProjectTableKey k : sortedKeySet) {
                sql.append("l.").append(k.getKeyName());
                if (sortedKeySet.size() != k.getNumber())
                    sql.append(", ");
            }
            sql.append(LINE_SEPARATOR).append("\t\t\tFROM :delta_keys l").append(LINE_SEPARATOR);
            sql.append("\t\t\tLEFT JOIN :e_current_level_to_be_aggregated r").append(LINE_SEPARATOR);
            sql.append("\t\t\tON");
            for (ProjectTableKey k : sortedKeySet) {
                if (k.getNumber() != 1 && k.getNumber() < i)
                    sql.append(" AND");
                if (k.getNumber() < i)
                    sql.append(" l.").append(k.getKeyName()).append(" = r.").append(k.getKeyName());
            }
            sql.append(LINE_SEPARATOR).append("\t\t\t\tWHERE");
            for (ProjectTableKey k : sortedKeySet) {
                if (k.getNumber() != 1 && k.getNumber() < i)
                    sql.append(" AND");
                if (k.getNumber() < i)
                    sql.append(" r.").append(k.getKeyName()).append(" IS NULL");
            }
            sql.append(LINE_SEPARATOR).append("\t\tUNION ALL").append(LINE_SEPARATOR);
            sql.append("\t\tSELECT ");
            for (ProjectTableKey k : sortedKeySet) {
                sql.append(k.getKeyName());
                if (k.getNumber() < sortedKeySet.size())
                    sql.append(", ");
            }
            sql.append(LINE_SEPARATOR);
            sql.append("\t\tFROM :e_current_level_to_be_aggregated;").append(LINE_SEPARATOR);
            sql.append("\tEND IF;").append(LINE_SEPARATOR).append(LINE_SEPARATOR);
        }

        sql.append("\tSELECT * FROM :delta_keys;").append(LINE_SEPARATOR);
        sql.append("END;");
        return sql.toString();
    }

    private String createInitialKeysStoredProcedureEventBased() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE PROCEDURE ").append(INTP_GEN_SCHEMA).append('.').append(initialKeysProcedureName)
                .append("(").append(LINE_SEPARATOR)
                .append("\tIN job_id NVARCHAR(64),").append(LINE_SEPARATOR)
                .append("\tIN job_ts TIMESTAMP").append(LINE_SEPARATOR)
                .append(")").append(LINE_SEPARATOR);
        sql.append("LANGUAGE SQLSCRIPT SQL SECURITY INVOKER AS").append(LINE_SEPARATOR);
        sql.append("BEGIN").append(LINE_SEPARATOR);
        ProjectTableKey[] projectKeys = application.getKeys();
        sql.append("\ta = select EVENT_ID, UPD_TIMESTAMP ");
        for (ProjectTableKey k : projectKeys) {
            sql.append(',');
            sql.append(k.getKeyName());
        }
        sql.append(" from ").append(INTP_GEN_SCHEMA).append('.').append(triggerTableName).append(" for update;");
        sql.append(LINE_SEPARATOR);
        sql.append("\tdelete from ").append(INTP_GEN_SCHEMA).append('.').append(triggerTableName);
        sql.append(" where EVENT_ID in (select EVENT_ID from :a);").append(LINE_SEPARATOR);
        sql.append("\texec 'commit';").append(LINE_SEPARATOR);
        sql.append("\tselect ");
        int initialKeysCount = 0;
        for (ProjectTableKey k : projectKeys)
            if (k.isInitial())
                initialKeysCount++;
        if (initialKeysCount > 0) {
            for (ProjectTableKey k : projectKeys) {
                if (!k.isInitial())
                    continue;
                sql.append(k.getKeyName());
                sql.append(',');
            }
            sql.deleteCharAt(sql.length() - 1);
            for (int i = 0; i < projectKeys.length - initialKeysCount; i++) {
                sql.append(",'");
                sql.append(AGGR_PLACEHOLDER);
                sql.append('\'');
            }
            sql.append(" from :a group by ");
            for (ProjectTableKey k : projectKeys) {
                if (!k.isInitial())
                    continue;
                sql.append(k.getKeyName());
                sql.append(',');
            }
            sql.deleteCharAt(sql.length() - 1);
            sql.append(';');
        } else {
            for (int i = 0; i < projectKeys.length; i++) {
                sql.append('\'');
                sql.append(INTP_GEN_SCHEMA);
                sql.append("',");
            }
            sql.deleteCharAt(sql.length() - 1);
            sql.append(" from dummy; ");
        }
        sql.append(LINE_SEPARATOR).append("end");
        return sql.toString();
    }

    private String createDeltaKeysStoredProcedureScheduledBased() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE PROCEDURE ").append("\"").append(INTP_GEN_SCHEMA).append("\"").append(".").append("\"").append(deltaKeysProcedureName).append("\"")
                .append("(").append(LINE_SEPARATOR)
                .append("\tIN job_id NVARCHAR(64),").append(LINE_SEPARATOR)
                .append("\tIN job_ts TIMESTAMP").append(LINE_SEPARATOR)
                .append(")");
        sql.append("LANGUAGE SQLSCRIPT SQL SECURITY INVOKER AS").append(LINE_SEPARATOR);
        sql.append("BEGIN").append(LINE_SEPARATOR);

        //select nothing
        sql.append("select ");
        for (ProjectTableKey key : application.getKeys())
            sql.append(key.getKeyName()).append(',');

        sql.deleteCharAt(sql.length() - 1);
        sql.append(LINE_SEPARATOR).append("\tfrom ");
        sql.append(application.getTableSchema()).append('.').append(application.getTableName());
        sql.append(" where 0 = 1;");

        sql.append(LINE_SEPARATOR).append("END");
        return sql.toString();
    }

    private String createInitialKeysStoredProcedureScheduledBased() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE PROCEDURE ").append(INTP_GEN_SCHEMA).append('.').append(initialKeysProcedureName)
                .append("(").append(LINE_SEPARATOR)
                .append("\tIN job_id NVARCHAR(64),").append(LINE_SEPARATOR)
                .append("\tIN job_ts TIMESTAMP").append(LINE_SEPARATOR)
                .append(")").append(LINE_SEPARATOR);
        sql.append("LANGUAGE SQLSCRIPT SQL SECURITY INVOKER AS").append(LINE_SEPARATOR);
        sql.append("BEGIN").append(LINE_SEPARATOR);

        //select all the keys
        sql.append("\tselect distinct ");
        for (ProjectTableKey key : application.getKeys()) {
            if (key.isInitial())
                sql.append(key.getKeyName()).append(',');
            else
                sql.append('\'').append(AGGR_PLACEHOLDER).append("',");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(LINE_SEPARATOR).append("\tfrom ");
        sql.append(application.getTableSchema()).append('.').append(application.getTableName()).append(';');

        sql.append(LINE_SEPARATOR).append("END");
        return sql.toString();
    }

    private void createIndex(DbMetadataManager dbMetadataManager, StatementManager statementManager, String tableName,
                             Collection<String> columns) throws SQLException {
        if (columns.size() == 0)
            return;
        final String indexName = createIndexName(tableName, columns);
        log.debug("creating index: " + indexName);
        if (dbMetadataManager.isIndexExists(INTP_GEN_SCHEMA, indexName)) {
            log.trace("skipping creating index, already exists: " + indexName);
            return;
        }
        StringBuilder sql = new StringBuilder();
        sql.append("create index ").append(INTP_GEN_SCHEMA).append('.').append(indexName).append(" on ");
        sql.append(INTP_GEN_SCHEMA).append('.').append(tableName).append(" (").append(LINE_SEPARATOR);
        for (String column : columns) {
            sql.append(column);
            sql.append(',');
        }
        sql.deleteCharAt(sql.length() - 1).append(')');
        final String sqlString = sql.toString();
        dbMetadataManager.executeSql(sqlString);
        statementManager.saveSqlStatement(application.getProjectId(), application.getVersion(), indexName, sqlString, "INDEX");
    }

    /******************************************************************************************************************/

    private void dropDefaultProcedures(DbMetadataManager dbMetadataManager) throws SQLException {
        final String query =
                String.format("delete from %s.RT_LIFECYCLE_JOBS where PROJECT_ID = '%s'", SCHEMA, application.getProjectId());
        dbMetadataManager.executeSql(query);
        dropCleanupTriggerlogProcedure(dbMetadataManager);
        dropArchiveRunLogProcedure(dbMetadataManager);
        dropRestartErrorsProcedure(dbMetadataManager);
        dropEreStatusProcedure(dbMetadataManager);
    }

    private void dropEreStatusProcedure(DbMetadataManager dbMetadataManager) throws SQLException {
        final String name = String.format("%s_ERE_STATUS", application.getProjectId());
        dropStoredProcedure(dbMetadataManager, name, INTP_GEN_SCHEMA);
    }

    private void dropCleanupTriggerlogProcedure(DbMetadataManager dbMetadataManager) throws SQLException {
        final String name = String.format("%s_CLEANUP_TRIGGER_LOG", application.getProjectId());
        dropStoredProcedure(dbMetadataManager, name, INTP_GEN_SCHEMA);
    }

    private void dropArchiveRunLogProcedure(DbMetadataManager dbMetadataManager) throws SQLException {
        final String name = String.format("%s_ARCHIVE_RUN_LOG", application.getProjectId());
        dropStoredProcedure(dbMetadataManager, name, INTP_GEN_SCHEMA);

    }

    private void dropRestartErrorsProcedure(DbMetadataManager dbMetadataManager) throws SQLException {
        final String name = String.format("%s_%s_RESTART_ERRORS", application.getProjectId(), application.getTableName());
        dropStoredProcedure(dbMetadataManager, name, INTP_GEN_SCHEMA);
    }

    private void dropIndex(DbMetadataManager dbMetadataManager, String tableName, Collection<String> columns)
            throws SQLException {
        if (columns.size() == 0)
            return;
        final String indexName = createIndexName(tableName, columns);
        log.debug("dropping index: " + indexName);
        if (!dbMetadataManager.isIndexExists(INTP_GEN_SCHEMA, indexName)) {
            log.trace("skipping dropping index, does not exists: " + indexName);
            return;
        }
        dbMetadataManager.dropIndex(INTP_GEN_SCHEMA, indexName);
    }

    private String createIndexName(String tableName, Collection<String> columns) {
        final StringBuilder nameBuilder = new StringBuilder();
        nameBuilder.append(tableName);
        for (String column : columns) {
            nameBuilder.append('_');
            nameBuilder.append(column);
        }
        nameBuilder.append("_IND");
        return nameBuilder.toString();
    }

    private void dropTrigger(DbMetadataManager dbMetadataManager, String triggerAction) throws SQLException {
        final String triggerName = String.format("%s_%s_%s_TRIGGER", application.getProjectId(),
                application.getTableName(), triggerAction.toUpperCase());
        log.debug("dropping trigger: " + triggerName);
        if (!dbMetadataManager.isTriggerExists(application.getTableSchema(), triggerName))
            return;
        dbMetadataManager.dropTrigger(application.getTableSchema(), triggerName);
    }

    private void dropTriggerTable(DbMetadataManager dbMetadataManager) throws SQLException {
        log.debug("dropping table: " + triggerTableName);
        if (application.getMetadataGenerationMode() == MetadataGenerationMode.SKIP_TRIGGERS) {
            log.debug("skipping trigger table dropping; gen_option == 1");
            return;
        }
        if (!dbMetadataManager.isTableExists(INTP_GEN_SCHEMA, triggerTableName))
            return;
        dbMetadataManager.dropTable(INTP_GEN_SCHEMA, triggerTableName);
    }

    private void dropRunLogTable(DbMetadataManager dbMetadataManager) throws SQLException {
        log.debug("dropping table: " + runLogTableName);

        if (!dbMetadataManager.isTableExists(INTP_GEN_SCHEMA, runLogTableName))
            return;
        dbMetadataManager.dropTable(INTP_GEN_SCHEMA, runLogTableName);
    }

    /**
     * This method drops the runtime info history table.
     *
     * @param dbMetadataManager the Database Metadata Manager.
     */
    private void dropRuntimeArchiveTable(DbMetadataManager dbMetadataManager) throws SQLException {
        log.debug("dropping table: " + runtimeArchiveTableName);

        if (!dbMetadataManager.isTableExists(INTP_GEN_SCHEMA, runtimeArchiveTableName))
            return;
        dbMetadataManager.dropTable(INTP_GEN_SCHEMA, runtimeArchiveTableName);
    }

    /**
     * This method drops the table for the semantic keys statistics.
     *
     * @param dbMetadataManager the Database Metadata Manager.
     */
    private void dropKeyStatisticsTable(DbMetadataManager dbMetadataManager) throws SQLException {
        log.debug("dropping table: " + keyStatisticsTableName);

        if (!dbMetadataManager.isTableExists(INTP_GEN_SCHEMA, keyStatisticsTableName))
            return;
        dbMetadataManager.dropTable(INTP_GEN_SCHEMA, keyStatisticsTableName);
    }

    /**
     * This method drops the key statistics procedure.
     *
     * @param dbMetadataManager the Database Metadata Manager.
     */
    private void dropUpdateKeyStatisticsProcedure(DbMetadataManager dbMetadataManager) throws SQLException {
        dropStoredProcedure(dbMetadataManager, updateKeyStatisticsProcedure, INTP_GEN_SCHEMA);
    }

    private void dropRuntimeView(DbMetadataManager dbMetadataManager) throws SQLException {
        final String view = String.format("%s_RUNTIME_INFO", application.getProjectId());
        dropView(dbMetadataManager, view);
    }

    private void dropLcView(DbMetadataManager dbMetadataManager) throws SQLException {
        final String view = String.format("%s_LIFECYCLE_LOG", application.getProjectId());
        dropView(dbMetadataManager, view);
    }

    private void dropView(DbMetadataManager dbMetadataManager, String view) throws SQLException {
        log.debug("dropping view: " + view);
        if (!dbMetadataManager.isViewExists(INTP_GEN_SCHEMA, view))
            return;
        dbMetadataManager.dropView(INTP_GEN_SCHEMA, view);
    }

    private void dropGetDeltaKeysStoredProcedure(DbMetadataManager dbMetadataManager) throws SQLException {
        dropStoredProcedure(dbMetadataManager, deltaKeysProcedureName, INTP_GEN_SCHEMA);
    }

    private void dropGetInitialKeysStoredProcedure(DbMetadataManager dbMetadataManager) throws SQLException {
        dropStoredProcedure(dbMetadataManager, initialKeysProcedureName, INTP_GEN_SCHEMA);
    }

    private void dropStoredProcedure(DbMetadataManager dbMetadataManager, String procName, String schema) throws SQLException {
        log.debug("dropping procedure: " + procName);
        if (!dbMetadataManager.isProcedureExists(schema, procName))
            return;
        dbMetadataManager.dropProcedure(schema, procName);
    }

    private void dropSequence(DbMetadataManager dbMetadataManager) throws SQLException {
        final String sequence = String.format("%s_%s_SEQ", application.getProjectId(), application.getTableName());
        log.debug("dropping sequence: " + sequence);
        if (!dbMetadataManager.isSequenceExists(INTP_GEN_SCHEMA, sequence))
            return;
        dbMetadataManager.dropSequence(INTP_GEN_SCHEMA, sequence);
    }
}
