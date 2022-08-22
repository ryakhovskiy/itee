package org.kr.intp.util.db.meta;

import org.kr.intp.application.AppContext;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.DataImporter;
import org.kr.intp.util.db.meta.hana.HanaDbMetadataManager;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import org.kr.intp.util.io.QueryReader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by kr on 7/17/2014.
 */
public class IntpMetadataManager {

    private static final Logger SLOG = LoggerFactory.getLogger(IntpMetadataManager.class);

    public static void checkIntpMetadata() {
        try {
            new IntpMetadataManager().checkMetadata();
        } catch (Exception e) {
            SLOG.error("Cannot create In-Time metadata. " + e.getMessage());
            throw new RuntimeException("Cannot create In-Time metadata. " + e.getMessage(), e);
        }
    }

    private final Logger log = SLOG;
    private final String schema;
    private final String genSchema;
    private final String fiCalendarSchema;
    private final String fiCalendarTable;

    private IntpMetadataManager() {
        final IntpConfig config = AppContext.instance().getConfiguration();
        this.schema = config.getIntpSchema();
        this.genSchema = config.getIntpGenObjectsSchema();
        this.fiCalendarSchema = config.getFiscalCalendarSchema();
        this.fiCalendarTable = config.getFiscalCalendarTable();
        preCheck();
    }

    private void preCheck() {
        try {
            try (DbMetadataManager dbMetadataManager = new HanaDbMetadataManager()) {
                checkIntpGenSchema(dbMetadataManager);
                checkCommunicationMethod(dbMetadataManager);
                checkFinCalendarSchema(dbMetadataManager);
                checkFinCalendarTable(dbMetadataManager);
                checkPowerMonField(dbMetadataManager);
            }
        } catch (SQLException e) {
            SLOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void checkMetadata() throws SQLException, IOException {
        DbMetadataManager metadataManager = null;
        Connection connection = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            metadataManager = new HanaDbMetadataManager(connection);
            checkMetadata(metadataManager);
            DataImporter.getDataImporter().checkData(metadataManager);
            checkSchedulerBaseTimeField(metadataManager);
        } finally {
            if (null != metadataManager)
                metadataManager.close();
            if (null != connection)
                connection.close();
        }
    }

    private void checkMetadata(DbMetadataManager metadataManager) throws SQLException, IOException {
        log.debug("checking metadata...");
        handleMdFile(metadataManager, "org/kr/intp/db/md/tables.sql", ObjectType.TABLE);
        handleMdFile(metadataManager, "org/kr/intp/db/md/views.sql", ObjectType.VIEW);
        handleMdFile(metadataManager, "org/kr/intp/db/md/procedures.sql", ObjectType.PROCEDURE);
        handleMdFile(metadataManager, "org/kr/intp/db/md/sequences.sql", ObjectType.SEQUENCE);
    }

    private void handleMdFile(DbMetadataManager metadataManager, String resource, ObjectType type) throws IOException, SQLException {
        for (QueryReader.Query query : new QueryReader(resource)) {
            if (!metadataManager.isObjectExists(schema, query.getName(), type)) {
                log.info(String.format("creating object %s: %s; v. %d", type.toString(), query.getName(), query.getVersion()));
                createNewObject(metadataManager, query, type);
            } else {
                final int newver = query.getVersion();
                final int currver = metadataManager.getMdObjectVersion(query.getName(), type);
                log.debug("Object exists: " + query.getName() + "; v. " + currver + "; new version: " + newver);
                if (currver < newver) {
                    log.debug("re-creating object, old version: " + currver + "; new version: " + newver);
                    if (type == ObjectType.TABLE)
                        renameOldTable(metadataManager, query, currver);
                    else
                        metadataManager.dropObject(schema, query.getName(), type);
                    log.debug("creating object: " + query.getName());
                    createNewObject(metadataManager, query, type);
                }
            }
        }
    }

    private void renameOldTable(DbMetadataManager mdManager, QueryReader.Query table, int currver) throws SQLException {
        final String name = table.getName() + "_" + currver;
        log.debug("keeping old data in the table: " + name);
        if (mdManager.isTableExists(name))
            mdManager.dropTable(name);
        final String sql = "rename table " + schema + '.' + table.getName() + " to " + name;
        mdManager.executeSql(sql);
    }

    private void createNewObject(DbMetadataManager metadataManager, QueryReader.Query query, ObjectType type) throws SQLException {
        metadataManager.executeSql(query.getQuery());
        metadataManager.saveObject(query.getName(), type, query.getVersion());
    }

    /***************************************************************************************************************/

    private void checkCommunicationMethod(DbMetadataManager dbMetadataManager) throws SQLException {
        SLOG.info("checking COMMUNICATION_METHOD field");
        boolean exists = dbMetadataManager.isFieldExists(schema, "RT_SERVER", "COMMUNICATION_METHOD");
        SLOG.debug("field exists: " + exists);
        if (!exists) {
            createField(dbMetadataManager, schema, "RT_SERVER", "COMMUNICATION_METHOD", "int");
            dbMetadataManager.executeUpdate("update " + schema + ".RT_SERVER set COMMUNICATION_METHOD = 1");
            SLOG.trace("COMMUNICATION_METHOD field created. Default communication method is DB");
        }
    }

    private String checkIntpGenSchema(DbMetadataManager dbMetadataManager) throws SQLException {
        final String schema = getIntpGenSchema(dbMetadataManager);
        if (!dbMetadataManager.isSchemaExists(schema))
            dbMetadataManager.executeUpdate("create schema " + schema);
        return schema;
    }

    private String getIntpGenSchema(DbMetadataManager dbMetadataManager) throws SQLException {
        SLOG.info("checking schema for generated objects...");
        SLOG.info("checking GENERATED_OBJECTS_SCHEMA field...");
        boolean exists = dbMetadataManager.isFieldExists(schema, "RT_SERVER", "GENERATED_OBJECTS_SCHEMA");
        SLOG.debug("field found: " + exists);
        if (exists) {
            ResultSet resultSet = null;
            try {
                String generatedObjectsSchemaFromDb = null;
                resultSet = dbMetadataManager.statement.executeQuery("select top 1 GENERATED_OBJECTS_SCHEMA from " +
                        schema + ".RT_SERVER");
                if (resultSet.next()) {
                    generatedObjectsSchemaFromDb = resultSet.getString(1);
                }
                if(generatedObjectsSchemaFromDb == null || !generatedObjectsSchemaFromDb.equals(genSchema)){
                    dbMetadataManager.executeUpdate("update " + schema + ".RT_SERVER set GENERATED_OBJECTS_SCHEMA = '" + genSchema + "'");
                }
                return genSchema;
            } finally {
                if (null != resultSet)
                    resultSet.close();
            }
        } else {
            createField(dbMetadataManager, schema, "RT_SERVER", "GENERATED_OBJECTS_SCHEMA", "nvarchar(128)");
            dbMetadataManager.executeUpdate("update " + schema + ".RT_SERVER set GENERATED_OBJECTS_SCHEMA = '" + genSchema + "'");
            return genSchema;
        }
    }

    private String checkFinCalendarSchema(DbMetadataManager dbMetadataManager) throws SQLException {
        SLOG.info("checking FI_CALENDAR_SCHEMA fields in the RT_SERVER table...");
        boolean exists = dbMetadataManager.isFieldExists(schema, "RT_SERVER", "FI_CALENDAR_SCHEMA");
        SLOG.debug("field exists: " + exists);
        if (!exists) {
            createField(dbMetadataManager, schema, "RT_SERVER", "FI_CALENDAR_SCHEMA", "nvarchar(128)");
            dbMetadataManager.executeUpdate("update " + schema + ".RT_SERVER set FI_CALENDAR_SCHEMA = '" +
                    fiCalendarSchema + "'");
            return fiCalendarSchema;
        } else {
            ResultSet resultSet = null;
            try {
                String fiscalCalendarSchemaFromDb = null;
                resultSet = dbMetadataManager.statement.executeQuery("select top 1 FI_CALENDAR_SCHEMA from "
                        + schema + ".RT_SERVER");
                if (resultSet.next()) {
                    fiscalCalendarSchemaFromDb = resultSet.getString(1);
                }
                if(fiscalCalendarSchemaFromDb == null || !fiscalCalendarSchemaFromDb.equals(fiCalendarSchema)){
                    dbMetadataManager.executeUpdate("update " + schema + ".RT_SERVER set FI_CALENDAR_SCHEMA = '" +
                            fiCalendarSchema + "'");
                }
                return fiCalendarSchema;
            } finally {
                if (null != resultSet)
                    resultSet.close();
            }
        }
    }

    private String checkFinCalendarTable(DbMetadataManager dbMetadataManager) throws SQLException {
        SLOG.info("checking FI_CALENDAR_TABLE fields in the RT_SERVER table...");
        boolean exists = dbMetadataManager.isFieldExists(schema, "RT_SERVER", "FI_CALENDAR_TABLE");
        SLOG.debug("field exists: " + exists);
        if (!exists) {
            createField(dbMetadataManager, schema, "RT_SERVER", "FI_CALENDAR_TABLE", "nvarchar(128)");
            dbMetadataManager.executeUpdate("update " + schema + ".RT_SERVER set FI_CALENDAR_TABLE = '" +
                    fiCalendarTable + "'");
            return fiCalendarTable;
        } else {
            ResultSet resultSet = null;
            try {
                String fiscalCalendarTableFromDb = null;
                resultSet = dbMetadataManager.statement.executeQuery("select top 1 FI_CALENDAR_TABLE from "
                        + schema + ".RT_SERVER");
                if (resultSet.next()) {
                    fiscalCalendarTableFromDb = resultSet.getString(1);
                }
                if(fiscalCalendarTableFromDb == null || !fiscalCalendarTableFromDb.equals(fiCalendarTable)){
                    dbMetadataManager.executeUpdate("update " + schema + ".RT_SERVER set FI_CALENDAR_TABLE = '" +
                            fiCalendarTable + "'");
                }
                return fiCalendarTable;
            } finally {
                if (null != resultSet)
                    resultSet.close();
            }
        }
    }

    private void checkPowerMonField(DbMetadataManager dbMetadataManager) throws SQLException {
        SLOG.info("checking POWER_MON field in the RT_SERVER table...");
        boolean exists = dbMetadataManager.isFieldExists(schema, "RT_SERVER", "POWER_MON");
        SLOG.debug("field exists: " + exists);
        if (!exists) {
            createField(dbMetadataManager, schema, "RT_SERVER", "POWER_MON", "nvarchar(400)");
            dbMetadataManager.executeUpdate("update " + schema + ".RT_SERVER set POWER_MON = '{\"power_mon_enable\":\"false\"}'");
        }
    }

    private void checkSchedulerBaseTimeField(DbMetadataManager dbMetadataManager) throws SQLException {
        SLOG.info("checking SCHEDULER_BASE_TIME field in the RT_ACTIVE_PROJECTS table...");
        boolean exists = dbMetadataManager.isFieldExists(schema, "RT_ACTIVE_PROJECTS", "SCHEDULER_TIME_BASE");
        SLOG.debug("field exists: " + exists);
        if (!exists) {
            createField(dbMetadataManager, schema, "RT_ACTIVE_PROJECTS", "SCHEDULER_TIME_BASE", "TIMESTAMP");
            //dbMetadataManager.executeUpdate("update " + SCHEMA + ".RT_ACTIVE_PROJECTS set SCHEDULER_TIME_BASE = NULL");
        }
    }

    private static void createField(final DbMetadataManager dbMetadataManager, String schema, String table,
                                    String field, String type) throws SQLException {
        SLOG.info("creating field: " + schema + "." + table + "." + field + " " + type);
        dbMetadataManager.executeUpdate("alter table " + schema + "." + table + " add (" + field + " " + type + ")");
    }
}
