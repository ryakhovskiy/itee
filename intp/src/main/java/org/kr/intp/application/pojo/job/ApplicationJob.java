package org.kr.intp.application.pojo.job;

import org.kr.intp.application.AppContext;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * Created by kr on 08.12.13.
 */
public class ApplicationJob {

    private final Logger log = LoggerFactory.getLogger(ApplicationJob.class);
    private final IntpConfig config = AppContext.instance().getConfiguration();

    private String projectId;
    private String application;
    private int version;
    private String tableSchema;
    private String procSchema;
    private JobTriggerType type;
    private String tableName;
    private String deltaProcedure;
    private String initialProcedure;
    private ProjectTableKey[] keys;
    private JobProperties initialProperties;
    private JobProperties deltaProperties;
    private MetadataGenerationMode mdGenerationMode;

    ApplicationJob(String projectId) {
        this.projectId = projectId;
    }

    void setProject(Project project) throws SQLException, IOException {
        this.projectId = project.getProjectId();
        this.application = project.getApplication();
        this.version = project.getVersion();
        this.tableSchema = project.getTableSchema();
        this.procSchema = project.getProcSchema();
        this.type = project.getType();
        this.mdGenerationMode = project.getMetadataGenerationMode();
        final JobProperties[] props = JobProperties.loadProperties(projectId, version);
        this.initialProperties = props[0];
        this.deltaProperties = props[1];
    }

    void setTable(ProjectTable table) {
        this.tableName = table.getName();
    }

    void setKeys(ProjectTableKey[] keys) {
        this.keys = keys;
    }

    void setProcedures(ProjectProcedure[] procedures) {
        for (ProjectProcedure pp : procedures) {
            switch (pp.getType()) {
                case 'D':
                    this.deltaProcedure = pp.getName();
                    break;
                case 'I':
                    this.initialProcedure = pp.getName();
                    break;
                default:
                    throw new IllegalArgumentException("Type is not supported: " + pp.getType());
            }
        }
    }

    public String getProjectId() {
        return projectId;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public String getProcSchema() {
        return procSchema;
    }

    public JobTriggerType getType() {
        return type;
    }

    public int getVersion() {
        return version;
    }

    public MetadataGenerationMode getMetadataGenerationMode() { return mdGenerationMode; }

    public String getInitialSqlStatement() {
        return getJobProcedureSqlStatement(initialProcedure);
    }

    public String getDeltaSqlStatement() {
        return getJobProcedureSqlStatement(deltaProcedure);
    }

    private String getJobProcedureSqlStatement(String procedure) {
        StringBuilder sql = new StringBuilder();
        sql.append("call \"");
        sql.append(procSchema);
        sql.append("\".\"");
        sql.append(procedure);
        sql.append("\"(?,?,");
        for (int i = 0; i < keys.length; i++)
            sql.append("?,");
        sql.deleteCharAt(sql.length() - 1); //delete last comma
        sql.append(')');
        return sql.toString();
    }

    public String getDeltaKeysSqlStatement() {
        return getKeysSqlStatement(getDeltaKeysProcedureFullName());
    }

    public String getInitialKeysSqlStatement() {
        if (!initialProperties.isFullload()) {
            final String sql = getKeysSqlStatement(getInitialKeysProcedureFullName());
            log.debug("full load disabled: " + sql);
            return sql;
        }
        else {
            final String sql = getFullKeysSqlStatement();
            log.debug("full load enabled: " + sql);
            return sql;
        }
    }

    private String getFullKeysSqlStatement() {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select distinct ");
        for (ProjectTableKey k : keys) {
            if (k.isInitial())
                sqlBuilder.append(k.getKeyName());
            else
                sqlBuilder.append("'").append(config.getAggregationPlaceholder()).append("'");
            sqlBuilder.append(",");
        }
        sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
        sqlBuilder.append(" from ").append(tableSchema).append('.').append(tableName);
        final String restrictions = initialProperties.getFullLoadRestrictions();
        if (restrictions.length() > 0)
            sqlBuilder.append(" where ").append(restrictions);
        return sqlBuilder.toString();
    }

    private String getKeysSqlStatement(String fullProcedureName) {
        StringBuilder sql = new StringBuilder();
        sql.append("call ");
        sql.append(fullProcedureName);
        sql.append("(?, CURRENT_UTCTIMESTAMP)");
        return sql.toString();
    }

    public String getInitialKeysProcedureName() {
        return getKeysProcedureName("INITIAL");
    }

    public String getInitialKeysProcedureFullName() {
        return getKeysProcedureFullName("INITIAL");
    }

    public String getInitialProcedure() {
        return initialProcedure;
    }

    public String getDeltaProcedure() {
        return deltaProcedure;
    }

    public String getDeltaKeysProcedureName() {
        return getKeysProcedureName("DELTA");
    }

    public String getDeltaKeysProcedureFullName() {
        return getKeysProcedureFullName("DELTA");
    }

    private String getKeysProcedureName(String type) {
        return String.format("%s_%s_GET_%s_KEYS", projectId.toUpperCase(), tableName.toUpperCase(), type.toUpperCase());
    }

    private String getKeysProcedureFullName(String type) {
        return String.format("\"%s\".\"%s_%s_GET_%s_KEYS\"", config.getIntpGenObjectsSchema().toUpperCase(),
                projectId.toUpperCase(), tableName.toUpperCase(), type.toUpperCase());
    }

    public int getTotalKeysCount() {
        return keys.length;
    }

    public ProjectTableKey[] getKeys() {
        return Arrays.copyOf(keys, keys.length);
    }

    public JobProperties getInitialProperties() {
        return initialProperties;
    }

    public JobProperties getDeltaProperties() {
        return deltaProperties;
    }

    @Override
    public String toString() {
        final String linesep = System.getProperty("line.separator");
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("T_SCHEMA: ").append(tableSchema).append("; P_SCHEMA: ").append(procSchema).append("; TABLE: ");
        stringBuilder.append(tableName).append(linesep);
        stringBuilder.append("DELTA_PROC: ").append(deltaProcedure).append("; INIT_PROC: ").append(initialProcedure).append(linesep);
        for (ProjectTableKey key : keys)
            stringBuilder.append(key).append(linesep);
        stringBuilder.append("DELTA: ").append(deltaProperties).append(linesep).append("INIT: ").append(initialProperties);
        if (initialProperties.isFullload())
            stringBuilder.append(linesep).append("FULL_INITIAL_LOAD by querying: ").append(linesep).append(getInitialKeysSqlStatement());
        return stringBuilder.toString();
    }

    public enum JobType {
        DELTA("D"),
        INITIAL("I");

        private final String type;
        JobType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }
}
