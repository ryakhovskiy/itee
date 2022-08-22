package org.kr.intp.application.pojo.job;

/**
 * Created by kr on 08.12.13.
 */
public class Project {

    private final String projectId;
    private final JobTriggerType type;
    private final int version;
    private final String application;
    private final String description;
    private final String tableSchema;
    private final String procSchema;
    private final MetadataGenerationMode mdGenerationMode;

    public Project(String projectId, String type, int version, String application, String description, String tableSchema,
                   String procSchema, MetadataGenerationMode mode) {
        this.projectId = projectId;
        this.type = type == null || type.toLowerCase().startsWith("schedul") ?
                JobTriggerType.SCHEDULE : JobTriggerType.EVENT;
        this.version = version;
        this.application = application;
        this.description = description;
        this.tableSchema = tableSchema;
        this.procSchema = procSchema;
        this.mdGenerationMode = this.type == JobTriggerType.EVENT ? mode : MetadataGenerationMode.SKIP_TRIGGERS;
    }

    public String getProjectId() {
        return projectId;
    }

    public JobTriggerType getType() {
        return type;
    }

    public int getVersion() {
        return version;
    }

    public String getApplication() {
        return application;
    }

    public String getDescription() {
        return description;
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public String getProcSchema() {
        return procSchema;
    }

    public MetadataGenerationMode getMetadataGenerationMode() { return mdGenerationMode; }
}
