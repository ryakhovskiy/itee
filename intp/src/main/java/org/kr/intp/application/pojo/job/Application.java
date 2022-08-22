package org.kr.intp.application.pojo.job;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by kr on 08.12.13.
 */
public class Application {

    private final ApplicationJob[] applicationJobs;
    private final String projectId;
    private final int version;
    private final Properties clientInfo;

    public Application(Project project, ProjectTable[] tables, ProjectTableKey[] keys,
                       ProjectProcedure[] procedures, Properties clientInfo)
            throws SQLException, IOException {
        this.applicationJobs = generateApplicationJobs(project, tables, keys, procedures);
        this.projectId = project.getProjectId();
        this.version = project.getVersion();
        this.clientInfo = clientInfo;
    }

    private ApplicationJob[] generateApplicationJobs(Project project, ProjectTable[] tables, ProjectTableKey[] keys,
                                         ProjectProcedure[] procedures) throws SQLException, IOException {
        final ApplicationJob[] jobs = new ApplicationJob[tables.length];
        int i = 0;
        for (ProjectTable table : tables) {
            final ApplicationJob job = new ApplicationJob(project.getProjectId());
            job.setProject(project);
            job.setTable(table);
            final List<ProjectTableKey> tableKeys = new ArrayList<ProjectTableKey>();
            for (ProjectTableKey ptKey : keys)
                if (ptKey.getTableNumber() == table.getNumber())
                    tableKeys.add(ptKey);
            job.setKeys(tableKeys.toArray(new ProjectTableKey[tableKeys.size()]));
            job.setProcedures(procedures);
            jobs[i++] = job;
        }
        return jobs;
    }

    public ApplicationJob[] getApplicationJobs() {
        return applicationJobs;
    }

    public String getProjectId() {
        return projectId;
    }

    public int getVersion() {
        return version;
    }

    public int getApplicationTablesCount() {
        return applicationJobs.length;
    }

    public Properties getClientInfo() {
        return clientInfo;
    }

    public boolean equals(Object other) {
        if (other == null)
            return false;
        if (other == this)
            return true;
        if (!(other instanceof Application))
            return false;
        final Application otherApp = (Application)other;
        return projectId.equals(otherApp.projectId) && version == otherApp.version;
    }

    public int hashCode() {
        return 11 * version + 13 * projectId.hashCode();
    }

    @Override
    public String toString() {
        final String linesep = System.getProperty("line.separator");
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(linesep).append("PROJECT_ID: ").append(projectId).append("; v. ").append(version).append(linesep);
        for (ApplicationJob job : applicationJobs)
            stringBuilder.append(job).append(linesep);
        return stringBuilder.toString();
    }
}
