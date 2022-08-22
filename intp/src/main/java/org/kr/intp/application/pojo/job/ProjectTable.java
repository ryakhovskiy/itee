package org.kr.intp.application.pojo.job;

/**
 * Created by kr on 08.12.13.
 */
public class ProjectTable {

    private final String projectId;
    private final String name;
    private final int number;

    public ProjectTable(String projectId, String name, int number) {
        this.projectId = projectId;
        this.name = name;
        this.number = number;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getName() {
        return name;
    }

    public int getNumber() {
        return number;
    }
}
