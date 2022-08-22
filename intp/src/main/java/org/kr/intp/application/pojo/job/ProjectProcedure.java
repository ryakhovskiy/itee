package org.kr.intp.application.pojo.job;

/**
 * Created by kr on 08.12.13.
 */
public class ProjectProcedure {

    private final String projectId;
    private final int number;
    private final char type;
    private final String name;

    public ProjectProcedure(String projectId, int number, char type, String name) {
        this.projectId = projectId;
        this.number = number;
        this.type = type;
        this.name = name;
    }

    public String getProjectId() {
        return projectId;
    }

    public int getNumber() {
        return number;
    }

    public char getType() {
        return type;
    }

    public String getName() {
        return name;
    }
}
