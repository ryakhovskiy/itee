package org.kr.intp.application.pojo.job;

/**
 * Created by kr on 08.12.13.
 */
public class ProjectTableKey {

    private final String projectId;
    private final int number;
    private final int tableNumber;
    private final String tableName;
    private final String keyName;
    private final ProjectKeyType keyType;
    private final String keyDbType;
    private final int threshold;
    private final boolean isInitial;
    private final boolean isSummable;
    private final boolean isSequential;


    public ProjectTableKey(String projectId, int number, int tableNumber, String tableName, String keyName,
                           ProjectKeyType keyType, String dbType, int threshold, boolean isInitial, boolean isSummable,
                           boolean isSequential) {
        this.projectId = projectId;
        this.number = number;
        this.tableNumber = tableNumber;
        this.tableName = tableName;
        this.keyName = keyName;
        this.keyType = keyType;
        this.keyDbType = dbType;
        this.threshold = threshold;
        this.isInitial = isInitial;
        this.isSummable = isSummable;
        this.isSequential = isSequential;
    }

    public int getNumber() {
        return number;
    }

    public String getProjectId() {
        return projectId;
    }

    public int getTableNumber() {
        return tableNumber;
    }

    public String getTableName() {
        return tableName;
    }

    public String getKeyName() {
        return keyName;
    }

    public ProjectKeyType getKeyType() {
        return keyType;
    }

    public String getKeyDbType() {
        return keyDbType;
    }

    public int getThreshold() {
        return threshold;
    }

    public boolean isInitial() {
        return isInitial;
    }

    public boolean isSummable() {
        return isSummable;
    }

    public boolean isSequential() { return isSequential; }
    @Override
    public String toString() {
        return String.format("N: %d; TN: %d; TD: %s; KN: %s; DBT: %s, THD: %d; I: %b; S: %b; Sq: %b", number,
                tableNumber, tableName, keyName, keyDbType, threshold, isInitial, isSummable, isSequential);
    }
}