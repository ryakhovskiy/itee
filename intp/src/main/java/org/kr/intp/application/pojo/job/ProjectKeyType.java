package org.kr.intp.application.pojo.job;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 9/9/13
 * Time: 11:29 PM
 * To change this template use File | Settings | File Templates.
 */
public enum ProjectKeyType {

    TINYINT,
    SMALLINT,
    INT,
    BIGINT,
    STRING,
    DATE,
    TIME,
    TIMESTAMP,
    DOUBLE,
    REAL,
    FLOAT,
    DECIMAL;

    public static ProjectKeyType convert(String type) {
        type = type.toUpperCase();
        if (type.startsWith("NVARCHAR"))
            return ProjectKeyType.STRING;
        else if (type.startsWith("VARCHAR"))
            return ProjectKeyType.STRING;
        else if (type.startsWith("CHAR"))
            return ProjectKeyType.STRING;
        else if (type.startsWith("NCHAR"))
            return ProjectKeyType.STRING;
        else if (type.equals("INT"))
            return ProjectKeyType.INT;
        else if (type.equals("INTEGER"))
            return ProjectKeyType.INT;
        else if (type.equals("TINYINT"))
            return ProjectKeyType.TINYINT;
        else if (type.equals("SMALLINT"))
            return ProjectKeyType.SMALLINT;
        else if (type.equals("BIGINT"))
            return ProjectKeyType.BIGINT;
        else if (type.equals("DATE"))
            return ProjectKeyType.DATE;
        else if (type.equals("TIME"))
            return ProjectKeyType.TIME;
        else if (type.equals("TIMESTAMP"))
            return ProjectKeyType.TIMESTAMP;
        else if (type.equals("DOUBLE"))
            return ProjectKeyType.DOUBLE;
        else if (type.equals("REAL"))
            return ProjectKeyType.REAL;
        else if (type.equals("FLOAT"))
            return ProjectKeyType.FLOAT;
        else if (type.equals("DECIMAL"))
            return ProjectKeyType.DECIMAL;

        throw new IllegalArgumentException("Type is not supported: " + type);
    }

}
