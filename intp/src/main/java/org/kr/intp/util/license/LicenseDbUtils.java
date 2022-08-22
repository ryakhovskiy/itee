package org.kr.intp.util.license;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;

/**
 * Created bykron 07.12.2016.
 */
public class LicenseDbUtils {

    private static final LicenseDbUtils dbutils = new LicenseDbUtils();
    public static LicenseDbUtils getInstance() { return dbutils; }
    private LicenseDbUtils() {}

    public void checkMetadata(Connection connection, String schema) throws SQLException, IOException {
        try (Statement statement = connection.createStatement()) {
            checkSchema(statement, schema);
            checkTable(statement, schema, "RT_SERVER", "org/kr/intp/db/md/rtserver.sql");
            checkTable(statement, schema, "LANDSCAPE", "org/kr/intp/db/md/landscape.sql");
            checkLandscapeRow(connection, schema);
        }
    }

    private void checkSchema(Statement statement, String schema) throws SQLException {
        if (!isSchemaExists(statement, schema))
            createSchema(statement, schema);
    }

    private void checkTable(Statement statement, String schema, String table, String resource) throws SQLException, IOException {
        if (!isTableExists(statement, schema, table))
            createTable(statement, schema, resource);
    }

    public boolean isTableExists(Statement statement, String schema, String table) throws SQLException {
        final String sqlt = "select * from M_TABLES where SCHEMA_NAME = '%s' and TABLE_NAME = '%s'";
        final String sql = String.format(sqlt, schema, table);
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            return resultSet.next();
        }
    }

    public boolean isSchemaExists(Statement statement, String schema) throws SQLException {
        final String sqlt = "select * from SCHEMAS where SCHEMA_NAME = '%s'";
        final String sql = String.format(sqlt, schema);
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            return resultSet.next();
        }
    }

    private void createSchema(Statement statement, String schema) throws SQLException {
        statement.execute("create schema " + schema);
    }

    private void createTable(Statement statement, String schema, String tableResource) throws IOException, SQLException {
        InputStream is = null;
        BufferedReader reader = null;
        try {
            is = Thread.currentThread().getContextClassLoader().getResourceAsStream(tableResource);
            if (null == is)
                throw new IOException("Cannot find resource: rtserver.sql. Please, create table RT_SERVER manually");
            reader = new BufferedReader(new InputStreamReader(is));
            String line;
            StringBuilder sqlBuilder = new StringBuilder();
            while (null != (line = reader.readLine()))
                sqlBuilder.append(line).append('\n');
            int schemaIndex = sqlBuilder.indexOf("$$SCHEMA$$");
            sqlBuilder.replace(schemaIndex, schemaIndex + "$$SCHEMA$$".length(), schema);
            statement.execute(sqlBuilder.toString());
        } finally {
            if (null != is)
                is.close();
            if (null != reader)
                reader.close();
        }
    }

    private void checkLandscapeRow(Connection connection, String schema) throws SQLException {
        final String st = schema + ".LANDSCAPE";
        final String sql = "select * from " + st;
        try (Statement statement = connection.createStatement();
             ResultSet set = statement.executeQuery(sql)) {
            if (set.next())
                return;
        }
        final String isql = "insert into " + st + " (DEV_NAME, DEV_INTP_HOST, QA_NAME, " +
                "QA_INTP_HOST, PROD_NAME, PROD_INTP_HOST) \n" +
                "values (?,?,?,?,?,?)";
        try (PreparedStatement statement = connection.prepareStatement(isql)) {
            statement.setString(1, "DEV_SYS");
            statement.setString(2, "dev_host");
            statement.setString(3, "QA_N/A");
            statement.setString(4, "QA_N/A");
            statement.setString(5, "PROD_N/A");
            statement.setString(6, "PROD_N/A");
            statement.executeUpdate();
        }
    }
}
