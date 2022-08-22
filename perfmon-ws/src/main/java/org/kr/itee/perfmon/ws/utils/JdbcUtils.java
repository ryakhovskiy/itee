package org.kr.itee.perfmon.ws.utils;

import org.kr.itee.perfmon.ws.conf.ConfigurationManager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by kr on 5/15/2016.
 */
public class JdbcUtils {

    private static final String SCHEMA = ConfigurationManager.instance().getSchema();

    public static void checkStoredProcedure(Connection connection) throws IOException, SQLException {
        final String resource =
                IOUtils.getInstance().getResourceAsString("sp_performance_info.sql");
        final String sql = resource.replace("$$SCHEMA$$", SCHEMA);
        Statement statement = null;
        try {
            statement = connection.createStatement();
            if (!isSchemaExists(statement, SCHEMA))
                statement.execute("create schema " + SCHEMA);
            if (!isProcedureExists(statement, SCHEMA, "SP_PERFORMANCE_INFO"))
                statement.execute(sql);
        } finally {
            if (null != statement)
                statement.close();
        }
    }

    //TODO: use safe parameters
    private static boolean isSchemaExists(Statement statement, String schema) throws SQLException {
        final String sql = "select schema_name from schemas where schema_name = '" + schema + "'";
        return isNotEmptyResultSet(statement, sql);
    }

    //TODO: use safe parameters
    private static boolean isProcedureExists(Statement statement, String schema, String procedure) throws SQLException {
        final String sql = "select procedure_name from PROCEDURES where schema_name = '" + schema +
                "' and procedure_name = '" + procedure + "'";
        return isNotEmptyResultSet(statement, sql);
    }

    private static boolean isNotEmptyResultSet(Statement statement, String sql) throws SQLException {
        try (ResultSet resultSet = statement.executeQuery(sql)) {
            return resultSet.next();
        }
    }

}
