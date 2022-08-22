package org.kr.intp.util.db.meta.hana;

import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.meta.DbMetadataManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by kr on 12/24/13.
 */
public class HanaDbMetadataManager extends DbMetadataManager {

    private final Logger log = LoggerFactory.getLogger(HanaDbMetadataManager.class);

    public HanaDbMetadataManager() throws SQLException {
        super();
    }

    public HanaDbMetadataManager(Connection connection) throws SQLException {
        super(connection);
    }

    @Override
    public boolean isViewExists(String view) throws SQLException {
        return isViewExists(schema, view);
    }

    @Override
    public boolean isViewExists(String schema, String view) throws SQLException {
        final String sql = "select VIEW_NAME from VIEWS where SCHEMA_NAME = ? and VIEW_NAME = ?";
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.prepareStatement(sql);
            statement.setString(1, schema);
            statement.setString(2, view);
            resultSet = statement.executeQuery();
            return resultSet.next();
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
        }
    }

    @Override
    public void dropProcedure(String name) throws SQLException {
        dropProcedure(schema, name);
    }

    @Override
    public void dropProcedure(String schema, String name) throws SQLException {
        final String sql = String.format("drop procedure %s.%s", schema, name);
        statement.execute(sql);
    }

    @Override
    public void dropView(String name) throws SQLException {
        dropView(schema, name);
    }

    @Override
    public void dropView(String schema, String name) throws SQLException {
        final String sql = String.format("drop view %s.%s", schema, name);
        statement.execute(sql);
    }

    @Override
    public void dropTrigger(String schema, String trigger) throws SQLException {
        final String sql = String.format("drop trigger %s.%s", schema, trigger);
        statement.execute(sql);
    }

    @Override
    public void dropTable(String table) throws SQLException {
        dropTable(schema, table);
    }

    @Override
    public void dropTable(String schema, String table) throws SQLException {
        final String sql = String.format("drop table %s.%s", schema, table);
        statement.execute(sql);
    }

    @Override
    public void dropSchema(String schema) throws SQLException {
        final String sql = String.format("drop schema %s", schema);
        statement.execute(sql);
    }

    @Override
    public void dropIndex(String schema, String indexName) throws SQLException {
        final String sql = String.format("drop index %s.%s", schema, indexName);
        statement.execute(sql);
    }

    @Override
    public void dropSequence(String schema, String sequence) throws SQLException {
        final String sql = String.format("drop sequence %s.%s", schema, sequence);
        statement.execute(sql);
    }

    @Override
    public boolean isSchemaExists(String schema) throws SQLException {
        final String sql = "select SCHEMA_NAME from SCHEMAS where SCHEMA_NAME = ?";
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.prepareStatement(sql);
            statement.setString(1, schema);
            resultSet = statement.executeQuery();
            return resultSet.next();
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
        }
    }

    @Override
    public boolean isSchemaExists() throws SQLException {
        return isSchemaExists(schema);
    }

    @Override
    public boolean isIndexExists(String schemaName, String indexName) throws SQLException {
        final String sql = "select INDEX_NAME from INDEXES where SCHEMA_NAME = ? and INDEX_NAME = ?";
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.prepareStatement(sql);
            statement.setString(1, schema);
            statement.setString(2, indexName);
            resultSet = statement.executeQuery();
            return resultSet.next();
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
        }
    }

    @Override
    public boolean isSequenceExists(String schema, String sequenceName) throws SQLException {
        final String sql = "select * from SEQUENCES where schema_name = ? and sequence_name = ?";
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.prepareStatement(sql);
            statement.setString(1, schema);
            statement.setString(2, sequenceName);
            resultSet = statement.executeQuery();
            return resultSet.next();
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
        }
    }

    @Override
    public boolean isTableExists(String table) throws SQLException {
        return isTableExists(schema, table);
    }

    @Override
    public boolean isTableExists(String schema, String table) throws SQLException {
        final String sql = "select TABLE_NAME from (" +
                "SELECT TABLE_NAME, SCHEMA_NAME FROM M_TABLES" +
                " UNION ALL SELECT TABLE_NAME, SCHEMA_NAME FROM VIRTUAL_TABLES)" +
                " where SCHEMA_NAME = ? and TABLE_NAME = ?";

        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.prepareStatement(sql);
            statement.setString(1, schema);
            statement.setString(2, table);
            resultSet = statement.executeQuery();
            return resultSet.next();
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
        }
    }

    @Override
    public boolean isFieldExists(String schema, String table, String field) throws SQLException {
        final String sql = "select column_name from table_columns where schema_name = ? and table_name = ? and column_name = ?";
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.prepareStatement(sql);
            statement.setString(1, schema);
            statement.setString(2, table);
            statement.setString(3, field);
            resultSet = statement.executeQuery();
            return resultSet.next();
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
        }
    }

    @Override
    public boolean isTriggerExists(String targetSchema, String trigger) throws SQLException {
        final String sql = "select TRIGGER_NAME from TRIGGERS where SCHEMA_NAME = ? and TRIGGER_NAME = ?";
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.prepareStatement(sql);
            statement.setString(1, targetSchema);
            statement.setString(2, trigger);
            resultSet = statement.executeQuery();
            return resultSet.next();
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
        }
    }

    @Override
    public boolean isProcedureExists(String procedure) throws SQLException {
        return isProcedureExists(schema, procedure);
    }

    @Override
    public boolean isProcedureExists(String schema, String procedure) throws SQLException {
        final String sql = "select PROCEDURE_NAME from PROCEDURES where SCHEMA_NAME = ? and PROCEDURE_NAME = ?";
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            statement = connection.prepareStatement(sql);
            statement.setString(1, schema);
            statement.setString(2, procedure);
            resultSet = statement.executeQuery();
            return resultSet.next();
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
        }
    }
}
