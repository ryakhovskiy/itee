package org.kr.intp.util.db.meta;

import org.kr.intp.application.AppContext;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by kr on 12/24/13.
 */
public abstract class DbMetadataManager implements AutoCloseable {

    protected final Connection connection;
    protected final Statement statement;
    protected final String schema = AppContext.instance().getConfiguration().getIntpSchema();
    private final Logger log = LoggerFactory.getLogger(DbMetadataManager.class);
    private final boolean connectionSelfCreated;
    private final AtomicInteger sqlCounter = new AtomicInteger(0);
    private final Map<Integer, PreparedStatement> statements = new HashMap<Integer, PreparedStatement>();

    public DbMetadataManager() throws SQLException {
        this(ServiceConnectionPool.instance().getConnection(), true);
    }

    public DbMetadataManager(Connection connection) throws SQLException {
        this(connection, false);
    }

    private DbMetadataManager(Connection connection, boolean connectionSelfCreated) throws SQLException {
        this.connection = connection;
        this.connectionSelfCreated = connectionSelfCreated;
        this.statement = connection.createStatement();
    }

    public abstract boolean isTableExists(String table) throws SQLException;
    public abstract boolean isTableExists(String schema, String table) throws SQLException;
    public abstract boolean isFieldExists(String schema, String table, String field) throws SQLException;
    public abstract boolean isSchemaExists(String schema) throws SQLException;
    public abstract boolean isSchemaExists() throws SQLException;
    public abstract boolean isTriggerExists(String schema, String trigger) throws SQLException;
    public abstract boolean isProcedureExists(String procedure) throws SQLException;
    public abstract boolean isProcedureExists(String schema, String name) throws SQLException;
    public abstract boolean isViewExists(String view) throws SQLException;
    public abstract boolean isViewExists(String schema, String view) throws SQLException;
    public abstract boolean isIndexExists(String schema, String indexName) throws SQLException;
    public abstract boolean isSequenceExists(String schema, String sequenceName) throws SQLException;
    public abstract void dropProcedure(String name) throws SQLException;
    public abstract void dropProcedure(String schema, String name) throws SQLException;
    public abstract void dropView(String view) throws SQLException;
    public abstract void dropView(String schema, String name) throws SQLException;
    public abstract void dropTrigger(String schema, String trigger) throws SQLException;
    public abstract void dropTable(String table) throws SQLException;
    public abstract void dropTable(String schema, String table) throws SQLException;
    public abstract void dropSchema(String schema) throws SQLException;
    public abstract void dropIndex(String schema, String indexName) throws SQLException;
    public abstract void dropSequence(String schema, String sequence) throws SQLException;

    public int getRecordsCount(String schema, String table) throws SQLException {
        ResultSet rs = null;
        try {
            rs = statement.executeQuery("select count(*) from " + schema + '.' + table);
            if (!rs.next())
                return 0;
            return rs.getInt(1);
        } finally {
            if (null != rs)
                rs.close();
        }
    }

    public void dropObject(String schema, String name, ObjectType type) throws SQLException {
        if (log.isTraceEnabled())
            log.trace(String.format("dropping object: schema: %s; name: %s; type: %s", schema, name, type.getTypeAsString()));
        switch (type) {
            case INDEX:
                dropIndex(schema, name);
                break;
            case TABLE:
                dropTable(schema, name);
                break;
            case TRIGGER:
                dropTrigger(schema, name);
                break;
            case PROCEDURE:
                dropProcedure(schema, name);
                break;
            case VIEW:
                dropView(schema, name);
                break;
            case SEQUENCE:
                dropSequence(schema, name);
                break;
            case STATEMENT:
                break;
            default:
                throw new UnsupportedOperationException("Object Type is not supported: " + type);
        }
    }

    public boolean isObjectExists(String schema, String name, ObjectType type) throws SQLException {
        switch (type) {
            case INDEX:
                return isIndexExists(schema, name);
            case TABLE:
                return isTableExists(schema, name);
            case TRIGGER:
                return isTriggerExists(schema, name);
            case PROCEDURE:
                return isProcedureExists(schema, name);
            case VIEW:
                return isViewExists(schema, name);
            case SEQUENCE:
                return isSequenceExists(schema, name);
            case STATEMENT:
                return false;
            default:
                throw new UnsupportedOperationException("Object Type is not supported: " + type);
        }
    }

    public int getMdObjectVersion(String name, ObjectType type) throws SQLException {
        final String sql = "select version from " + schema + ".MD_REPOSITORY where name = ? and type = ?";
        ResultSet set = null;
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(sql);
            statement.setString(1, name);
            statement.setString(2, type.getTypeAsString());
            statement.execute();
            set = statement.getResultSet();
            int version = 0;
            if (set.next())
                version = set.getInt(1);
            return version;
        } finally {
            if (null != set)
                set.close();
            if (null != statement)
                statement.close();
        }
    }

    public void executeSql(String sql) throws SQLException {
        statement.execute(sql);
    }

    public void executeUpdate(String sql) throws SQLException {
        statement.executeUpdate(sql);
    }

    public void saveObject(String name, ObjectType type, int version) throws SQLException {
        deleteMdRow(name, type.getTypeAsString());
        addMdRow(name, type.getTypeAsString(), version);
    }

    private void deleteMdRow(String name, String type) throws SQLException {
        PreparedStatement stmnt = null;
        try {
            final String sql = "delete from " + schema + ".MD_REPOSITORY where name = ? and type = ?";
            stmnt = connection.prepareStatement(sql);
            stmnt.setString(1, name);
            stmnt.setString(2, type);
            stmnt.execute();
        } finally {
            if (null != stmnt)
                stmnt.close();
        }
    }

    private void addMdRow(String name, String type, int version) throws SQLException {
        PreparedStatement addStmnt = null;
        try {
            final String addsql = "insert into " + schema + ".MD_REPOSITORY (name, type, version) values (?,?,?)";
            addStmnt = connection.prepareStatement(addsql);
            addStmnt.setString(1, name);
            addStmnt.setString(2, type);
            addStmnt.setInt(3, version);
            addStmnt.execute();
        } finally {
            if (null != addStmnt)
                addStmnt.close();
        }
    }

    public int prepareSql(String sql) throws SQLException {
        final PreparedStatement ps = connection.prepareStatement(sql);
        final int id = sqlCounter.getAndIncrement();
        statements.put(id, ps);
        return id;
    }

    public void closePrepared(int sqlId) {
        if (!statements.containsKey(sqlId))
            return;
        final PreparedStatement ps = statements.remove(sqlId);
        if (null == ps)
            return;
        closeStatement(ps);
    }

    public void executePrepared(int sqlId, Object... params) throws SQLException {
        final PreparedStatement ps = statements.get(sqlId);
        if (null == ps)
            throw new IllegalArgumentException("Cannot find prepared statement with id: " + sqlId);
        for (int i = 0; i < params.length; i++) {
            final Object param = params[i];
            if (null == param || param.toString().equals("<NULL>"))
                ps.setObject(i + 1, null);
            else
                ps.setObject(i + 1, params[i]);
        }
        ps.execute();
    }

    public void close() {
        if (connectionSelfCreated) {
            closeStatement(statement);
            for (PreparedStatement ps : statements.values())
                closeStatement(ps);
            statements.clear();
            closeConnection(connection);
        }
    }

    private void closeStatement(Statement statement) {
        if (null != statement) {
            try {
                statement.close();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private void closeConnection(Connection connection) {
        if (null != connection) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
