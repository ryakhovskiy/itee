package org.kr.intp.util.db.meta;

import org.kr.intp.application.AppContext;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.*;

/**
 * Created by kr on 12/24/13.
 */
public abstract class StatementManager {

    private static final boolean SAVE_DDL = true;
    private static final boolean REMOVE_DDL = false;  //to keep version control possibilities
    private final Logger log = LoggerFactory.getLogger(StatementManager.class);
    private final Connection connection;
    private final PreparedStatement insertStmnt;
    private final PreparedStatement updateStmnt;
    private final PreparedStatement deleteStmnt;
    private final PreparedStatement isExistsStmnt;
    private final boolean connectionSelfCreated;

    protected StatementManager() throws SQLException {
        this(ServiceConnectionPool.instance().getConnection(), true);
    }

    protected StatementManager(Connection connection) throws SQLException {
        this(connection, false);
    }

    private StatementManager(Connection connection, boolean connectionSelfCreated) throws SQLException {
        this.connection = connection;
        this.connectionSelfCreated = connectionSelfCreated;
        final String schema = AppContext.instance().getConfiguration().getIntpSchema();
        isExistsStmnt = connection.prepareStatement(String.format("select NAME from %s.RT_STATEMENTS where PROJECT_ID = ? and NAME = ? and VERSION = ?", schema));
        insertStmnt = connection.prepareStatement(String.format("insert into %s.RT_STATEMENTS (PROJECT_ID, NAME, VERSION, STATEMENT, OBJECT_TYPE) values (?, ?, ?, ?, ?)", schema));
        updateStmnt = connection.prepareStatement(String.format("update %s.RT_STATEMENTS set STATEMENT = ? where PROJECT_ID = ? and NAME = ? and VERSION = ?", schema));
        deleteStmnt = connection.prepareStatement(String.format("delete from %s.RT_STATEMENTS where PROJECT_ID = ? and VERSION = ?", schema));
    }

    public void saveSqlStatement(String projectId, int version, String name, String sql, String objectType) throws SQLException {
        if (!SAVE_DDL)
            return;
        if (isStatementSaved(projectId, version, name))
            updateSqlStatement(projectId, version, name, sql);
        else
            insertSqlStatement(projectId, version, name, sql, objectType);
    }

    private void insertSqlStatement(String projectId, int version, String name, String sql, String objectType) throws SQLException {
        insertStmnt.setString(1, projectId);
        insertStmnt.setString(2, name);
        insertStmnt.setInt(3, version);
        insertStmnt.setString(4, sql);
        insertStmnt.setString(5, objectType);
        insertStmnt.execute();
    }

    private void updateSqlStatement(String projectId, int version, String name, String sql) throws SQLException {
        updateStmnt.setString(1, sql);
        updateStmnt.setString(2, projectId);
        updateStmnt.setString(3, name);
        updateStmnt.setInt(4, version);
        updateStmnt.execute();
    }

    public void deleteSqlStatements(String projectId, int version) throws SQLException {
        if (!REMOVE_DDL)
            return;
        deleteStmnt.setString(1, projectId);
        deleteStmnt.setInt(2, version);
        deleteStmnt.execute();
    }

    private boolean isStatementSaved(String projectId, int version, String name) throws SQLException {
        ResultSet resultSet = null;
        try {
            isExistsStmnt.setString(1, projectId);
            isExistsStmnt.setString(2, name);
            isExistsStmnt.setInt(3, version);
            resultSet = isExistsStmnt.executeQuery();
            return resultSet.next();
        } finally {
            if (null != resultSet)
                resultSet.close();
        }
    }

    public void close() {
        if (connectionSelfCreated) {
            closeStmnt(insertStmnt);
            closeStmnt(updateStmnt);
            closeStmnt(deleteStmnt);
            closeStmnt(isExistsStmnt);
            if (null != connection) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    private void closeStmnt(Statement statement) {
        if (null != statement)
            try {
                statement.close();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
    }
}
