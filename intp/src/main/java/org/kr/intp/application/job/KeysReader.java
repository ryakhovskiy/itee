package org.kr.intp.application.job;

import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.IDataSource;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KeysReader {

    private final Logger log = LoggerFactory.getLogger(KeysReader.class);
    private final boolean isTraceEnabled = log.isTraceEnabled();
    private final IDataSource dataSource;
    private String keysSqlStatement;
    private final int keysCount;
    private final String projectId;

    public KeysReader(String projectId, IDataSource dataSource, String keysSqlStatement, int keysCount) {
        this.projectId = projectId;
        this.keysSqlStatement = keysSqlStatement;
        this.dataSource = dataSource;
        this.keysCount = keysCount + 1;
    }

    public List<Object[]> getKeys(String uuid) throws SQLException {
        Connection connection = null;
        CallableStatement statement = null;
        try {
            connection = getConnection();
            statement = getStatement(connection, uuid);
            return getKeys(statement);
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private List<Object[]> getKeys(CallableStatement keysStatement) throws SQLException {
        long start = System.currentTimeMillis();
        ResultSet set = null;
        List<Object[]> keysList = new ArrayList<Object[]>();
        try {
            keysStatement.execute();
            set = keysStatement.getResultSet();
            while (set.next()) {
                int rsLength = set.getMetaData().getColumnCount();
                Object[] keys = new Object[rsLength];
                for (int i = 0; i < rsLength; i++)
                    keys[i] = set.getObject(i + 1);
                keysList.add(keys);
            }
        } finally {
            if (null != set)
                set.close();
        }
        long end = System.currentTimeMillis();
        if (isTraceEnabled && keysList.size() > 0) {
            log.debug(projectId + " reading keys took (ms): " + (end - start) + "; keys count: " + keysList.size());
            StringBuilder keys = new StringBuilder();
            for (Object[] k : keysList)
                keys.append(Arrays.toString(k));
            log.trace(projectId + " Keys retrieved from DB: {" + keys + '}');
        }
        return keysList;
    }


    private Connection getConnection() throws SQLException {
        long start = System.currentTimeMillis();
        Connection connection = dataSource.getConnection();
        long end = System.currentTimeMillis();
        if (isTraceEnabled)
            log.trace(projectId + " Retrieving connection took (ms): " + (end - start));
        return connection;
    }

    private CallableStatement getStatement(Connection connection, String uuid) throws SQLException {
        long start = System.currentTimeMillis();
        CallableStatement statement = connection.prepareCall(keysSqlStatement); //statement should be pooled
        // Only executed if full load is not enabled
        if(keysSqlStatement.contains("?")){
            statement.setString(1, uuid);
        }
        long end = System.currentTimeMillis();
        if (isTraceEnabled)
            log.trace(projectId + " Preparing statement took (ms): " + (end - start));
        return statement;
    }

    public String getKeysSqlStatement() {
        return keysSqlStatement;
    }
}
