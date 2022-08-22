package org.kr.db.loader;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.concurrent.BlockingQueue;

/**
 * Created by kr on 21.01.14.
 */
public class SqlExecutor implements Runnable {

    private static final DbQueryType DB_QUERY_TYPE = AppConfig.getInstance().getQueryType();
    private final Logger log = Logger.getLogger(SqlExecutor.class);
    private final BlockingQueue<String> queryQueue;
    private final ConnectionManager connectionManager;
    private final MqLogger mqLogger;

    public SqlExecutor(BlockingQueue<String> queryQueue, ConnectionManager connectionManager, MqLogger mqLogger)
            throws SQLException {
        this.queryQueue = queryQueue;
        this.connectionManager = connectionManager;
        this.mqLogger = mqLogger;
    }

    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                performSqlExecutorJob();
            } catch (InterruptedException e) {
                log.debug("interrupted");
                Thread.currentThread().interrupt();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private void performSqlExecutorJob() throws InterruptedException, SQLException {
        final String sql = queryQueue.take();
        log.debug(sql);
        long start = AppMain.currentTimeMillis();
        switch (DB_QUERY_TYPE) {
            case CALL:
                executeCall(sql);
                break;
            case QUERY:
                executeQuery(sql);
                break;
            default:
                log.error("Wrong query type");
                break;
        }
        if (null != mqLogger) {
            long end = AppMain.currentTimeMillis();
            int size = queryQueue.size();
            mqLogger.log(start, end, size, sql);
        }
    }

    private void executeCall(String sql) throws SQLException {
        Connection connection = null;
        CallableStatement statement = null;
        try {
            connection = connectionManager.getConnection();
            statement = connection.prepareCall(sql);
            statement.execute();
        } finally {
            if (null != connection)
                connection.close();
            if (null != statement)
                statement.close();
        }
    }

    private void executeQuery(String sql) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = connectionManager.getConnection();
            statement = connection.prepareStatement(sql);
            statement.execute();
        } finally {
            if (null != connection)
                connection.close();
            if (null != statement)
                statement.close();
        }
    }
}
