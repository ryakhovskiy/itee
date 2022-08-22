package org.kr.db.loader.ui.monitor;

import org.kr.db.loader.ui.conf.GlobalConfiguration;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by kr on 6/27/2014.
 */
public class ExpensiveStatementsMonitor implements IMonitor {

    private static final String HEADERS = "STATEMENT\tUSER\tSCHEMA\tAVG_TIME\tMAX_TIME\tMIN_TIME";
    private static final String EXPENSIVE_STATEMENTS = "select STATEMENT_STRING, USER_NAME, SCHEMA_NAME, \n" +
            "\tROUND(AVG_EXECUTION_TIME / 1000, 0) AS AVG_TIME, \n" +
            "\tROUND(MAX_EXECUTION_TIME / 1000, 0) AS MAX_TIME,\n" +
            "\tROUND(MIN_EXECUTION_TIME / 1000, 0) AS MIN_TIME\n" +
            " from M_SQL_PLAN_CACHE where AVG_EXECUTION_TIME > ";

    private final List<List<String>> results = new ArrayList<>();
    private final String url;
    private final String expensiveStatementQuery;
    private final Object resultsGuard = new Object();
    private final CountDownLatch startGate;
    private final long monitor_sleep_ms;
    private volatile boolean done = false;

    private Connection connection;
    private PreparedStatement expensiveQueryStatement;

    public ExpensiveStatementsMonitor(String url, CountDownLatch startGate, long monitor_sleep_ms,
                                      long expensive_length_ms) throws SQLException {
        this.url = url;
        this.monitor_sleep_ms = monitor_sleep_ms;
        this.startGate = startGate;
        this.expensiveStatementQuery = EXPENSIVE_STATEMENTS + TimeUnit.MILLISECONDS.toMicros(expensive_length_ms);
    }

    private void init() throws SQLException {
        this.connection = DriverManager.getConnection(url);
        this.expensiveQueryStatement = connection.prepareStatement(expensiveStatementQuery);
    }

    @Override
    public List<String> getResults() {
        synchronized (resultsGuard) {
            if (results.size() == 0)
                return Collections.singletonList("No data");
            else {
                final List<String> data = results.get(results.size() - 1);
                data.add(0, HEADERS);
                data.add("");
                return data;
            }
        }
    }

    @Override
    public void shutdown() {
        this.done = true;
    }

    @Override
    public void run() {
        try {
            init();
            while (!done) {
                startGate.await();
                monitor();
                Thread.sleep(monitor_sleep_ms);
            }
        } catch (Exception e) {
          addResult(Collections.singletonList(e.getMessage()));
        } finally {
            close();
        }
    }

    private void addResult(List<String> data) {
        synchronized (resultsGuard) {
            results.add(data);
        }
    }

    private void close() {
        try {
            expensiveQueryStatement.close();
            connection.close();
        } catch (SQLException e) {
            addResult(Collections.singletonList(e.getMessage()));
        }
    }

    private void monitor() throws SQLException {
        final List<String> data = new ArrayList<String>();
        ResultSet resultSet = null;
        try {
            resultSet = expensiveQueryStatement.executeQuery();
            while (resultSet.next()) {
                final StringBuilder resultsBuilder = new StringBuilder();
                resultsBuilder.append(resultSet.getString(1)).append("\t");
                resultsBuilder.append(resultSet.getString(2)).append("\t");
                resultsBuilder.append(resultSet.getString(3)).append("\t");
                resultsBuilder.append(resultSet.getString(4)).append("\t");
                resultsBuilder.append(resultSet.getString(5)).append("\t");
                resultsBuilder.append(resultSet.getString(5));
                data.add(resultsBuilder.toString());
            }
        } finally {
            if (null != resultSet)
                resultSet.close();
        }
        if (GlobalConfiguration.keepStatsInMemory)
            addResult(data);
    }
}
