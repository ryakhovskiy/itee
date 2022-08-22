package org.kr.itee.perfmon.ws.monitor;

import org.kr.itee.perfmon.ws.conf.ConfigurationManager;
import org.kr.itee.perfmon.ws.conf.GlobalConfiguration;
import org.kr.itee.perfmon.ws.utils.MathUtils;
import org.apache.log4j.Logger;

import java.sql.*;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by kr on 7/20/2014.
 */
public class VPerformanceMonitor implements IMonitor {

    private static final int HANA_USERS_LIMIT = 65536;
    private static final String LINESEP = System.getProperty("line.separator");

    private final Logger log = Logger.getLogger(VPerformanceMonitor.class);
    private final Object resultsGuard = new Object();
    private final List<String> results = new ArrayList<String>();
    private volatile boolean done = false;
    private final long sleep_ms;
    private final String url;
    private Connection connection;
    private CallableStatement statement;
    private final List<Long> memoryBytes = new ArrayList<Long>();
    private final List<Double> memoryGBytes = new ArrayList<Double>();
    private final List<Integer> cpus = new ArrayList<Integer>();
    private final List<Integer> updateProcesses = new ArrayList<Integer>();
    private final List<Integer> queryProcesses = new ArrayList<Integer>();
    private final List<Timestamp> timestamps = new ArrayList<Timestamp>();
    private final List<Timestamp> utctimestamps = new ArrayList<Timestamp>();
    private final Map<String, List<Measurements>> userMeasurements = new HashMap<String, List<Measurements>>();
    private final List<Long> alltimes = new ArrayList<Long>();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final int age_seconds;
    private final String colName;
    private RunLogMonitor runLogMonitor;
    private boolean isProcessingMonitorEnabled = false;

    public VPerformanceMonitor(String url, long sleep_ms, String colName, int age_seconds) {
        this.colName = colName;
        this.age_seconds = age_seconds;
        this.sleep_ms = sleep_ms;
        this.url = url;
    }

    @Override
    public void run() {
        try {
            prepareJdbcObjects();
            while (!done)
                monitor();
        } catch (InterruptedException e) {
            log.info("VPerfMon has been interrupted");
        } catch (Exception e) {
            log.error(e);
        } finally {
            close();
        }
    }

    private void monitor() throws SQLException, InterruptedException {
        ResultSet resultSet = null;
        Future<Long> future = null;
        try {
            if (isProcessingMonitorEnabled)
                future = executor.submit(runLogMonitor);
            statement.execute();
            resultSet = statement.getResultSet();
            int totalUsers = 0;
            int cpu = 0;
            double dmem = 0;
            double dmemMax = 0;
            long mem = 0;
            long memMax = 0;
            Timestamp timestamp = null;
            Timestamp utctimestamp = null;
            while (resultSet.next()) {
                final String user = resultSet.getString(1);
                dmem = resultSet.getDouble(2);
                dmemMax = resultSet.getDouble(3);
                mem = resultSet.getLong(11);
                memMax = resultSet.getLong(12);
                cpu = resultSet.getInt(4);
                final int tps = resultSet.getInt(5);
                final int avg_exec_time = resultSet.getInt(6);
                final int max_exec_time = resultSet.getInt(7);
                final int activeConnections = resultSet.getInt(8);
                totalUsers += activeConnections;
                timestamp  = resultSet.getTimestamp(9);
                utctimestamp = resultSet.getTimestamp(10);
                if (GlobalConfiguration.keepStatsInMemory) {
                    synchronized (resultsGuard) {
                        if (!userMeasurements.containsKey(user))
                            userMeasurements.put(user, new ArrayList<Measurements>());
                        userMeasurements.get(user).add(new Measurements(tps, avg_exec_time, max_exec_time, activeConnections));

                    }
                }
            }
            if (GlobalConfiguration.keepStatsInMemory) {
//                if (null != updateProcessesMonitor)
//                    updateProcesses.add(updateProcessesMonitor.getRunningProcessesCount());
//                if (null != queryProcessMonitor)
//                    queryProcesses.add(queryProcessMonitor.getRunningProcessesCount());
                memoryBytes.add(mem);
                memoryGBytes.add(dmem);
                cpus.add(cpu);
                timestamps.add(timestamp);
                utctimestamps.add(utctimestamp);
                if (null != future && isProcessingMonitorEnabled) {
                    Long alltime = future.get();
                    alltimes.add(alltime);
                }
            }
            Thread.sleep(sleep_ms);
        } catch (ExecutionException e) {
            log.error("Error while reading ALL_TIME parameter", e);
        } finally {
            if (null != resultSet)
                resultSet.close();
        }
    }

    private void prepareJdbcObjects() throws SQLException {
        connection = DriverManager.getConnection(url);
        final String sql = "call \"" + ConfigurationManager.instance().getSchema() + "\".\"SP_PERFORMANCE_INFO\"(?,?)";
        statement = connection.prepareCall(sql);
        statement.setInt(1, age_seconds);
        statement.setString(2, "all_users");
        try {
            runLogMonitor = new RunLogMonitor(url);
            isProcessingMonitorEnabled = runLogMonitor.isEnabled();
        } catch (Exception e) {
            log.error("Cannot start RunLog Monitor: " + e.getMessage(), e);
            isProcessingMonitorEnabled = false;
        }
        log.info("RunLogMonitor enabled: " + isProcessingMonitorEnabled);
    }

    @Override
    public List<String> getResults() {
        synchronized (resultsGuard) {
            prepareResults(colName);
            return results;
        }
    }

    private void prepareResults(String colName) {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("TIMESTAMP: \t");
        for (Timestamp ts : utctimestamps)
            stringBuilder.append(ts.toString()).append('\t');
        stringBuilder.append(LINESEP);

        stringBuilder.append(colName).append(" UPDATE PROCESSES:\t");
        for (Integer up : updateProcesses)
            stringBuilder.append(up).append('\t');
        stringBuilder.append(LINESEP);

        stringBuilder.append(colName).append(" QUERY PROCESSES:\t");
        for (Integer qp : queryProcesses)
            stringBuilder.append(qp).append('\t');
        stringBuilder.append(LINESEP);

        stringBuilder.append(colName).append(" CPU (%):\t");
        for (Integer cpu : cpus)
            stringBuilder.append(cpu).append('\t');
        stringBuilder.append(LINESEP);

        stringBuilder.append(colName).append(" CPU Integral:\t");
        stringBuilder.append(getConsumedIntegral(getLongs(cpus)));
        stringBuilder.append(LINESEP);

        stringBuilder.append(colName).append(" Index Server MEMORY (Bytes):\t");
        for (Long mem : memoryBytes)
            stringBuilder.append(mem).append('\t');
        stringBuilder.append(LINESEP);

        stringBuilder.append(colName).append(" Index Server MEMORY (GBytes):\t");
        final NumberFormat numberFormat = NumberFormat.getInstance(ConfigurationManager.instance().getDecimalLocale());
        for (Double mem : memoryGBytes)
            stringBuilder.append(numberFormat.format(mem)).append('\t');
        stringBuilder.append(LINESEP);

        stringBuilder.append(colName).append(" Index Server MEMORY Integral:\t");
        double[] values = new double[memoryGBytes.size()];
        for (int i = 0; i < memoryGBytes.size(); i++)
            values[i] = memoryGBytes.get(i);
        long integral = (long) getIntegral(values);
        stringBuilder.append(integral);
        stringBuilder.append(LINESEP);

        stringBuilder.append(colName).append(" Index Server Consumed MEMORY Integral:\t");
        values = new double[memoryGBytes.size()];
        for (int i = 0; i < memoryGBytes.size(); i++)
            values[i] = memoryGBytes.get(i);
        integral = (long) getConsumedIntegral(values);
        stringBuilder.append(integral);
        stringBuilder.append(LINESEP);

        if (null != runLogMonitor && runLogMonitor.isEnabled()) {
            stringBuilder.append(colName).append(" ALL_TIME (ms):\t");
            for (Long alltime : alltimes)
                stringBuilder.append(alltime).append('\t');
            stringBuilder.append(LINESEP);
        }

        for (Map.Entry<String, List<Measurements>> e : userMeasurements.entrySet()) {
            String user = e.getKey();
            List<Measurements> measurements = e.getValue();
            stringBuilder.append(user).append(' ').append(colName).append(" (TPS):\t");
            for (Measurements m : measurements)
                stringBuilder.append(m.tps).append('\t');
            stringBuilder.append(LINESEP);

            stringBuilder.append(user).append(' ').append(colName).append(" (#ACTIVE_CONNECTIONS):\t");
            for (Measurements m : measurements)
                stringBuilder.append(m.activeConnections).append('\t');
            stringBuilder.append(LINESEP);

            stringBuilder.append(user).append(' ').append(colName).append(" AVG EXEC TIME (ms):\t");
            for (Measurements m : measurements)
                stringBuilder.append(m.avg_exec_time).append('\t');
            stringBuilder.append(LINESEP);

            stringBuilder.append(user).append(' ').append(colName).append(" MAX EXEC TIME (ms):\t");
            for (Measurements m : measurements)
                stringBuilder.append(m.max_exec_time).append('\t');
            stringBuilder.append(LINESEP);
        }
        results.add(stringBuilder.toString());
    }

    @Override
    public void shutdown() {
        this.done = true;
    }

    private void close() {
        if (null != statement)
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        if (null != connection)
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        if (null != runLogMonitor)
            runLogMonitor.close();
        executor.shutdownNow();
    }

    private double getIntegral(double[] points) {
        final long h = sleep_ms / 1000;
        if (points.length < 2)
            return 0;
        if (points.length == 2)
            return ((points[1] - points[0]) / 2) * h;
        return MathUtils.getTrapeziumIntegral(points, h);
    }

    private double getConsumedIntegral(double[] values) {
        final long h = sleep_ms / 1000;
        if (values.length < 2)
            return 0;
        if (values.length == 2)
            return ((values[1] - values[0]) / 2) * h;
        final double firstVal = values[0];
        double[] points = new double[values.length];
        for (int i = 0; i < points.length; i++)
            points[i] = values[i] - firstVal;
        return MathUtils.getTrapeziumIntegral(points, h);
    }

    private long getConsumedIntegral(List<Long> values) {
        final long h = sleep_ms / 1000;
        if (values.size() < 2)
            return 0;
        if (values.size() == 2)
            return ((values.get(1) - values.get(0)) / 2) * h;
        final long firstVal = values.get(0);
        long[] points = new long[values.size()];
        for (int i = 0; i < points.length; i++)
            points[i] = values.get(i) - firstVal;
        return MathUtils.getTrapeziumIntegral(points, h);
    }

    private List<Long> getLongs(List<Integer> values) {
        List<Long> longs = new ArrayList<Long>(values.size());
        for (Integer v : values)
            longs.add(Long.valueOf(v.toString()));
        return longs;
    }


    static class Measurements {

        private final int tps;
        private final int avg_exec_time;
        private final int max_exec_time;
        private final int activeConnections;

        Measurements(int tps, int avg_exec_time, int max_exec_time, int activeConnections) {
            this.tps = tps;
            this.avg_exec_time = avg_exec_time;
            this.max_exec_time = max_exec_time;
            this.activeConnections = activeConnections;
        }
    }
}
