package org.kr.db.loader.ui.monitor;

import org.kr.db.loader.ui.derby.DerbySaver;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by kr on 5/27/2014.
 */
public abstract class MonitorBase implements IMonitor {

    private final Map<MonitorElement, List<Long>> values = new HashMap<MonitorElement, List<Long>>();
    private final Object resultsLock = new Object();
    private final List<Timestamp> timestamps = new ArrayList<Timestamp>();
    private final int topx;
    private final String col1title;
    private final String col2title;

    protected Connection connection;
    protected PreparedStatement statement;
    protected String statement_query;
    private volatile boolean done = false;
    private final Object doneJobLock = new Object();

    private final String url;
    private final CountDownLatch startGate;
    protected final long monitor_sleep_ms;
    private Throwable exception;
    private final DerbySaver derbySaver;

    public MonitorBase(String url, int topx, CountDownLatch startGate, long monitor_sleep_ms,
                       DerbySaver derbySaver) throws Exception {
        this.startGate = startGate;
        this.monitor_sleep_ms = monitor_sleep_ms;
        this.url = url;
        this.topx = topx;
        this.col1title = getCol1title();
        this.col2title = getCol2title();
        this.derbySaver = derbySaver;
    }

    protected abstract String getStatementQuery();
    protected abstract String getCol1title();
    protected abstract String getCol2title();

    @Override
    public void run() {
        try {
            doWork();
        } catch (Exception e) {
            e.printStackTrace();
            this.exception = e;
        }
    }

    private void doWork() throws Exception {
        try {
            prepareConnection();
            startGate.await();
            while (!done) {
                monitor();
                synchronized (doneJobLock) {
                    doneJobLock.wait(monitor_sleep_ms);
                }
            }
        } finally {
            close();
        }
    }

    private void prepareConnection() throws SQLException, InterruptedException {
        this.connection = DriverManager.getConnection(url);
        this.statement_query = getStatementQuery();
        this.statement = connection.prepareStatement(statement_query);
    }

    public void shutdown() {
        synchronized (doneJobLock) {
            this.done = true;
            doneJobLock.notify();
        }
    }

    private void monitor() throws SQLException {
        ResultSet set = null;
        try {
            statement.execute();
            set = statement.getResultSet();
            Timestamp timestamp = null;
            while (set.next()) {
                final String component = set.getString(1);
                final String category = set.getString(2);
                final int depth = set.getInt(3);
                final long size = set.getLong(4);
                timestamp = set.getTimestamp(5);
                final MonitorElement e = new MonitorElement(component, category, depth);
                if (null != derbySaver)
                    derbySaver.saveStats(e, timestamp, size);
                //synchronized (resultsLock) {
                //   if (!values.containsKey(e))
                //        values.put(e, new ArrayList<Long>());
                //    values.get(e).add(size);
                //}
            }
            //timestamps.add(timestamp);
        } finally {
            if (null != set)
                set.close();
        }
    }

    private void close() throws SQLException {
        if (null != statement)
            statement.close();

        if (null != connection)
            connection.close();
    }

    public List<String> getResults() {
        synchronized (resultsLock) {
            final List<String> data = getLines();
            if (null != exception)
                data.add(exception.getMessage());
            data.add("");
            return data;
        }
    }

    private List<String> getLines() {
        final List<String> data = new ArrayList<String>();

        if (values.size() == 0)
            return new ArrayList<String>();
        if (values.entrySet().iterator().next().getValue().size() == 0)
            return new ArrayList<String>();

        //get top values
        final TreeSet<MonitorElement> sortedvals = new TreeSet<MonitorElement>(new Comparator<MonitorElement>() {
            @Override
            public int compare(MonitorElement o1, MonitorElement o2) {
                long o1lastval = values.get(o1).get(values.get(o1).size() - 1);
                long o2lastval = values.get(o2).get(values.get(o2).size() - 1);
                long diff = o2lastval - o1lastval;
                if (diff > 0)
                    return 1;
                else if (diff < 0)
                    return -1;
                else return 0;
            }
        });
        sortedvals.addAll(values.keySet());

        final Set<MonitorElement> topvals = new HashSet<MonitorElement>(topx);
        for (int i = 0; i < topx && sortedvals.size() > 0; i++)
            topvals.add(sortedvals.pollFirst());

        //get string lines
        final StringBuilder header = new StringBuilder();
        header.append(col1title).append('\t');
        if (col2title.length() > 0)
            header.append(col2title);
        for (Timestamp ts : timestamps)
            header.append('\t').append(ts);
        data.add(header.toString());
        for (MonitorElement e : topvals) {
            final StringBuilder line = new StringBuilder();
            line.append(e.getComponent()).append('\t');
            if (col2title.length() > 0)
                line.append(e.getCategory());
            for (Long v : values.get(e))
                line.append('\t').append(v);
            data.add(line.toString());
        }
        return data;
    }
}
