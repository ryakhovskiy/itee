package org.kr.intp.util.db.pool;

import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class SAPHistoricalDataSource extends SAPSimpleDataSource {

    private static final AtomicLong SOURCE_COUNTER = new AtomicLong(0);

    private final AtomicLong connectionCounter = new AtomicLong(0);
    private final Logger log = LoggerFactory.getLogger(SAPHistoricalDataSource.class);
    private final boolean isTraceEnabled = log.isTraceEnabled();
    private final Timestamp timestamp;
    private final Map<Long, Connection> connections = new HashMap<>();
    private final ConnectionMonitor monitor = new ConnectionMonitor();

    public SAPHistoricalDataSource(long time, Properties clientInfo, long connectionRetryPauseMS, long connectionTimeoutMS, long connectionTotalTimeoutMS) {
        super(clientInfo, connectionRetryPauseMS, connectionTimeoutMS, connectionTotalTimeoutMS);
        this.timestamp = new Timestamp(time);
        monitor.start();
    }

    public SAPHistoricalDataSource(long time, long connectionRetryPauseMS, long connectionTimeoutMS, long connectionTotalTimeoutMS) {
        this(time, null, connectionRetryPauseMS, connectionTimeoutMS, connectionTotalTimeoutMS);
    }

    public synchronized void reset(long time) {
        super.checkState();
        log.debug("reset historical session to time: " + time);
        this.timestamp.setTime(time);
    }

    public void reset(Timestamp timestamp) {
        reset(timestamp.getTime());
    }

    @Override
    public void close() {
        super.close();
        log.trace("closing data source");
        monitor.interrupt();
        log.trace("removing all connections");
        synchronized (connections) {
            for (Connection c : connections.values()) {
                try {
                    if (c.isClosed())
                        continue;
                    c.commit();
                    c.close();
                } catch (SQLException e) {
                    log.error("Error while closing historical connection: " + e.getMessage(), e);
                }
            }
            connections.clear();
        }
        try {
            monitor.join(ConnectionMonitor.TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (isTraceEnabled)
            log.trace("retrieving historical connection, setting auto-commit to false, timestamp: " + timestamp);
        Connection connection = super.getConnection();
        configureConnection(connection);
        return connection;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        if (isTraceEnabled)
            log.trace("retrieving historical connection, setting auto-commit to false, timestamp: " + timestamp);
        Connection connection = super.getConnection(username, password);
        configureConnection(connection);
        return connection;
    }

    @Override
    protected void configureConnection(Connection connection) throws SQLException {
        if (null == connection)
            return;
        super.configureConnection(connection);
        connection.setAutoCommit(false);
        long id = connectionCounter.incrementAndGet();
        synchronized (connections) {
            connections.put(id, connection);
        }
        if (isTraceEnabled)
            log.trace("connection #" + id + " is created and configured");
    }

    private class ConnectionMonitor extends Thread {

        private static final long TIMEOUT_MS = 5000;

        private ConnectionMonitor() {
            super("HCM" + SOURCE_COUNTER.incrementAndGet());
            setDaemon(true);
        }

        public void run() {
            do {
                sleep();
                removeClosedConnections();
            } while (interrupted());


        }

        private void sleep() {
            try {
                sleep(TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private void removeClosedConnections() {
            try {
                final List<Long> closedIDs = getClosedConnectionIDs();
                if (closedIDs.size() == 0)
                    return;

                synchronized (connections) {
                    for (Long id : closedIDs) {
                        if (isTraceEnabled) {
                            log.trace("removing connection #" + id);
                        }
                        Connection c = connections.remove(id);
                        c = null;
                    }
                }
            } catch (SQLException e) {
                log.error(e.getMessage());
            }
        }

        private List<Long> getClosedConnectionIDs() throws SQLException {
            final Map<Long, Connection> copy = new HashMap<>();
            //use copy to allow main threads continue working
            synchronized (connections) {
                copy.putAll(SAPHistoricalDataSource.this.connections);
            }
            final List<Long> closedIDs = new ArrayList<>();
            for (Map.Entry<Long, Connection> e : copy.entrySet()) {
                final Connection c = e.getValue();
                if (c.isClosed())
                    closedIDs.add(e.getKey());
            }
            return closedIDs;
        }
    }
}
