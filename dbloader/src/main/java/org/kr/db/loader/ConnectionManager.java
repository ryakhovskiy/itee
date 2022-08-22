package org.kr.db.loader;

import org.apache.log4j.Logger;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by kr on 31.01.14.
 */
public class ConnectionManager {

    static {
        try {
            Class.forName(AppConfig.getInstance().getJdbcDriverName());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

    private static final boolean USE_POOLING = AppConfig.getInstance().isConnectionPoolingEnabled();

    private static final String url = AppConfig.getInstance().getJdbcURL();

    public static ConnectionManager newInstance(int capacity) throws SQLException {
        return newInstance(capacity, capacity == 1 ? 1 : capacity / 2);
    }

    public static ConnectionManager newInstance(int capacity, int initialSize) throws SQLException {
        ConnectionManager connectionManager = new ConnectionManager(capacity);
        try {
            connectionManager.init(initialSize);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return connectionManager;
    }

    private final ExecutorService connectionCreatorExecutor = Executors.newCachedThreadPool(NamedThreadFactory.newInstance("CC"));
    private final Logger log = Logger.getLogger(ConnectionManager.class);
    private final boolean isTraceEnabled = log.isTraceEnabled();
    private final AtomicInteger counter = new AtomicInteger(0);
    private final int capacity;
    private final BlockingQueue<ProxyConnection> availableConnections;
    private final ReentrantLock guard = new ReentrantLock();
    private final Set<ProxyConnection> connections = new HashSet<ProxyConnection>();
    private volatile boolean isClosed = false;
    private volatile boolean isFullCapacity = false;

    private ConnectionManager(int capacity) {
        log.info("initializing");
        this.capacity = capacity;
        this.availableConnections = new ArrayBlockingQueue<ProxyConnection>(capacity);
    }

    public Connection getConnection() throws SQLException {
        if (!USE_POOLING)
            return DriverManager.getConnection(url);
        if (isClosed)
            throw new IllegalStateException("ConnectionManager is closed");
        if (isFullCapacity)
            return getConnectionFromQueue();
        try {
            guard.lock();
            if (availableConnections.size() > 0)
                return getConnectionFromQueue();
            if (counter.get() < capacity)
                createNewConnection();
        } finally {
            guard.unlock();
        }
        return getConnectionFromQueue();
    }

    private Connection getConnectionFromQueue() throws SQLException {
        if (isTraceEnabled)
            log.trace("retrieving connection");
        try {
            ProxyConnection connection = availableConnections.take();
            connection.inpool = false;
            if (isTraceEnabled)
                log.trace("retrieved connection #" + connection.id);
            return connection;
        } catch (InterruptedException e) {
            log.debug("interrupted");
            Thread.currentThread().interrupt();
            throw new SQLException(e);
        }
    }

    private void init(int initialConnectionsCount) throws SQLException, InterruptedException {
        if (isTraceEnabled)
            log.trace(String.format("initializing %d connections", initialConnectionsCount));
        for (int i = 0; i < initialConnectionsCount; i++)
            createNewConnection();
    }

    private void createNewConnection() {
        int index = counter.incrementAndGet();
        this.isFullCapacity = index == capacity;
        connectionCreatorExecutor.submit(new ConnectionCreator(index));
    }

    public void close() {
        this.isClosed = true;
        log.info("closing pool...");
        connectionCreatorExecutor.shutdownNow();
        for (ProxyConnection connection : connections) {
            try {
                connection.dispose();
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        }
        connections.clear();
        counter.set(0);
    }

    private class ConnectionCreator implements Runnable {

        private final int id;

        private ConnectionCreator(int id) {
            this.id = id;
        }

        public void run() {
            try {
                Connection connection = DriverManager.getConnection(url);
                ProxyConnection proxyConnection = new ProxyConnection(connection, id);
                connections.add(proxyConnection);
                availableConnections.put(proxyConnection);
            } catch (SQLException e) {
                log.error(e.getMessage(), e);
            } catch (InterruptedException e) {
                log.debug("interrupted");
                Thread.currentThread().interrupt();
            }
        }
    }

    public class ProxyConnection implements Connection {

        private final int id;
        private final Connection connection;
        private volatile boolean inpool = false;
        private final Map<String, Statement> statements = new HashMap<String, Statement>();

        private ProxyConnection(Connection connection, int id) {
            this.id = id;
            if (isTraceEnabled)
                log.trace(String.format("connection #%d initializing...", id));
            this.connection = connection;
        }

        private void dispose() throws SQLException {
            log.debug("disposing connection # " + id);
            //close all bonded statements
            for (Statement statement : statements.values())
                if (statement instanceof ProxyPreparedStatement)
                    ((ProxyPreparedStatement)statement).dispose();
            if (null != connection && !connection.isClosed())
                connection.close();
        }

        @Override
        public int hashCode() {
            return id;
        }

        @Override
        public boolean equals(Object other) {
            if (other == this)
                return true;
            if (null == other)
                return false;
            if (!(other instanceof ProxyConnection))
                return false;
            ProxyConnection otherConnection = (ProxyConnection)other;
            return otherConnection.id == id;
        }

        @Override
        public void close() throws SQLException {
            if (this.inpool)
                return;
            try {
                if (isTraceEnabled)
                    log.trace(String.format("returning connection #%d to pool", id));
                this.inpool = true;
                availableConnections.put(this);
            } catch (InterruptedException e) {
                log.debug("interrupted");
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public Statement createStatement() throws SQLException {
            Statement statement = connection.createStatement();
            statement.setPoolable(true);
            return statement;
        }

        @Override
        public PreparedStatement prepareStatement(String sql) throws SQLException {
            if (statements.containsKey(sql))
                return (PreparedStatement)statements.get(sql);
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setPoolable(true);
            ProxyPreparedStatement proxyPreparedStatement = new ProxyPreparedStatement(statement);
            statements.put(sql, proxyPreparedStatement);
            return proxyPreparedStatement;
        }

        @Override
        public CallableStatement prepareCall(String sql) throws SQLException {
            if (statements.containsKey(sql))
                return (CallableStatement)statements.get(sql);
            CallableStatement statement = connection.prepareCall(sql);
            statement.setPoolable(true);
            ProxyCallableStatement proxyCallableStatement = new ProxyCallableStatement(statement);
            statements.put(sql, proxyCallableStatement);
            return proxyCallableStatement;
        }

        @Override
        public String nativeSQL(String sql) throws SQLException {
            return connection.nativeSQL(sql);
        }

        @Override
        public void setAutoCommit(boolean autoCommit) throws SQLException {
            connection.setAutoCommit(autoCommit);
        }

        @Override
        public boolean getAutoCommit() throws SQLException {
            return connection.getAutoCommit();
        }

        @Override
        public void commit() throws SQLException {
            connection.commit();
        }

        @Override
        public void rollback() throws SQLException {
            connection.rollback();
        }

        @Override
        public boolean isClosed() throws SQLException {
            return connection.isClosed();
        }

        @Override
        public DatabaseMetaData getMetaData() throws SQLException {
            return connection.getMetaData();
        }

        @Override
        public void setReadOnly(boolean readOnly) throws SQLException {
            connection.setReadOnly(readOnly);
        }

        @Override
        public boolean isReadOnly() throws SQLException {
            return connection.isReadOnly();
        }

        @Override
        public void setCatalog(String catalog) throws SQLException {
            connection.setCatalog(catalog);
        }

        @Override
        public String getCatalog() throws SQLException {
            return connection.getCatalog();
        }

        @Override
        public void setTransactionIsolation(int level) throws SQLException {
            connection.setTransactionIsolation(level);
        }

        @Override
        public int getTransactionIsolation() throws SQLException {
            return connection.getTransactionIsolation();
        }

        @Override
        public SQLWarning getWarnings() throws SQLException {
            return connection.getWarnings();
        }

        @Override
        public void clearWarnings() throws SQLException {
            connection.clearWarnings();
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            Statement statement = connection.createStatement(resultSetType, resultSetConcurrency);
            statement.setPoolable(true);
            return statement;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            PreparedStatement statement = connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
            statement.setPoolable(true);
            return statement;
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            CallableStatement statement = connection.prepareCall(sql, resultSetType, resultSetConcurrency);
            statement.setPoolable(true);
            return statement;
        }

        @Override
        public Map<String, Class<?>> getTypeMap() throws SQLException {
            return connection.getTypeMap();
        }

        @Override
        public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
            connection.setTypeMap(map);
        }

        @Override
        public void setHoldability(int holdability) throws SQLException {
            connection.setHoldability(holdability);
        }

        @Override
        public int getHoldability() throws SQLException {
            return connection.getHoldability();
        }

        @Override
        public Savepoint setSavepoint() throws SQLException {
            return connection.setSavepoint();
        }

        @Override
        public Savepoint setSavepoint(String name) throws SQLException {
            return connection.setSavepoint(name);
        }

        @Override
        public void rollback(Savepoint savepoint) throws SQLException {
            connection.rollback(savepoint);
        }

        @Override
        public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            connection.releaseSavepoint(savepoint);
        }

        @Override
        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            Statement statement = connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
            statement.setPoolable(true);
            return statement;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            PreparedStatement statement = connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
            statement.setPoolable(true);
            return statement;
        }

        @Override
        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            CallableStatement statement = connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
            statement.setPoolable(true);
            return statement;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            PreparedStatement statement = connection.prepareStatement(sql, autoGeneratedKeys);
            statement.setPoolable(true);
            return statement;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            PreparedStatement statement = connection.prepareStatement(sql, columnIndexes);
            statement.setPoolable(true);
            return statement;
        }

        @Override
        public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            PreparedStatement statement = connection.prepareStatement(sql, columnNames);
            statement.setPoolable(true);
            return statement;
        }

        @Override
        public Clob createClob() throws SQLException {
            return connection.createClob();
        }

        @Override
        public Blob createBlob() throws SQLException {
            return connection.createBlob();
        }

        @Override
        public NClob createNClob() throws SQLException {
            return connection.createNClob();
        }

        @Override
        public SQLXML createSQLXML() throws SQLException {
            return connection.createSQLXML();
        }

        @Override
        public boolean isValid(int timeout) throws SQLException {
            return connection.isValid(timeout);
        }

        @Override
        public void setClientInfo(String name, String value) throws SQLClientInfoException {
            connection.setClientInfo(name, value);
        }

        @Override
        public void setClientInfo(Properties properties) throws SQLClientInfoException {
            connection.setClientInfo(properties);
        }

        @Override
        public String getClientInfo(String name) throws SQLException {
            return connection.getClientInfo(name);
        }

        @Override
        public Properties getClientInfo() throws SQLException {
            return connection.getClientInfo();
        }

        @Override
        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return connection.createArrayOf(typeName, elements);
        }

        @Override
        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return connection.createStruct(typeName, attributes);
        }

        @Override
        public void setSchema(String schema) throws SQLException {
            this.connection.setSchema(schema);
        }

        @Override
        public String getSchema() throws SQLException {
            return this.connection.getSchema();
        }

        @Override
        public void abort(Executor executor) throws SQLException {
            this.connection.abort(executor);
        }

        @Override
        public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
            this.connection.setNetworkTimeout(executor, milliseconds);
        }

        @Override
        public int getNetworkTimeout() throws SQLException {
            return connection.getNetworkTimeout();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            return connection.unwrap(iface);
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return connection.isWrapperFor(iface);
        }

        public class ProxyPreparedStatement implements PreparedStatement {

            private final PreparedStatement statement;

            private ProxyPreparedStatement(PreparedStatement statement) {
                this.statement = statement;
            }

            private void dispose() throws SQLException {
                if (null != statement && !statement.isClosed())
                    statement.close();
            }

            @Override
            public ResultSet executeQuery(String sql) throws SQLException {
                return statement.executeQuery(sql);
            }

            @Override
            public int executeUpdate(String sql) throws SQLException {
                return statement.executeUpdate(sql);
            }

            @Override
            public void close() throws SQLException {
                //TODO: override
                //do not do anything. not-thread-safe implementation of caching statements!
            }

            @Override
            public int getMaxFieldSize() throws SQLException {
                return statement.getMaxFieldSize();
            }

            @Override
            public void setMaxFieldSize(int max) throws SQLException {
                statement.setMaxFieldSize(max);
            }

            @Override
            public int getMaxRows() throws SQLException {
                return statement.getMaxRows();
            }

            @Override
            public void setMaxRows(int max) throws SQLException {
                statement.setMaxRows(max);
            }

            @Override
            public void setEscapeProcessing(boolean enable) throws SQLException {
                statement.setEscapeProcessing(enable);
            }

            @Override
            public int getQueryTimeout() throws SQLException {
                return statement.getQueryTimeout();
            }

            @Override
            public void setQueryTimeout(int seconds) throws SQLException {
                statement.setQueryTimeout(seconds);
            }

            @Override
            public void cancel() throws SQLException {
                statement.cancel();
            }

            @Override
            public SQLWarning getWarnings() throws SQLException {
                return statement.getWarnings();
            }

            @Override
            public void clearWarnings() throws SQLException {
                statement.clearWarnings();
            }

            @Override
            public void setCursorName(String name) throws SQLException {
                statement.setCursorName(name);
            }

            @Override
            public boolean execute(String sql) throws SQLException {
                return statement.execute(sql);
            }

            @Override
            public ResultSet getResultSet() throws SQLException {
                return statement.getResultSet();
            }

            @Override
            public int getUpdateCount() throws SQLException {
                return statement.getUpdateCount();
            }

            @Override
            public boolean getMoreResults() throws SQLException {
                return statement.getMoreResults();
            }

            @Override
            public void setFetchDirection(int direction) throws SQLException {
                statement.setFetchDirection(direction);
            }

            @Override
            public int getFetchDirection() throws SQLException {
                return statement.getFetchDirection();
            }

            @Override
            public void setFetchSize(int rows) throws SQLException {
                statement.setFetchSize(rows);
            }

            @Override
            public int getFetchSize() throws SQLException {
                return statement.getFetchSize();
            }

            @Override
            public int getResultSetConcurrency() throws SQLException {
                return statement.getResultSetConcurrency();
            }

            @Override
            public int getResultSetType() throws SQLException {
                return statement.getResultSetType();
            }

            @Override
            public void addBatch(String sql) throws SQLException {
                statement.addBatch(sql);
            }

            @Override
            public void clearBatch() throws SQLException {
                statement.clearBatch();
            }

            @Override
            public int[] executeBatch() throws SQLException {
                return statement.executeBatch();
            }

            @Override
            public Connection getConnection() throws SQLException {
                return ProxyConnection.this;
            }

            @Override
            public boolean getMoreResults(int current) throws SQLException {
                return statement.getMoreResults(current);
            }

            @Override
            public ResultSet getGeneratedKeys() throws SQLException {
                return statement.getGeneratedKeys();
            }

            @Override
            public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
                return statement.executeUpdate(sql, autoGeneratedKeys);
            }

            @Override
            public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
                return statement.executeUpdate(sql, columnIndexes);
            }

            @Override
            public int executeUpdate(String sql, String[] columnNames) throws SQLException {
                return statement.executeUpdate(sql, columnNames);
            }

            @Override
            public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
                return statement.execute(sql, autoGeneratedKeys);
            }

            @Override
            public boolean execute(String sql, int[] columnIndexes) throws SQLException {
                return statement.execute(sql, columnIndexes);
            }

            @Override
            public boolean execute(String sql, String[] columnNames) throws SQLException {
                return statement.execute(sql, columnNames);
            }

            @Override
            public int getResultSetHoldability() throws SQLException {
                return statement.getResultSetHoldability();
            }

            @Override
            public boolean isClosed() throws SQLException {
                return statement.isClosed();
            }

            @Override
            public void setPoolable(boolean poolable) throws SQLException {
                statement.setPoolable(poolable);
            }

            @Override
            public boolean isPoolable() throws SQLException {
                return statement.isPoolable();
            }

            @Override
            public void closeOnCompletion() throws SQLException {
                statement.closeOnCompletion();
            }

            @Override
            public boolean isCloseOnCompletion() throws SQLException {
                return statement.isCloseOnCompletion();
            }

            /*@Override
            public void closeOnCompletion() throws SQLException {
                statement.closeOnCompletion();
            }

            @Override
            public boolean isCloseOnCompletion() throws SQLException {
                return statement.isCloseOnCompletion();
            }*/

            @Override
            public <T> T unwrap(Class<T> iface) throws SQLException {
                return statement.unwrap(iface);
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) throws SQLException {
                return statement.isWrapperFor(iface);
            }

            @Override
            public ResultSet executeQuery() throws SQLException {
                return statement.executeQuery();
            }

            @Override
            public int executeUpdate() throws SQLException {
                return statement.executeUpdate();
            }

            @Override
            public void setNull(int parameterIndex, int sqlType) throws SQLException {
                statement.setNull(parameterIndex, sqlType);
            }

            @Override
            public void setBoolean(int parameterIndex, boolean x) throws SQLException {
                statement.setBoolean(parameterIndex, x);
            }

            @Override
            public void setByte(int parameterIndex, byte x) throws SQLException {
                statement.setByte(parameterIndex, x);
            }

            @Override
            public void setShort(int parameterIndex, short x) throws SQLException {
                statement.setShort(parameterIndex, x);
            }

            @Override
            public void setInt(int parameterIndex, int x) throws SQLException {
                statement.setInt(parameterIndex, x);
            }

            @Override
            public void setLong(int parameterIndex, long x) throws SQLException {
                statement.setLong(parameterIndex, x);
            }

            @Override
            public void setFloat(int parameterIndex, float x) throws SQLException {
                statement.setFloat(parameterIndex, x);
            }

            @Override
            public void setDouble(int parameterIndex, double x) throws SQLException {
                statement.setDouble(parameterIndex, x);
            }

            @Override
            public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
                statement.setBigDecimal(parameterIndex, x);
            }

            @Override
            public void setString(int parameterIndex, String x) throws SQLException {
                statement.setString(parameterIndex, x);
            }

            @Override
            public void setBytes(int parameterIndex, byte[] x) throws SQLException {
                statement.setBytes(parameterIndex, x);
            }

            @Override
            public void setDate(int parameterIndex, Date x) throws SQLException {
                statement.setDate(parameterIndex, x);
            }

            @Override
            public void setTime(int parameterIndex, Time x) throws SQLException {
                statement.setTime(parameterIndex, x);
            }

            @Override
            public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
                statement.setTimestamp(parameterIndex, x);
            }

            @Override
            public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
                statement.setAsciiStream(parameterIndex, x, length);
            }

            @Override
            @Deprecated
            public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
                statement.setUnicodeStream(parameterIndex, x, length);
            }

            @Override
            public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
                statement.setBinaryStream(parameterIndex, x, length);
            }

            @Override
            public void clearParameters() throws SQLException {
                statement.clearParameters();
            }

            @Override
            public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
                statement.setObject(parameterIndex, x, targetSqlType);
            }

            @Override
            public void setObject(int parameterIndex, Object x) throws SQLException {
                statement.setObject(parameterIndex, x);
            }

            @Override
            public boolean execute() throws SQLException {
                return statement.execute();
            }

            @Override
            public void addBatch() throws SQLException {
                statement.addBatch();
            }

            @Override
            public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
                statement.setCharacterStream(parameterIndex, reader, length);
            }

            @Override
            public void setRef(int parameterIndex, Ref x) throws SQLException {
                statement.setRef(parameterIndex, x);
            }

            @Override
            public void setBlob(int parameterIndex, Blob x) throws SQLException {
                statement.setBlob(parameterIndex, x);
            }

            @Override
            public void setClob(int parameterIndex, Clob x) throws SQLException {
                statement.setClob(parameterIndex, x);
            }

            @Override
            public void setArray(int parameterIndex, Array x) throws SQLException {
                statement.setArray(parameterIndex, x);
            }

            @Override
            public ResultSetMetaData getMetaData() throws SQLException {
                return statement.getMetaData();
            }

            @Override
            public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
                statement.setDate(parameterIndex, x, cal);
            }

            @Override
            public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
                statement.setTime(parameterIndex, x, cal);
            }

            @Override
            public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
                statement.setTimestamp(parameterIndex, x, cal);
            }

            @Override
            public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
                statement.setNull(parameterIndex, sqlType, typeName);
            }

            @Override
            public void setURL(int parameterIndex, URL x) throws SQLException {
                statement.setURL(parameterIndex, x);
            }

            @Override
            public ParameterMetaData getParameterMetaData() throws SQLException {
                return statement.getParameterMetaData();
            }

            @Override
            public void setRowId(int parameterIndex, RowId x) throws SQLException {
                statement.setRowId(parameterIndex, x);
            }

            @Override
            public void setNString(int parameterIndex, String value) throws SQLException {
                statement.setNString(parameterIndex, value);
            }

            @Override
            public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
                statement.setNCharacterStream(parameterIndex, value, length);
            }

            @Override
            public void setNClob(int parameterIndex, NClob value) throws SQLException {
                statement.setNClob(parameterIndex, value);
            }

            @Override
            public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
                statement.setClob(parameterIndex, reader, length);
            }

            @Override
            public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
                statement.setBlob(parameterIndex, inputStream, length);
            }

            @Override
            public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
                statement.setNClob(parameterIndex, reader, length);
            }

            @Override
            public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
                statement.setSQLXML(parameterIndex, xmlObject);
            }

            @Override
            public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
                statement.setObject(parameterIndex, x, targetSqlType, scaleOrLength);
            }

            @Override
            public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
                statement.setAsciiStream(parameterIndex, x, length);
            }

            @Override
            public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
                statement.setBinaryStream(parameterIndex, x, length);
            }

            @Override
            public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
                statement.setCharacterStream(parameterIndex, reader, length);
            }

            @Override
            public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
                statement.setAsciiStream(parameterIndex, x);
            }

            @Override
            public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
                statement.setBinaryStream(parameterIndex, x);
            }

            @Override
            public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
                statement.setCharacterStream(parameterIndex, reader);
            }

            @Override
            public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
                statement.setNCharacterStream(parameterIndex, value);
            }

            @Override
            public void setClob(int parameterIndex, Reader reader) throws SQLException {
                statement.setClob(parameterIndex, reader);
            }

            @Override
            public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
                statement.setBlob(parameterIndex, inputStream);
            }

            @Override
            public void setNClob(int parameterIndex, Reader reader) throws SQLException {
                statement.setNClob(parameterIndex, reader);
            }
        }

        public class ProxyCallableStatement extends ProxyPreparedStatement implements CallableStatement {

            private final CallableStatement statement;

            private ProxyCallableStatement(CallableStatement statement) {
                super(statement);
                this.statement = statement;
            }

            @Override
            public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
                statement.registerOutParameter(parameterIndex, sqlType);
            }

            @Override
            public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
                statement.registerOutParameter(parameterIndex, sqlType, scale);
            }

            @Override
            public boolean wasNull() throws SQLException {
                return statement.wasNull();
            }

            @Override
            public String getString(int parameterIndex) throws SQLException {
                return statement.getString(parameterIndex);
            }

            @Override
            public boolean getBoolean(int parameterIndex) throws SQLException {
                return statement.getBoolean(parameterIndex);
            }

            @Override
            public byte getByte(int parameterIndex) throws SQLException {
                return statement.getByte(parameterIndex);
            }

            @Override
            public short getShort(int parameterIndex) throws SQLException {
                return statement.getShort(parameterIndex);
            }

            @Override
            public int getInt(int parameterIndex) throws SQLException {
                return statement.getInt(parameterIndex);
            }

            @Override
            public long getLong(int parameterIndex) throws SQLException {
                return statement.getLong(parameterIndex);
            }

            @Override
            public float getFloat(int parameterIndex) throws SQLException {
                return statement.getFloat(parameterIndex);
            }

            @Override
            public double getDouble(int parameterIndex) throws SQLException {
                return statement.getDouble(parameterIndex);
            }

            @Override
            @Deprecated
            public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
                return statement.getBigDecimal(parameterIndex, scale);
            }

            @Override
            public byte[] getBytes(int parameterIndex) throws SQLException {
                return statement.getBytes(parameterIndex);
            }

            @Override
            public Date getDate(int parameterIndex) throws SQLException {
                return statement.getDate(parameterIndex);
            }

            @Override
            public Time getTime(int parameterIndex) throws SQLException {
                return statement.getTime(parameterIndex);
            }

            @Override
            public Timestamp getTimestamp(int parameterIndex) throws SQLException {
                return statement.getTimestamp(parameterIndex);
            }

            @Override
            public Object getObject(int parameterIndex) throws SQLException {
                return statement.getObject(parameterIndex);
            }

            @Override
            public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
                return statement.getBigDecimal(parameterIndex);
            }

            @Override
            public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
                return statement.getObject(parameterIndex, map);
            }

            @Override
            public Ref getRef(int parameterIndex) throws SQLException {
                return statement.getRef(parameterIndex);
            }

            @Override
            public Blob getBlob(int parameterIndex) throws SQLException {
                return statement.getBlob(parameterIndex);
            }

            @Override
            public Clob getClob(int parameterIndex) throws SQLException {
                return statement.getClob(parameterIndex);
            }

            @Override
            public Array getArray(int parameterIndex) throws SQLException {
                return statement.getArray(parameterIndex);
            }

            @Override
            public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
                return statement.getDate(parameterIndex, cal);
            }

            @Override
            public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
                return statement.getTime(parameterIndex, cal);
            }

            @Override
            public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
                return statement.getTimestamp(parameterIndex, cal);
            }

            @Override
            public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
                statement.registerOutParameter(parameterIndex, sqlType, typeName);
            }

            @Override
            public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
                statement.registerOutParameter(parameterName, sqlType);
            }

            @Override
            public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
                statement.registerOutParameter(parameterName, sqlType, scale);
            }

            @Override
            public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
                statement.registerOutParameter(parameterName, sqlType, typeName);
            }

            @Override
            public URL getURL(int parameterIndex) throws SQLException {
                return statement.getURL(parameterIndex);
            }

            @Override
            public void setURL(String parameterName, URL val) throws SQLException {
                statement.setURL(parameterName, val);
            }

            @Override
            public void setNull(String parameterName, int sqlType) throws SQLException {
                statement.setNull(parameterName, sqlType);
            }

            @Override
            public void setBoolean(String parameterName, boolean x) throws SQLException {
                statement.setBoolean(parameterName, x);
            }

            @Override
            public void setByte(String parameterName, byte x) throws SQLException {
                statement.setByte(parameterName, x);
            }

            @Override
            public void setShort(String parameterName, short x) throws SQLException {
                statement.setShort(parameterName, x);
            }

            @Override
            public void setInt(String parameterName, int x) throws SQLException {
                statement.setInt(parameterName, x);
            }

            @Override
            public void setLong(String parameterName, long x) throws SQLException {
                statement.setLong(parameterName, x);
            }

            @Override
            public void setFloat(String parameterName, float x) throws SQLException {
                statement.setFloat(parameterName, x);
            }

            @Override
            public void setDouble(String parameterName, double x) throws SQLException {
                statement.setDouble(parameterName, x);
            }

            @Override
            public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
                statement.setBigDecimal(parameterName, x);
            }

            @Override
            public void setString(String parameterName, String x) throws SQLException {
                statement.setString(parameterName, x);
            }

            @Override
            public void setBytes(String parameterName, byte[] x) throws SQLException {
                statement.setBytes(parameterName, x);
            }

            @Override
            public void setDate(String parameterName, Date x) throws SQLException {
                statement.setDate(parameterName, x);
            }

            @Override
            public void setTime(String parameterName, Time x) throws SQLException {
                statement.setTime(parameterName, x);
            }

            @Override
            public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
                statement.setTimestamp(parameterName, x);
            }

            @Override
            public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
                statement.setAsciiStream(parameterName, x, length);
            }

            @Override
            public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
                statement.setBinaryStream(parameterName, x, length);
            }

            @Override
            public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
                statement.setObject(parameterName, x, targetSqlType, scale);
            }

            @Override
            public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
                statement.setObject(parameterName, x, targetSqlType);
            }

            @Override
            public void setObject(String parameterName, Object x) throws SQLException {
                statement.setObject(parameterName, x);
            }

            @Override
            public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
                statement.setCharacterStream(parameterName, reader, length);
            }

            @Override
            public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
                statement.setDate(parameterName, x, cal);
            }

            @Override
            public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
                statement.setTime(parameterName, x, cal);
            }

            @Override
            public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
                statement.setTimestamp(parameterName, x, cal);
            }

            @Override
            public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
                statement.setNull(parameterName, sqlType, typeName);
            }

            @Override
            public String getString(String parameterName) throws SQLException {
                return statement.getString(parameterName);
            }

            @Override
            public boolean getBoolean(String parameterName) throws SQLException {
                return statement.getBoolean(parameterName);
            }

            @Override
            public byte getByte(String parameterName) throws SQLException {
                return statement.getByte(parameterName);
            }

            @Override
            public short getShort(String parameterName) throws SQLException {
                return statement.getShort(parameterName);
            }

            @Override
            public int getInt(String parameterName) throws SQLException {
                return statement.getInt(parameterName);
            }

            @Override
            public long getLong(String parameterName) throws SQLException {
                return statement.getLong(parameterName);
            }

            @Override
            public float getFloat(String parameterName) throws SQLException {
                return statement.getFloat(parameterName);
            }

            @Override
            public double getDouble(String parameterName) throws SQLException {
                return statement.getDouble(parameterName);
            }

            @Override
            public byte[] getBytes(String parameterName) throws SQLException {
                return statement.getBytes(parameterName);
            }

            @Override
            public Date getDate(String parameterName) throws SQLException {
                return statement.getDate(parameterName);
            }

            @Override
            public Time getTime(String parameterName) throws SQLException {
                return statement.getTime(parameterName);
            }

            @Override
            public Timestamp getTimestamp(String parameterName) throws SQLException {
                return statement.getTimestamp(parameterName);
            }

            @Override
            public Object getObject(String parameterName) throws SQLException {
                return statement.getObject(parameterName);
            }

            @Override
            public BigDecimal getBigDecimal(String parameterName) throws SQLException {
                return statement.getBigDecimal(parameterName);
            }

            @Override
            public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
                return statement.getObject(parameterName, map);
            }

            @Override
            public Ref getRef(String parameterName) throws SQLException {
                return statement.getRef(parameterName);
            }

            @Override
            public Blob getBlob(String parameterName) throws SQLException {
                return statement.getBlob(parameterName);
            }

            @Override
            public Clob getClob(String parameterName) throws SQLException {
                return statement.getClob(parameterName);
            }

            @Override
            public Array getArray(String parameterName) throws SQLException {
                return statement.getArray(parameterName);
            }

            @Override
            public Date getDate(String parameterName, Calendar cal) throws SQLException {
                return statement.getDate(parameterName, cal);
            }

            @Override
            public Time getTime(String parameterName, Calendar cal) throws SQLException {
                return statement.getTime(parameterName, cal);
            }

            @Override
            public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
                return statement.getTimestamp(parameterName, cal);
            }

            @Override
            public URL getURL(String parameterName) throws SQLException {
                return statement.getURL(parameterName);
            }

            @Override
            public RowId getRowId(int parameterIndex) throws SQLException {
                return statement.getRowId(parameterIndex);
            }

            @Override
            public RowId getRowId(String parameterName) throws SQLException {
                return statement.getRowId(parameterName);
            }

            @Override
            public void setRowId(String parameterName, RowId x) throws SQLException {
                statement.setRowId(parameterName, x);
            }

            @Override
            public void setNString(String parameterName, String value) throws SQLException {
                statement.setNString(parameterName, value);
            }

            @Override
            public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
                statement.setNCharacterStream(parameterName, value, length);
            }

            @Override
            public void setNClob(String parameterName, NClob value) throws SQLException {
                statement.setNClob(parameterName, value);
            }

            @Override
            public void setClob(String parameterName, Reader reader, long length) throws SQLException {
                statement.setClob(parameterName, reader, length);
            }

            @Override
            public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
                statement.setBlob(parameterName, inputStream, length);
            }

            @Override
            public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
                statement.setNClob(parameterName, reader, length);
            }

            @Override
            public NClob getNClob(int parameterIndex) throws SQLException {
                return statement.getNClob(parameterIndex);
            }

            @Override
            public NClob getNClob(String parameterName) throws SQLException {
                return statement.getNClob(parameterName);
            }

            @Override
            public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
                statement.setSQLXML(parameterName, xmlObject);
            }

            @Override
            public SQLXML getSQLXML(int parameterIndex) throws SQLException {
                return statement.getSQLXML(parameterIndex);
            }

            @Override
            public SQLXML getSQLXML(String parameterName) throws SQLException {
                return statement.getSQLXML(parameterName);
            }

            @Override
            public String getNString(int parameterIndex) throws SQLException {
                return statement.getNString(parameterIndex);
            }

            @Override
            public String getNString(String parameterName) throws SQLException {
                return statement.getNString(parameterName);
            }

            @Override
            public Reader getNCharacterStream(int parameterIndex) throws SQLException {
                return statement.getNCharacterStream(parameterIndex);
            }

            @Override
            public Reader getNCharacterStream(String parameterName) throws SQLException {
                return statement.getNCharacterStream(parameterName);
            }

            @Override
            public Reader getCharacterStream(int parameterIndex) throws SQLException {
                return statement.getCharacterStream(parameterIndex);
            }

            @Override
            public Reader getCharacterStream(String parameterName) throws SQLException {
                return statement.getCharacterStream(parameterName);
            }

            @Override
            public void setBlob(String parameterName, Blob x) throws SQLException {
                statement.setBlob(parameterName, x);
            }

            @Override
            public void setClob(String parameterName, Clob x) throws SQLException {
                statement.setClob(parameterName, x);
            }

            @Override
            public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
                statement.setAsciiStream(parameterName, x, length);
            }

            @Override
            public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
                statement.setBinaryStream(parameterName, x, length);
            }

            @Override
            public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
                statement.setCharacterStream(parameterName, reader, length);
            }

            @Override
            public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
                statement.setAsciiStream(parameterName, x);
            }

            @Override
            public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
                statement.setBinaryStream(parameterName, x);
            }

            @Override
            public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
                statement.setCharacterStream(parameterName, reader);
            }

            @Override
            public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
                statement.setNCharacterStream(parameterName, value);
            }

            @Override
            public void setClob(String parameterName, Reader reader) throws SQLException {
                statement.setClob(parameterName, reader);
            }

            @Override
            public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
                statement.setBlob(parameterName, inputStream);
            }

            @Override
            public void setNClob(String parameterName, Reader reader) throws SQLException {
                statement.setNClob(parameterName, reader);
            }

            @Override
            public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
                return statement.getObject(parameterIndex, type);
            }

            @Override
            public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
                return statement.getObject(parameterName, type);
            }
        }
    }
}
