package org.kr.intp.util.db.pool;

import org.kr.intp.application.AppContext;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.retry.RetryingInvokable;
import org.kr.intp.util.retry.RetryingInvokerBase;
import com.sap.db.jdbcext.DataSourceSAP;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

public class SAPSimpleDataSource implements IDataSource {

    private final IntpConfig config = AppContext.instance().getConfiguration();

    private final Logger log = LoggerFactory.getLogger(SAPSimpleDataSource.class);
    private final boolean isTraceEnabled = log.isTraceEnabled();
    private final Properties clientInfo = new Properties();
    private final DataSource dataSource = setupDataSource();
    private final boolean isDbHaEnabled = config.isDbHaEnabled();
    private final long connectionRetryPauseMS;
    private final long connectionTimeoutMS;
    private final long connectionTotalTimeoutMS;

    private volatile boolean isClosed = false;

    SAPSimpleDataSource(long connectionRetryPauseMS, long connectionTimeoutMS, long connectionTotalTimeoutMS) {
        this(null, connectionRetryPauseMS, connectionTimeoutMS, connectionTotalTimeoutMS);
    }

    SAPSimpleDataSource(Properties clientInfo,
                        long connectionRetryPauseMS, long connectionTimeoutMS, long connectionTotalTimeoutMS) {
        if (null == clientInfo) {
            this.clientInfo.setProperty("APPLICATION", config.getWorkloadPriority());
        } else {
            this.clientInfo.putAll(clientInfo);
        }
        this.connectionRetryPauseMS = connectionRetryPauseMS;
        this.connectionTimeoutMS = connectionTimeoutMS;
        this.connectionTotalTimeoutMS = connectionTotalTimeoutMS;
        if (isTraceEnabled)
            log.trace("Client info properties set: " + clientInfo);
    }

    @Override
    public void close() {
        isClosed = true;
        log.info(getClass() + " data source closed");
    }

    @Override
    public Connection getConnection() throws SQLException {
        if (isDbHaEnabled)
            return retrieveConfiguredConnectionWithRetries(null, null);
        else
            return retrieveConfiguredConnectionWithoutRetries(null, null);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        if (isDbHaEnabled)
            return retrieveConfiguredConnectionWithRetries(null, null);
        else
            return retrieveConfiguredConnectionWithoutRetries(null, null);
    }

    private Connection retrieveConfiguredConnectionWithRetries(final String username,
                                                               final String password) throws SQLException {
        final RetryingInvokable<Connection> retryingInvokable = new RetryingInvokable<Connection>() {
            @Override
            public Connection invoke() throws Exception {
                return retrieveConfiguredConnectionWithoutRetries(username, password);
            }
        };
        final RetryingInvokerBase<Connection> retryingInvokerBase =
                new RetryingInvokerBase<Connection>(connectionRetryPauseMS, connectionTimeoutMS, "CM") {
            @Override
            public boolean isErrorRecoverable(Throwable e) {
                return true;
            }
        };
        return retryingInvokerBase.invokeWithRetries(retryingInvokable, connectionTotalTimeoutMS);
    }

    private Connection retrieveConfiguredConnectionWithoutRetries(final String username, final String password) throws SQLException {
        checkState();
        Connection connection;
        if (null == username)
            connection = dataSource.getConnection();
        else
            connection = dataSource.getConnection(username, password);
        configureConnection(connection);
        return connection;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        checkState();
        return dataSource.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        checkState();
        return dataSource.isWrapperFor(iface);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        checkState();
        return dataSource.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        checkState();
        dataSource.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        checkState();
        dataSource.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        checkState();
        return dataSource.getLoginTimeout();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        checkState();
        return dataSource.getParentLogger();
    }

    protected void configureConnection(Connection connection) throws SQLException {
        if (null != clientInfo)
            connection.setClientInfo(clientInfo);
    }

    protected void checkState() {
        if (isClosed)
            throw new IllegalStateException("DataSource is closed");
    }

    private DataSource setupDataSource() {
        DataSourceSAP sourceSAP = new DataSourceSAP();
        sourceSAP.setServerName(config.getDbHost());
        sourceSAP.setPort(config.getDbPort());
        sourceSAP.setUser(config.getDbUser());
        sourceSAP.setPassword(config.getDbPassword());
        return sourceSAP;
    }
}
