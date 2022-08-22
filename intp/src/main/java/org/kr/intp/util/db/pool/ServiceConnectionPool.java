package org.kr.intp.util.db.pool;

import org.kr.intp.application.AppContext;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class ServiceConnectionPool implements IDataSource {

    private static ServiceConnectionPool instance = new ServiceConnectionPool();

    public static void restart() {
        instance = new ServiceConnectionPool();
    }

    public static ServiceConnectionPool instance() {
        return instance;
    }

    private final Logger log = LoggerFactory.getLogger(ServiceConnectionPool.class);
    private final IDataSource dataSource;

    private ServiceConnectionPool() {
        int poolSize = AppContext.instance().getConfiguration().getServiceConnectionPoolSize();
        this.dataSource = DataSourceFactory.newDataSource(poolSize);
    }



    @Override
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return dataSource.getConnection(username, password);
    }

    @Override
    public void close() {
        log.warn("closing service connection pool");
        dataSource.close();
    }


    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return dataSource.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return dataSource.isWrapperFor(iface);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return dataSource.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        dataSource.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        dataSource.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return dataSource.getLoginTimeout();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return dataSource.getParentLogger();
    }
}
