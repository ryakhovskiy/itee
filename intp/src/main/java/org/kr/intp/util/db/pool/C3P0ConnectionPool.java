package org.kr.intp.util.db.pool;

import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

public class C3P0ConnectionPool implements IDataSource {

    static IDataSource newInstance(Properties properties, Properties clientInfo) {
        return new C3P0ConnectionPool(properties, clientInfo);
    }

    static IDataSource newInstance(Properties properties) {
        return new C3P0ConnectionPool(properties);
    }

    private final Logger log = LoggerFactory.getLogger(C3P0ConnectionPool.class);
    private final DataSource dataSource;
    private final Properties clientInfo;

    private C3P0ConnectionPool(Properties properties, Properties clientInfo) {
        log.debug("instantiating C3P0 data source");
        final Properties copy = new Properties();
        copy.putAll(properties);
        this.dataSource = setupDataSource(copy);
        this.clientInfo = clientInfo;
    }

    private C3P0ConnectionPool(Properties properties) {
        this(properties, null);
    }

    @Override
    public Connection getConnection() throws SQLException {
        Connection connection = dataSource.getConnection();
        setClientInfo(connection);
        return connection;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        Connection connection = dataSource.getConnection(username, password);
        setClientInfo(connection);
        return connection;
    }

    private void setClientInfo(Connection connection) throws SQLClientInfoException {
        if (null == clientInfo)
            return;
        connection.setClientInfo(clientInfo);
    }

    private ComboPooledDataSource setupDataSource(Properties properties) {
        if (log.isTraceEnabled())
            log.trace("C3P0 DataSource properties: " + properties);
        try {
            final ComboPooledDataSource cpds = new ComboPooledDataSource();
            setPropertiesReflective(properties, cpds);
            String className = cpds.getDriverClass();
            log.info("Pool initialized for class: " + className);
            return cpds;
        } catch (Exception e) {
            throw new RuntimeException("Error while initializing connection pool", e);
        }
    }

    private void setPropertiesReflective(Properties properties, ComboPooledDataSource dataSource) throws Exception {
        Class<?> klass = dataSource.getClass();
        Method[] methods = klass.getMethods();
        for (Method method : methods) {
            String methodName = method.getName();
            if (!methodName.startsWith("set"))
                continue;
            if (methodName.equals("set") || methodName.equals("setProperties"))
                continue;
            if (methodName.length() <= 5)
                log.warn("Cannot set property: " + methodName);
            String key = Character.toLowerCase(methodName.charAt(3)) + methodName.substring(4);
            if (!properties.containsKey(key)) {
                log.warn("skipping property: " + key);
                continue;
            }
            String value = properties.getProperty(key);
            Class<?>[] paramClasses = method.getParameterTypes();
            if (paramClasses.length == 0) {
                log.warn("setter without parameter: " + methodName);
                continue;
            }
            if (paramClasses.length > 1) {
                log.warn("setter with more than 1 parameter: " + methodName);
                continue;
            }
            Class<?> paramType = paramClasses[0];
            String paramTypeName = paramType.getName();
            if (paramTypeName.equals("java.lang.String")) {
                method.invoke(dataSource, value);
            } else if (paramTypeName.equals("java.lang.Integer") || paramTypeName.equals("int")) {
                method.invoke(dataSource, Integer.parseInt(value));
            } else if (paramTypeName.equals("java.lang.Long") || paramTypeName.equals("long")) {
                method.invoke(dataSource, Long.parseLong(value));
            } else if (paramTypeName.equals("java.lang.Boolean") || paramTypeName.equals("boolean")) {
                method.invoke(dataSource, value.equalsIgnoreCase("true"));
            } else {
                log.warn("Unknown parameter type: " + paramType.getName());
            }
        }
    }

    public void close() {
        ((ComboPooledDataSource)dataSource).close();
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
