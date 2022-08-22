package org.kr.intp.util.db.pool;

import org.junit.After;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class ServiceConnectionPoolTest {

    private static final String SOCKS_PROXY_KEY = "socksProxyHost";
    private static final String DUMMY_PROXY = "127.0.0.1";
    private static final Properties ORIGINAL_SYSTEM_PROPERTIES =
            (Properties)System.getProperties().clone();

    @After
    public void tearDown() {
        //make sure that the original value is set back before starting next test
        System.setProperties(ORIGINAL_SYSTEM_PROPERTIES);

    }

    @Test
    public void testGetConnection() throws Exception {
        ServiceConnectionPool pool = ServiceConnectionPool.instance();
        Thread.sleep(15000L);
        Exception e = null;
        try {
            execDummyQuery(pool);
        } catch (SQLException ex) {
            e = ex;
        }
        assertNotNull(e);
        Thread.sleep(15000L);
        execDummyQuery(pool);
        pool.close();
    }

    private void execDummyQuery(DataSource dataSource) throws SQLException {
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select 0 from dummy");
        resultSet.next();
        final int val = resultSet.getInt(1);
        assertTrue(0 == val);
        resultSet.close();
        statement.close();
        connection.close();
    }

}