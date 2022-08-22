package org.kr.intp.util.db.pool;

import org.kr.intp.application.pojo.job.JobProperties;
import org.junit.Test;

import java.sql.Connection;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 *
 */
public class C3P0ConnectionPoolTest {

    @Test(timeout = 10000L)
    public void testClientInfo() throws Exception {
        String key = ("key" + System.currentTimeMillis()).toUpperCase();
        String value = "value" + System.currentTimeMillis();
        Properties clientInfo = new Properties();
        clientInfo.setProperty(key, value);

        IDataSource dataSource = DataSourceFactory.newDataSource(5, clientInfo, JobProperties.ConnectionType.POOL, 0, 1000, 0);
        Connection connection = dataSource.getConnection();
        assertNotNull(connection);
        Properties properties = connection.getClientInfo();
        assertNotNull(properties);
        assertTrue(properties.containsKey(key));
        assertEquals(properties.getProperty(key), value);
        connection.close();
        dataSource.close();
    }

}