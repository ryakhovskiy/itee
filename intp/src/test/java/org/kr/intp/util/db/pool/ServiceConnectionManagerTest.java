package org.kr.intp.util.db.pool;

import org.junit.Test;

import java.sql.Connection;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ServiceConnectionManagerTest {

    @Test
    public void testGetConnection() throws Exception {
        ServiceConnectionPool connectionManager = ServiceConnectionPool.instance();
        assertNotNull(connectionManager);
        long start = System.currentTimeMillis();
        Connection connection = connectionManager.getConnection();
        long duration1 = System.currentTimeMillis() - start;
        System.out.printf("<============== %d%n", duration1);
        assertNotNull(connection);
        connection.close();

        start = System.currentTimeMillis();
        connection = connectionManager.getConnection();
        long duration2 = System.currentTimeMillis() - start;
        System.out.printf("<============== %d%n", duration2);
        assertNotNull(connection);
        connection.close();

        assertTrue(duration1 > duration2); //pooled connection should be cheaper (depending on config)

        connectionManager.close();
    }

}