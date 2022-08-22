package org.kr.intp.util.db.pool;

import org.junit.Test;

import java.sql.Connection;

import static org.junit.Assert.assertNotNull;

/**
 *
 */
public class SAPHistoricalDataSourceTest {

    @Test
    public void getConnection() throws Exception {
        SAPHistoricalDataSource dataSource = new SAPHistoricalDataSource(System.currentTimeMillis(), 1000L, 1000L, 0);
        Connection connection = dataSource.getConnection();
        assertNotNull(connection);
        connection.close();
        connection = dataSource.getConnection();
        assertNotNull(connection);
        connection.close();
        connection = dataSource.getConnection();
        assertNotNull(connection);
        connection.close();
        Thread.sleep(5010);
        connection = dataSource.getConnection();
        assertNotNull(connection);
        connection.close();
        dataSource.close();
    }

}