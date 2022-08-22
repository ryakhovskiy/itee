package org.kr.intp.util.db.pool;

import org.kr.intp.application.AppContext;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.config.IntpFileConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.util.Properties;

import static org.junit.Assert.assertNotNull;

public class DataSourceFactoryTest {

    @BeforeClass
    public static void setup() {
        IntpConfig config = new IntpConfig(IntpFileConfig.getResourceInstance().getPropertiesMap());
        AppContext.instance().setConfiguration(config);
    }

    @Test
    public void testDefaultDataSource() throws Exception {
        IDataSource dataSource = DataSourceFactory.newDefaultDataSource();
        assertNotNull(dataSource);
        Connection connection = dataSource.getConnection();
        assertNotNull(connection);
        connection.close();
        dataSource.close();
    }

    @Test(expected = RuntimeException.class)
    public void testWrongDataSource() throws Exception {
        Properties properties = AppContext.instance().getConfiguration().getJdbcPoolProperties();
        properties.setProperty("pool.class", "blabla-wrongclass");
        DataSourceFactory.newDataSource(properties, 0, 1000L, 0);
    }

    @Test
    public void testNewConnectionPool() throws Exception {
        IDataSource dataSource = DataSourceFactory.newDataSource(1);
        assertNotNull(dataSource);
        Connection connection = dataSource.getConnection();
        assertNotNull(connection);
        connection.close();
        dataSource.close();
    }

}