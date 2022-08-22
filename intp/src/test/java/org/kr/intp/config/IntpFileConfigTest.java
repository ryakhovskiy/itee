package org.kr.intp.config;

import org.kr.intp.IntpTestBase;
import org.kr.intp.application.AppContext;
import org.junit.Test;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;

import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 9/14/13
 * Time: 3:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class IntpFileConfigTest extends IntpTestBase {

    private final IntpConfig config = AppContext.instance().getConfiguration();
    private final Logger log = LoggerFactory.getLogger(IntpFileConfigTest.class);

    @Test
    public void testGetJdbcURL() throws Exception {
        String jdbcUrl = config.getJdbcPoolProperties().getProperty("jdbcUrl");
        log.info(jdbcUrl);
        assertNotNull("IntpFileConfig returns nullable JdbcURL", jdbcUrl);
    }

    @Test
    public void testConfig() {
        char type = config.getIntpType();
        assert type == 'D';
    }

    @Test
    public void testJdbcProperties() {
        Properties properties = config.getJdbcPoolProperties();
        assertNotNull(properties);
    }



}
