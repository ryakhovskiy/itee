package org.kr.intp.util.db;

import org.junit.Test;

/**
 *
 */
public class TimeControllerTest {

    @Test
    public void testGetServerUtcTimeMillis() throws Exception {

    }

    @Test
    public void testGetServerTimeMillis() throws Exception {

    }

    @Test
    public void testGetClinetUtcTimeMillis() throws Exception {

    }

    @Test
    public void testGetClinetTimeMillis() throws Exception {
        final TimeController timeController = new TimeController();
        final long currentUtcTimeMillis = timeController.getClinetUtcTimeMillis();
        final long serverUtcTimeMillis = timeController.getServerUtcTimeMillis();
        System.out.println(currentUtcTimeMillis);
        System.out.println(serverUtcTimeMillis);

    }
}