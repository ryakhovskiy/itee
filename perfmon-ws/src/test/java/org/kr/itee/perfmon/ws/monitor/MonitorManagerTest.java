package org.kr.itee.perfmon.ws.monitor;

import org.junit.Test;

/**
 *
 */
public class MonitorManagerTest {

    @Test
    public void testRun() throws Exception {
        final String driver = "com.sap.db.jdbc.Driver";
        final String url = "jdbc:sap://10.118.38.2:30215?user=DGH_ADMIN&password=xxx";
        final String powerConsumptionServer = "10.118.38.2";
        final int powerConcumptionPort = 9999;
        final long powerConcumptionDuration = 300000L;
        final int topx = 3;
        final long monitorSleepMS = 5000L;
        final int monitorAge = 120;
        final long expensiveStatementsAge = 5000L;


        final MonitorManager monitorManager = new MonitorManager(driver, url, powerConsumptionServer, powerConcumptionPort,
                powerConcumptionDuration, topx,  monitorSleepMS, monitorAge, expensiveStatementsAge, "TEST");

        new Thread(monitorManager).start();

        Thread.sleep(10000L);

        monitorManager.shutdown();
        Thread.sleep(30000L);
    }

}