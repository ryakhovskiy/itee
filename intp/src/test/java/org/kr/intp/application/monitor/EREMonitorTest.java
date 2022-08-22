package org.kr.intp.application.monitor;

import org.kr.intp.IntpTestBase;

import java.util.HashMap;
import java.util.Map;

public class EREMonitorTest extends IntpTestBase {

    private static final String PROJECT_ID = "AUTOTST" + System.currentTimeMillis();
    private final EreMdManipulator ereMDManipulator = new EreMdManipulator();

    public void testERE() throws Exception {
        final Map<String, Integer> users = new HashMap<String, Integer>();
        users.put("all_users", 30);

        final EREMonitor ereMonitor = ereMDManipulator.createEREMonitor(PROJECT_ID,
                "Material Management", true, 120,
                "\"_SYS_BIC\".\"kr_test/CV_REAL_TIME_STOCK_VALUATION\"", 30000,
                10000, 60000, 30, 40, 2, 2, users);
        Thread t = new Thread(ereMonitor);
        t.start();
        Thread.sleep(60000);
        t.join();
        ereMDManipulator.deleteERE(PROJECT_ID);
    }

}