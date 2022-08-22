package org.kr.db.loader.ui.monitor;

import org.kr.db.loader.ui.derby.DerbySaver;

import java.util.concurrent.CountDownLatch;

/**
 * Created by kr on 5/27/2014.
 */
public class ServicesMemoryMonitor extends MonitorBase {

    private static final String STATEMENT = "select SERVICE_NAME||'-'||HOST||':'||PORT as COMPONENT, \n" +
            "'N/A' as CATEGORY, 0 as DEPTH, TOTAL_MEMORY_USED_SIZE as SIZE, \n" +
            "CURRENT_UTCTIMESTAMP \n" +
            "from M_SERVICE_MEMORY \n" +
            "order by 1";

    public ServicesMemoryMonitor(String url, CountDownLatch startGate, long monitor_sleep_ms,
                                 DerbySaver derbySaver) throws Exception {
        super(url, 6, startGate, monitor_sleep_ms, derbySaver);
    }

    @Override
    protected String getStatementQuery() {
        return STATEMENT;
    }

    @Override
    protected String getCol1title() {
        return "COMPONENT";
    }

    @Override
    protected String getCol2title() {
        return "";
    }
}
