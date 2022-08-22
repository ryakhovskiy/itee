package org.kr.db.loader.ui.monitor;

import org.kr.db.loader.ui.derby.DerbySaver;

import java.util.concurrent.CountDownLatch;

/**
 * Created bykron 04.12.2014.
 */
public class ServiceAllocMonitor extends MonitorBase {

    private static final String STMNT_TMPLT =
            "select '%s', CATEGORY, DEPTH, sum(INCLUSIVE_SIZE_IN_USE) as INCLUSIVE_SIZE_IN_USE, CURRENT_UTCTIMESTAMP \n" +
            "from M_HEAP_MEMORY hm\n" +
            "\tinner join m_services s on hm.host = s.host and hm.port = s.port\n" +
            "where DEPTH <= 2 and s.service_name = '%s'\n" +
            "group by CATEGORY, DEPTH, CURRENT_UTCTIMESTAMP";

    private final String sql;
    private final String serviceName;

    public ServiceAllocMonitor(String serviceName, String url, int topx, CountDownLatch startGate,
                               long monitor_sleep_ms, DerbySaver derbySaver) throws Exception {
        super(url, topx, startGate, monitor_sleep_ms, derbySaver);
        this.serviceName = serviceName;
        this.sql = String.format(STMNT_TMPLT, serviceName, serviceName);
    }

    @Override
    protected String getStatementQuery() {
        return sql;
    }

    @Override
    protected String getCol1title() {
        return "COMPONENT";
    }

    @Override
    protected String getCol2title() {
        return "CATEGORY";
    }

    public String getServiceName() {
        return serviceName;
    }
}
