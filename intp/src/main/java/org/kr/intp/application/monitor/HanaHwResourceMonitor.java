package org.kr.intp.application.monitor;

import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HanaHwResourceMonitor extends HwResourceMonitor {

    private static final HanaHwResourceMonitor instance = new HanaHwResourceMonitor();

    protected static HanaHwResourceMonitor getInstance() {
        return instance;
    }

    private final Logger log = LoggerFactory.getLogger(HanaHwResourceMonitor.class);
    private final boolean isTraceEnabled = log.isTraceEnabled();

    private HanaHwResourceMonitor() {
        super();
        log.info("HANA_HW_MONITOR created.");
    }

    protected int[] monitor() throws SQLException {
        if (isTraceEnabled)
            log.trace("HW_MONITOR, requesting stats...");
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery(HW_RESOURCES_SQL);
            if (!resultSet.next())
                return new int[3];
            final int cpu = resultSet.getInt(1);
            final int mem = resultSet.getInt(2);
            final int memabs = resultSet.getInt(3);
            return new int[] { cpu, mem, memabs };
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private static final String HW_RESOURCES_SQL =
            "select \n" +
            "    case when ss.process_cpu > 0 then ss.process_cpu else 0 end as PROCESS_CPU, \n" +
            "    round((sm.TOTAL_MEMORY_USED_SIZE / sm.EFFECTIVE_ALLOCATION_LIMIT) * 100) as ALLOCATED_PERCENTAGE,\n" +
            "    round(sm.TOTAL_MEMORY_USED_SIZE / (1024 * 1024 * 1024)) as ALLOCATED_GB\n" +
            "from sys.M_SERVICE_MEMORY sm \n" +
            "    inner join sys.m_service_statistics ss on sm.SERVICE_NAME = ss.SERVICE_NAME \n" +
            "where sm.SERVICE_NAME = 'indexserver'";
}
