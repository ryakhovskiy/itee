package org.kr.intp.application.monitor;

import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class HanaActiveConnectionsMonitor extends ActiveConnectionsMonitor {

    private static final HanaActiveConnectionsMonitor instance = new HanaActiveConnectionsMonitor();

    protected static HanaActiveConnectionsMonitor getInstance() {
        return instance;
    }

    private final Logger log = LoggerFactory.getLogger(HanaActiveConnectionsMonitor.class);

    private HanaActiveConnectionsMonitor() {
        super();
        log.info("HANA_AC_MONITOR created.");
    }

    @Override
    protected Map<String, Integer> monitor() throws Exception {
        if (log.isTraceEnabled())
            log.trace("AC_MONITOR, requesting stats...");
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.createStatement();
            resultSet = statement.executeQuery(SQL);
            final Map<String, Integer> data = new HashMap<String, Integer>();
            int total = 0;
            while (resultSet.next()) {
                final String user = resultSet.getString(1);
                final Integer connections = resultSet.getInt(2);
                total += connections;
                data.put(user, connections);
            }
            data.put("all_users", total);
            return data;
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private static final String SQL =
            "select user_name, count(user_name) \n" +
            "from M_CONNECTIONS \n" +
            "where CONNECTION_STATUS = 'RUNNING' \n" +
            "group by user_name";
}
