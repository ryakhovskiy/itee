package org.kr.itee.perfmon.ws.monitor;

import org.kr.itee.perfmon.ws.conf.ProcedureConfiguration;
import org.apache.log4j.Logger;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 */
public class SPExecutor {

    private final Logger log = Logger.getLogger(SPExecutor.class);
    private final boolean isDebugEnabled = log.isDebugEnabled();

    private final String url;
    private final String sql;

    public SPExecutor(ProcedureConfiguration config) {
        this(config.getUrl(), config.getSchema(), config.getName());
    }

    public SPExecutor(String url, String schema, String proc) {
        this.url = url;
        this.sql = String.format("call \"%s\".\"%s\"()", schema, proc);
    }

    public void call() throws SQLException {
        if (isDebugEnabled)
            log.debug("executing query: " + sql);
        try (Connection c = DriverManager.getConnection(url);
                CallableStatement s = c.prepareCall(sql)) {
            s.execute();
        }
    }

}
