package org.kr.db.loader.ui.monitor;

import org.kr.db.loader.ui.conf.ProcedureConfiguration;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 *
 */
public class SPExecutor {

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
        System.out.println("executing query: " + sql);
        try (Connection c = DriverManager.getConnection(url);
                CallableStatement s = c.prepareCall(sql)) {
            s.execute();
        }
    }

}
