package org.kr.itee.perfmon.ws.monitor;

import org.kr.itee.perfmon.ws.conf.ConfigurationManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.concurrent.Callable;

/**
 * Created bykron 23.07.2014.
 */
public class RunLogMonitor implements Callable<Long> {

    private static final String FILE = "mon.properties";

    private static final String query_template = "select max(proc_ms)%n" +
            "from (%n" +
            "\tselect top 5 t.upd_ts, nano100_between(t.triggered, f.finished) / 10000 as proc_ms%n" +
            "\tfrom %s t%n" +
            "\t\tinner join %s f on t.uuid = f.uuid%n" +
            "\twhere t.status = 0 and f.status = 50%n" +
            "\torder by t.upd_ts desc)";

    private final Logger log = Logger.getLogger(RunLogMonitor.class);
    private final String query;
    private final boolean enabled;
    private final ConfigurationManager config = ConfigurationManager.instance();
    private Connection connection;
    private PreparedStatement statement;

    public RunLogMonitor(String url) throws IOException, SQLException {
        this.enabled = config.isDumpRunLogEnabled();
        final String table = config.getDumpRunLogTable();

        query = String.format(query_template, table, table);
        if (table.length() == 0)
            return;
        connection = DriverManager.getConnection(url);
        statement = connection.prepareStatement(query);
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public Long call() throws Exception {
        ResultSet resultSet = null;
        try {
            resultSet = statement.executeQuery();
            if (resultSet.next())
                return resultSet.getLong(1);
            else
                return 0L;
        } finally {
            if (null != resultSet)
                resultSet.close();
        }
    }

    public void close() {
        if (null != statement)
            try {
                statement.close();
            } catch (SQLException e) {
                log.error(e);
            }

        if (null != connection)
            try {
                connection.close();
            } catch (SQLException e) {
                log.error(e);
            }
    }
}
