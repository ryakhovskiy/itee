package org.kr.intp.util.db;

import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.*;
import java.util.TimeZone;

/**
 *
 */
public class TimeController {

    private final static long clientUtcOffset;
    static {
        clientUtcOffset = getClientUtcOffset();
    }

    private static final TimeController instance = new TimeController();

    public static TimeController getInstance() {
        return instance;
    }

    public static long getClinetUtcTimeMillis() {
        return System.currentTimeMillis() + clientUtcOffset;
    }

    public static long getClinetTimeMillis() {
        return System.currentTimeMillis();
    }

    private final Logger log = LoggerFactory.getLogger(TimeController.class);
    private final long serverOffset;
    private final long serverUtcOffset;

    protected TimeController() {
        this.serverOffset = getServerOffsetWrapped();
        this.serverUtcOffset = getServerUtcOffsetWrapped();
    }

    public long getServerUtcTimeMillis() {
        return System.currentTimeMillis() - serverUtcOffset;
    }

    public long getServerTimeMillis() {
        return System.currentTimeMillis() - serverOffset;
    }

    private long getServerOffsetWrapped() {
        try {
            return getServerOffset();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            return 0;
        }
    }

    private long getServerUtcOffsetWrapped() {
        try {
            return getServerUtcOffset();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            return getClientUtcOffset();
        }
    }

    private long getServerOffset() throws SQLException {
        ResultSet resultSet = null;
        try (Connection connection = ServiceConnectionPool.instance().getConnection();
             Statement statement = connection.createStatement()) {
            resultSet = statement.executeQuery("select current_timestamp from dummy");
            return readOffsetFromResultSet(resultSet);
        } finally {
            if (null != resultSet)
                resultSet.close();
        }
    }

    private long getServerUtcOffset() throws SQLException {
        ResultSet resultSet = null;
        try (Connection connection = ServiceConnectionPool.instance().getConnection();
             Statement statement = connection.createStatement()) {
            resultSet = statement.executeQuery("select current_utctimestamp from dummy");
            return readOffsetFromResultSet(resultSet);
        } finally {
            if (null != resultSet)
                resultSet.close();
        }
    }

    private long readOffsetFromResultSet(ResultSet resultSet) throws SQLException {
        final long clientTime = System.currentTimeMillis();
        if (resultSet.next()) {
            final Timestamp ts = resultSet.getTimestamp(1);
            final long time = ts.getTime();
            return clientTime - time;
        } else
            return 0;
    }

    private static long getClientUtcOffset() {
        final TimeZone tz = TimeZone.getDefault();
        return (-1) * tz.getOffset(System.currentTimeMillis());
    }
}
