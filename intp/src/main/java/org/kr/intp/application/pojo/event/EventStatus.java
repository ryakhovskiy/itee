package org.kr.intp.application.pojo.event;

import org.kr.intp.application.AppContext;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kr on 30.01.14.
 */
public class EventStatus {

    private static final EventStatus EVENT_STATUS = new EventStatus();
    public static EventStatus getInstance() { return EVENT_STATUS; }
    private final Logger log = LoggerFactory.getLogger(EventStatus.class);
    private final String schema = AppContext.instance().getConfiguration().getIntpSchema();
    private final Map<String, Status> statusMap = new HashMap<String, Status>();

    private EventStatus() {
        try {
            init();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        }
    }

    private void init() throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet set = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.prepareStatement(String.format("select ID, NAME, DESCRIPTION from %s.RT_STATUS_DESC", schema));
            set = statement.executeQuery();
            while (set.next()) {
                int id = set.getInt(1);
                String name = set.getString(2).toUpperCase();
                String description = set.getString(3).toUpperCase();
                statusMap.put(name, new Status(id, name, description));
            }
        } finally {
            if (null != set)
                set.close();
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    public Status getTriggeredStatus() {
        return getStatusInfo("T");
    }

    public Status getWaitingStatus() {
        return getStatusInfo("W");
    }

    public Status getStartedStatus() {
        return getStatusInfo("S");
    }

    public Status getAggregatingStatus() {
        return getStatusInfo("A");
    }

    public Status getProcessingStatus() {
        return getStatusInfo("P");
    }

    public Status getFinishedStatus() {
        return getStatusInfo("F");
    }

    public Status getErrorStatus() {
        return getStatusInfo("E");
    }

    public Status getStatusInfo(char type) {
        String name = String.valueOf(type);
        return getStatusInfo(name);
    }

    private Status getStatusInfo(String name) {
        if (statusMap.containsKey(name))
            return statusMap.get(name);
        log.error("No status with info: " + name);
        return new Status(Integer.MIN_VALUE, name, "UNDEFINED");
    }

    public class Status {
        private final int id;
        private final String name;
        private final String description;
        private Status(int id, String name, String description) {
            this.id = id;
            this.name = name;
            this.description = description;
        }

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }
    }
}
