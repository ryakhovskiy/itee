package org.kr.itee.perfmon.ws.utils;

import org.kr.itee.perfmon.ws.monitor.MonitorElement;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created bykron 14.07.2015.
 */
public class DerbySaver {

    private static final String DERBY_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String DERBY_URL = "jdbc:derby:./derbydb/measurements;create=true";
    private static final String SAVE_SQL = "insert into stats values (?, ?, ?)";
    private static final String GET_MONITOR_ID_SQL = "select id from monitors where category = ? and component = ? and depth = ?";
    public static DerbySaver getInstance() { return new DerbySaver(); }

    private final Logger log = Logger.getLogger(DerbySaver.class);

    private boolean initialized = false;
    private Connection connection;
    private PreparedStatement saveStatement;
    private PreparedStatement getMonitorIdStatement;
    private final Object monitorIDsSync = new Object();
    private final Map<MonitorElement, Integer> monitorIDs = new HashMap<>();

    private DerbySaver() {
        log.debug("initializing Derby database...");
        try {
            Class.forName(DERBY_DRIVER);
            connection = DriverManager.getConnection(DERBY_URL);
            createTables();
            saveStatement = connection.prepareStatement(SAVE_SQL);
            getMonitorIdStatement = connection.prepareStatement(GET_MONITOR_ID_SQL);
            initialized = true;
        } catch (Exception e) {
            log.error("Cannot initialize Derby Database", e);
            initialized = false;
        }
    }

    private void createTables() throws SQLException {
        final DatabaseMetaData databaseMetaData = connection.getMetaData();
        try (Statement statement = connection.createStatement();
            ResultSet resultSet = databaseMetaData.getTables(null, null, null, new String[]{"TABLE"})) {
            boolean monitorIDsExists = false;
            boolean statsExists = false;
            while (resultSet.next()) {
                if (resultSet.getString("TABLE_NAME").toLowerCase().equals("monitors"))
                    monitorIDsExists = true;
                if (resultSet.getString("TABLE_NAME").toLowerCase().equals("stats"))
                    statsExists = true;
            }
            if (!monitorIDsExists)
                statement.execute("create table monitors (id int, category varchar(128), component varchar(128), depth int)");
            loadMonitorIDs();

            if (!statsExists)
                statement.execute("create table stats (id int, ts timestamp, val bigint)");
            else
                statement.execute("truncate table stats");
        }
    }

    public List<String> getLines() throws SQLException {
        final List<String> lines = new ArrayList<String>();
        final String tssql = "select ts from stats where id = ?";
        final String vsql = "select val from stats where id = ?";
        PreparedStatement tsstmnt = null;
        PreparedStatement vstmnt = null;
        ResultSet resultSet = null;
        StringBuilder header = new StringBuilder();
        try {
            int id = monitorIDs.values().iterator().next();
            tsstmnt = connection.prepareStatement(tssql);
            tsstmnt.setInt(1, id);
            resultSet = tsstmnt.executeQuery();
            header.append("COMPONENT").append('\t').append("CATEGORY").append('\t');
            while (resultSet.next())
                header.append(resultSet.getTimestamp(1)).append('\t');
            lines.add(header.toString());
            resultSet.close();
            vstmnt = connection.prepareStatement(vsql);
            for (Map.Entry<MonitorElement, Integer> e : monitorIDs.entrySet()) {
                id = monitorIDs.get(e.getKey());
                vstmnt.setInt(1, id);
                resultSet = vstmnt.executeQuery();
                StringBuilder data = new StringBuilder();
                data.append(e.getKey().getComponent()).append('\t').append(e.getKey().getCategory()).append('\t');
                while (resultSet.next())
                    data.append(resultSet.getLong(1)).append('\t');
                lines.add(data.toString());
                data.setLength(0);
            }
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != tsstmnt)
                tsstmnt.close();
            if (null != vstmnt)
                vstmnt.close();
        }
        return lines;
    }

    private void loadMonitorIDs() throws SQLException {
        try (Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select id, category, component, depth from monitors")) {
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String category = resultSet.getString("category");
                String component = resultSet.getString("component");
                int depth = resultSet.getInt("depth");
                MonitorElement e = new MonitorElement(component, category, depth);
                synchronized (monitorIDsSync) {
                    monitorIDs.put(e, id);
                }
            }
        }
    }

    public void saveStats(MonitorElement e, Timestamp timestamp, long value) throws SQLException {
        if (!initialized) {
            System.out.println("DerbySaver is not initialized");
            return;
        }
        final int id = getMonitorId(e);
        saveStatement.setInt(1, id);
        saveStatement.setTimestamp(2, timestamp);
        saveStatement.setLong(3, value);
        saveStatement.execute();
    }

    private int getMonitorId(MonitorElement e) throws SQLException {
        int id = 0;
        synchronized (monitorIDsSync) {
            if (monitorIDs.containsKey(e))
                id = monitorIDs.get(e);
            else {
                id = getMonitorIdFromDb(e);
                if (id >= 0)
                    monitorIDs.put(e, id);
                else
                    id = saveMonitorToDb(e);
            }
        }
        return id;
    }

    private int saveMonitorToDb(MonitorElement e) throws SQLException {
        int maxId = 1;
        try (Statement getMaxIdStmnt = connection.createStatement();
             PreparedStatement statement = connection.prepareStatement("insert into monitors values (?,?,?,?)");
             ResultSet resultSet = getMaxIdStmnt.executeQuery("select max(id) from monitors")) {

            if (resultSet.next())
                maxId = resultSet.getInt(1) + 1;
            statement.setInt(1, maxId);
            statement.setString(2, e.getCategory());
            statement.setString(3, e.getComponent());
            statement.setInt(4, e.getDepth());
            statement.execute();
        }
        return maxId;
    }

    private int getMonitorIdFromDb(MonitorElement e) throws SQLException {
        ResultSet resultSet = null;
        try {
            getMonitorIdStatement.setString(1, e.getCategory());
            getMonitorIdStatement.setString(2, e.getComponent());
            getMonitorIdStatement.setInt(3, e.getDepth());
            resultSet = getMonitorIdStatement.executeQuery();
            if (resultSet.next())
                return resultSet.getInt("id");
            else return -1;
        }  finally {
            if (null != resultSet)
                resultSet.close();
        }
    }

    public void close() {
        if (null != saveStatement)
            try {
                saveStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        if (null != connection)
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
    }


}
