package org.kr.db.loader.ui.utils;

import org.kr.db.loader.ui.AppMain;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * Created bykron 26.01.2015.
 */
public class Stats2DbSaver implements AutoCloseable {

    private static final String SCHEMA = "INTP_STATS";
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss:SSS");
    private static final SimpleDateFormat kpiTimestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private final Connection connection;
    private final long time = AppMain.currentTimeMillis();
    private final int sid;

    public Stats2DbSaver(String url) throws SQLException {
        this.connection = DriverManager.getConnection(url);
        checkMD();
        this.sid = getNextSidAndSave();
    }

    public void savePowerCollectorInfo(List<String> lines) throws SQLException, ParseException {
        if (lines.size() <= 1)
            return;
        final String sql = String.format("insert into %s.POWER_DATA values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", SCHEMA);
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(sql);
            int i = 0;
            for (String line : lines) {
                if (i++ == 0) {
                    if (!line.toLowerCase().startsWith("system time")) {
                        addPwNullLine16(line, statement);
                        return;
                    }
                    continue; //first line -- headers
                }
                final String[] data = line.split(";", -1);
                if (data.length == 16)
                    parsePwLine16(statement, data);
                else if (data.length == 2)
                    parsePwLine2(connection, data);
                else
                    parsePwLine1(connection, line);
            }
            statement.executeBatch();
        } finally {
            if (null != statement)
                statement.close();
        }
    }

    private void parsePwLine1(Connection connection, String data) throws SQLException {
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement("insert into " + SCHEMA + ".POWER_SUMMARY values (?,?,?)");
            statement.setInt(1, sid);
            statement.setString(2, data);
            statement.setNull(3, Types.DECIMAL);
            statement.addBatch();
        } finally {
            if (null != statement)
                statement.close();
        }
    }

    private void parsePwLine2(Connection connection, String[] data) throws SQLException {
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement("insert into " + SCHEMA + ".POWER_SUMMARY values (?,?,?)");
            statement.setInt(1, sid);
            statement.setString(2, data[0]);
            statement.setDouble(3, Double.valueOf(data[1]));
            statement.addBatch();
        } finally {
            if (null != statement)
                statement.close();
        }
    }

    private void parsePwLine16(PreparedStatement statement, String[] data) throws ParseException, SQLException {
        final java.util.Date date = simpleDateFormat.parse(data[0]);
        final Timestamp timestamp = new Timestamp(date.getTime());
        final long rdtsc = Long.valueOf(data[1]);
        final double elapsedTime = Double.valueOf(data[2]);
        final int iafreq = Integer.valueOf(data[3]);
        double ppw = Double.valueOf(data[4]);
        double cpej = Double.valueOf(data[5]);
        double cpew = Double.valueOf(data[6]);
        double iapw = Double.valueOf(data[7]);
        double ciaej = Double.valueOf(data[8]);
        double ciaew = Double.valueOf(data[9]);
        double gtpw = Double.valueOf(data[10]);
        double cgtej = Double.valueOf(data[11]);
        double cgtew = Double.valueOf(data[12]);
        double drampw = Double.valueOf(data[13]);
        double cdramej = Double.valueOf(data[14]);
        double cdramew = Double.valueOf(data[15]);

        statement.setInt(1, sid);
        statement.setTimestamp(2, timestamp);
        statement.setLong(3, rdtsc);
        statement.setDouble(4, elapsedTime);
        statement.setInt(5, iafreq);
        statement.setDouble(6, ppw);
        statement.setDouble(7, cpej);
        statement.setDouble(8, cpew);
        statement.setDouble(9, iapw);
        statement.setDouble(10, ciaej);
        statement.setDouble(11, ciaew);
        statement.setDouble(12, gtpw);
        statement.setDouble(13, cgtej);
        statement.setDouble(14, cgtew);
        statement.setDouble(15, drampw);
        statement.setDouble(16, cdramej);
        statement.setDouble(17, cdramew);
        statement.setNull(18, Types.VARCHAR);
        statement.addBatch();
    }

    private void addPwNullLine16(String line, PreparedStatement statement) throws SQLException {
        statement.setInt(1, sid);
        statement.setNull(2, Types.TIMESTAMP);
        statement.setNull(3, Types.BIGINT);
        statement.setNull(4, Types.DECIMAL);
        statement.setNull(5, Types.INTEGER);
        statement.setNull(6, Types.DECIMAL);
        statement.setNull(7, Types.DECIMAL);
        statement.setNull(8, Types.DECIMAL);
        statement.setNull(9, Types.DECIMAL);
        statement.setNull(10, Types.DECIMAL);
        statement.setNull(11, Types.DECIMAL);
        statement.setNull(12, Types.DECIMAL);
        statement.setNull(13, Types.DECIMAL);
        statement.setNull(14, Types.DECIMAL);
        statement.setNull(15, Types.DECIMAL);
        statement.setNull(16, Types.DECIMAL);
        statement.setNull(17, Types.DECIMAL);
        statement.setString(18, line);
        statement.addBatch();
    }

    public void saveServiceMemoryInfo(List<String> lines) throws ParseException, SQLException {
        if (lines.size() <= 1)
            return;
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement("insert into " + SCHEMA + ".service_memory values (?,?,?,?)");
            final String[] timestamps = lines.remove(0).split("\t", -1);

            for (String line : lines) {
                final String[] data = line.split("\t", -1);
                final String component = data[0];
                for (int i = 2; i < data.length; i++) {
                    final long value = Long.valueOf(data[i]);
                    final Timestamp timestamp = new Timestamp(kpiTimestampFormat.parse(timestamps[i]).getTime());
                    statement.setInt(1, sid);
                    statement.setTimestamp(2, timestamp);
                    statement.setString(3, component);
                    statement.setLong(4, value);
                    statement.addBatch();
                }
            }
            statement.executeBatch();
        } finally {
            if (null != statement)
                statement.close();
        }
    }

    public void saveServiceAllocatorInfo(List<String> lines, String name) throws SQLException, ParseException {
        if (lines.size() <= 1)
            return;
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement("insert into " + SCHEMA + ".allocators values (?,?,?,?,?)");
            final String[] timestamps = lines.remove(0).split("\t", -1);
            for (String line : lines) {
                final String[] data = line.split("\t", -1);
                if (data.length < 3)
                    return;
                final String component = data[0];
                final String category = data[1];
                for (int i = 2; i < data.length; i++) {
                    final long value = Long.valueOf(data[i]);
                    final Timestamp timestamp = new Timestamp(kpiTimestampFormat.parse(timestamps[i]).getTime());
                    statement.setInt(1, sid);
                    statement.setTimestamp(2, timestamp);
                    statement.setString(3, component);
                    statement.setString(4, category);
                    statement.setLong(5, value);
                    statement.addBatch();
                }
            }
            statement.executeBatch();
        } finally {
            if (null != statement)
                statement.close();
        }
    }

    public void saveExpensiveStatementsInfo(List<String> lines) throws SQLException {
        if (lines.size() <= 1)
            return;
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement("insert into " + SCHEMA + ".expensive_statements values (?,?,?,?,?,?,?)");
            int i = 0;
            for (String line : lines) {
                if (i++ == 0)
                    continue; //first line -- headers;
                final String[] data = line.split("\t", -1);
                if (data.length != 6)
                    continue; //just skip it
                statement.setInt(1, sid);
                statement.setString(2, data[0]);
                statement.setString(3, data[1]);
                statement.setString(4, data[2]);
                statement.setInt(5, Integer.valueOf(data[3]));
                statement.setInt(6, Integer.valueOf(data[4]));
                statement.setInt(7, Integer.valueOf(data[5]));
                statement.addBatch();
            }
            statement.executeBatch();
        } finally {
            if (null != statement)
                statement.close();
        }
    }

    public void saveVPerformanceInfo(List<String> slines) throws SQLException, ParseException {
        if (slines.size() <= 1)
            return;
        final String l = slines.get(0);
        final String[] lines = l.split("\r\n", -1);
        String[] data = null;
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement("insert into " + SCHEMA + ".VPERFORMANCE_COUNTERS values (?,?,?,?)");
            final String[] timestamps = lines[0].split("\t", -1); //first line -- timestamps
            for (int i = 1; i < lines.length; i++) {
                final String line = lines[i];
                /*String[]*/ data = line.split("\t", -1);
                String kpiName = data[0];
                for (int j = 1; j < data.length; j++) {
                    if (data[j].indexOf('.') > 0)
                        data[j] = data[j].substring(0, data[j].indexOf('.'));
                    if (data[j].indexOf(',') > 0)
                        data[j] = data[j].substring(0, data[j].indexOf(','));
                    long kpiValue = Long.valueOf(data[j]);
                    statement.setInt(1, sid);
                    Timestamp timestamp = new Timestamp(kpiTimestampFormat.parse(timestamps[j]).getTime());
                    statement.setTimestamp(2, timestamp);
                    statement.setString(3, kpiName);
                    statement.setLong(4, kpiValue);
                    statement.addBatch();
                }
            }
            statement.executeBatch();
        } finally {
            if (null != statement)
                statement.close();
        }
    }

    public void saveCommonData(String data) throws SQLException {
        final String[] lines = data.split("\n", -1);
        saveServerInfo(lines);
    }

    private void saveServerInfo(String[] lines) throws SQLException {
        if (lines.length < 2)
            return;
        PreparedStatement statement = null;
        try {
            final String[] headers = lines[0].split("\t", -1);
            final String[] values = lines[1].split("\t", -1);
            if (values.length != headers.length) {
                System.err.println("Server Info is formatted wrongly, skipping");
                return;
            }
            statement = connection.prepareStatement("insert into " + SCHEMA + ".SERVER_INFO values (?,?,?)");
            for (int i = 0; i < headers.length; i++) {
                statement.setInt(1, sid);
                statement.setString(2, headers[i]);
                statement.setString(3, values[i]);
                statement.addBatch();
            }
            if (lines.length > 4)
                saveTableCommonInfo(statement, lines[4], lines[5]);
            statement.executeBatch();
        } finally {
            if (null != statement)
                statement.close();
        }
    }

    private void saveTableCommonInfo(PreparedStatement statement, String storageSize1, String storageSize2) throws SQLException {
        final String rowLine = storageSize1.toLowerCase().startsWith("row") ? storageSize1 : storageSize2;
        final String columnLine = storageSize1.toLowerCase().startsWith("column") ? storageSize1 : storageSize2;
        String rowSize = rowLine.split("\t", -1)[1].trim();
        String colSize = columnLine.split("\t", -1)[1].trim();

        statement.setInt(1, sid);
        statement.setString(2, "ROW_STORE_SIZE");
        statement.setString(3, rowSize);
        statement.addBatch();

        statement.setInt(1, sid);
        statement.setString(2, "COLUMN_STORE_SIZE");
        statement.setString(3, colSize);
        statement.addBatch();
    }

    private void checkMD() throws SQLException {
        checkSchema(connection);
        checkTable(connection, SCHEMA, "STATS_INFO", STATS_INFO_SQL);
        checkTable(connection, SCHEMA, "POWER_DATA", POWER_DATA_SQL);
        checkTable(connection, SCHEMA, "EXPENSIVE_STATEMENTS", EXPENSIVE_STATEMENTS_SQL);
        checkTable(connection, SCHEMA, "POWER_SUMMARY", POWER_SUMMARY_SQL);
        checkTable(connection, SCHEMA, "VPERFORMANCE_COUNTERS", VPERFORMANCE_COUNTERS_SQL);
        checkTable(connection, SCHEMA, "SERVICE_MEMORY", SERVICE_MEMORY_SQL);
        checkTable(connection, SCHEMA, "ALLOCATORS", ALLOCATORS_SQL);
        checkTable(connection, SCHEMA, "SERVER_INFO", SERVER_INFO_SQL);
    }

    private void checkSchema(Connection connection) throws SQLException {
        PreparedStatement pstmnt = null;
        ResultSet set = null;
        Statement statement = null;
        try {
            pstmnt = connection.prepareStatement("select * from schemas where schema_name = ?");
            pstmnt.setString(1, SCHEMA);
            set = pstmnt.executeQuery();
            if (set.next())
                return;
            statement = connection.createStatement();
            statement.execute("create schema " + SCHEMA);
        } finally {
            if (null != set)
                set.close();
            if (null != statement)
                statement.close();
            if (null != pstmnt)
                pstmnt.close();
        }
    }

    private void checkTable(Connection connection, String schema, String tableName, String tableSql) throws SQLException {
        final String sql = "select * from m_tables where schema_name = ? and table_name = ?";
        PreparedStatement pstmnt = null;
        ResultSet set = null;
        Statement statement = null;
        try {
            pstmnt = connection.prepareStatement(sql);
            pstmnt.setString(1, schema);
            pstmnt.setString(2, tableName);
            set = pstmnt.executeQuery();
            if (set.next())
                return;
            statement = connection.createStatement();
            statement.execute(tableSql);
        } finally {
            if (null != set)
                set.close();
            if (null != statement)
                statement.close();
            if (null != pstmnt)
                pstmnt.close();
        }
    }

    private int getNextSidAndSave() throws SQLException {
        Statement statement = null;
        PreparedStatement pstmnt = null;
        ResultSet set = null;
        try {
            statement = connection.createStatement();
            set = statement.executeQuery("select ifnull(max(id), 0) + 1 from " + SCHEMA + ".stats_info");
            int sid = 1;
            if (set.next())
                sid = set.getInt(1);
            pstmnt = connection.prepareStatement("insert into " + SCHEMA + ".stats_info values (?,?,?,?)");
            pstmnt.setInt(1, sid);
            pstmnt.setTimestamp(2, new Timestamp(time));
            pstmnt.setNull(3, Types.INTEGER);
            pstmnt.setNull(4, Types.VARCHAR);
            pstmnt.execute();
            return sid;
        } finally {
            if (null != set)
                set.close();
            if (null != statement)
                statement.close();
            if (null != pstmnt)
                pstmnt.close();
        }
    }

    public void close() {
        if (null != connection) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private static final String POWER_DATA_SQL = "create column table INTP_STATS.POWER_DATA\n" +
            "\t(SID INTEGER null,\n" +
            "\tSYSTEM_TIME TIMESTAMP null,\n" +
            "\tRDTSC BIGINT null,\n" +
            "\tELAPSED_TIME DECIMAL (19,6) null,\n" +
            "\tIA_FREQUENCY_0 INTEGER null,\n" +
            "\tPROCESSOR_POWER DECIMAL (19,6) null,\n" +
            "\tCUMULATIVE_PROCESSOR_ENERGY_JOULES DECIMAL (19,6) null,\n" +
            "\tCUMULATIVE_PROCESSOR_ENERGY_UWH DECIMAL (19,6) null,\n" +
            "\tIA_POWER_WATT DECIMAL (19,6) null,\n" +
            "\tCUMULATIVE_IA_ENERGY_JOULES DECIMAL (19,6) null,\n" +
            "\tCUMULATIVE_IA_ENERGY_UWH DECIMAL (19,6) null,\n" +
            "\tGT_POWER_WATT DECIMAL (19,6) null,\n" +
            "\tCUMULATIVE_GT_ENERGY_JOULES DECIMAL (19,6) null,\n" +
            "\tCUMULATIVE_GT_ENERGY_UWH DECIMAL (19,6) null,\n" +
            "\tDRAM_POWER_WATT DECIMAL (19,6) null,\n" +
            "\tCUMULATIVE_DRAM_ENERGY_JOULES DECIMAL (19,6) null,\n" +
            "\tCUMULATIVE_DRAM_ENERGY_UWH DECIMAL (19,6) null,\n" +
            "\tCOMMENTS VARCHAR (255) null)";

    private static final String EXPENSIVE_STATEMENTS_SQL = "create column table INTP_STATS.EXPENSIVE_STATEMENTS\n" +
            "\t(SID INTEGER null,\n" +
            "\tSTATEMENT TEXT null,\n" +
            "\tUSER VARCHAR (255) null,\n" +
            "\tSCHEMA VARCHAR (255) null,\n" +
            "\tAVG_TIME INTEGER null,\n" +
            "\tMAX_TIME INTEGER null,\n" +
            "\tMIN_TIME INTEGER null)";

    private static final String STATS_INFO_SQL = "create column table INTP_STATS.STATS_INFO \n" +
            "(ID INTEGER not null,\n" +
            "START_TIME TIMESTAMP null,\n" +
            "SERVER_ID INTEGER null,\n" +
            "COMMENTS VARCHAR (4000) null,\n" +
            "primary key (ID))";

    private static final String POWER_SUMMARY_SQL = "create column table INTP_STATS.POWER_SUMMARY\n" +
            "\t(SID INTEGER null,\n" +
            "\t NAME VARCHAR (255) null,\n" +
            "\t VALUE DECIMAL (19,6) null)";

    private static final String VPERFORMANCE_COUNTERS_SQL = "create column table INTP_STATS.VPERFORMANCE_COUNTERS\n" +
            "\t(SID INTEGER null,\n" +
            "\tUTC_TIME TIMESTAMP null,\n" +
            "\tKPI VARCHAR (255) null,\n" +
            "\tVALUE BIGINT null)";

    private static final String SERVICE_MEMORY_SQL = "create column table INTP_STATS.SERVICE_MEMORY\n" +
            "\t(SID INTEGER null,\n" +
            "\t TIMESTAMP TIMESTAMP null,\n" +
            "\t COMPONENT VARCHAR (255) null,\n" +
            "\t VALUE BIGINT null)";

    private static final String ALLOCATORS_SQL = "create column table INTP_STATS.ALLOCATORS\n" +
            "\t(SID INTEGER null,\n" +
            "\tTIMESTAMP TIMESTAMP null,\n" +
            "\tCOMPONENT VARCHAR (255) null,\n" +
            "\tCATEGORY VARCHAR (255) null,\n" +
            "\tVALUE BIGINT null)";

    private static final String SERVER_INFO_SQL = "create column table INTP_STATS.SERVER_INFO\n" +
            "\t(SID INTEGER null,\n" +
            "\tPROPERTY VARCHAR (255) null,\n" +
            "\tVALUE VARCHAR (255) null)";
}
