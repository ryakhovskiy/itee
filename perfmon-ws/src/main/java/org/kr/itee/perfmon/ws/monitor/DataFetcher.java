package org.kr.itee.perfmon.ws.monitor;

import java.sql.*;

/**
 * Created bykron 10.06.2014.
 */
public class DataFetcher {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private static final String TABLE_SIZE_OVERVIEW = "select table_type, sum(table_size)\n" +
            "from m_tables\n" +
            "group by table_type";

    private static final String TOTAL_VIEWS_OVERVIEW = "select upper(object_suffix) AS TYPE, count(object_name)\n" +
            "from _SYS_REPO.ACTIVE_OBJECT \n" +
            "where package_id not like 'sap.%' and package_id not like 'system-local.%' \n" +
            " and object_suffix in ('calculationview', 'analyticview', 'attributeview', 'procedure', 'hdbprocedure') \n" +
            " group by object_suffix";

    private static final String OBJECTS_OBERVIEW = "select t.schema_name, t.table_type||' TABLE' as OBJECT_TYPE, " +
            "count(table_name) as COUNT, sum(t.table_size) as SIZE\n" +
            "from m_tables t\n" +
            "group by t.schema_name, t.table_type\n" +
            "union all\n" +
            "select schema_name, 'STORED PROCEDURE', count(procedure_name), null\n" +
            "from procedures\n" +
            "group by schema_name\n" +
            "union all\n" +
            "select package_id, upper(object_suffix), count(object_name), null\n" +
            "from \"_SYS_REPO\".\"ACTIVE_OBJECT\"\n" +
            "where package_id not like 'sap.%' and package_id not like 'system-local.%'\n" +
            " and object_suffix in ('calculationview', 'analyticview', 'attributeview', 'procedure', 'hdbprocedure')\n" +
            "group by package_id, object_suffix\n" +
            "order by 1, 2";

    private static final String SYSTEM_OVERVIEW = "select cmr.value as \"CPU Manufacturer\",\n" +
            "\t\tcml.value as \"CPU Model\", \n" +
            "\t\tccr.value as \"CPU Cores\",\n" +
            "\t\tct.value as \"CPU Threads\",\n" +
            "\t\tto_nvarchar(cck.value)||' MHz' as \"CPU Clock\",\n" +
            "\t\tto_nvarchar(round(mp.value / (1024 * 1024), 0))||' MB' as \"Physical Memory\",\n" +
            "\t\tto_nvarchar(round(ms.value / (1024 * 1024), 0))||' MB' as \"Swap Memory\",\n" +
            "\t\tosn.value as \"OS Name\",\n" +
            "\t\tosp.value as \"OS PPMS\"\n" +
            "from m_services s\n" +
            "\tinner join m_host_information cmr on s.host = cmr.host and cmr.key = 'cpu_manufacturer'\n" +
            "\tinner join m_host_information cml on s.host = cml.host and cml.key = 'cpu_model'\n" +
            "\tinner join m_host_information ccr on s.host = ccr.host and ccr.key = 'cpu_cores'\n" +
            "\tinner join m_host_information ct on s.host = ct.host and ct.key = 'cpu_threads'\n" +
            "\tinner join m_host_information cck on s.host = cck.host and cck.key = 'cpu_clock'\n" +
            "\tinner join m_host_information mp on s.host = mp.host and mp.key = 'mem_phys'\n" +
            "\tinner join m_host_information ms on s.host = ms.host and ms.key = 'mem_swap'\n" +
            "\tinner join m_host_information osn on s.host = osn.host and osn.key = 'os_name'\n" +
            "\tinner join m_host_information osp on s.host = osp.host and osp.key = 'os_ppms_name'\n" +
            "where s.service_name = 'indexserver' and s.coordinator_type = 'MASTER'";

    private static final String EXPENSIVE_STATEMENTS = "select STATEMENT_STRING, USER_NAME, SCHEMA_NAME, \n" +
            "\tROUND(AVG_EXECUTION_TIME / 1000, 0) AS AVG_TIME, \n" +
            "\tROUND(MAX_EXECUTION_TIME / 1000, 0) AS MAX_TIME,\n" +
            "\tROUND(MIN_EXECUTION_TIME / 1000, 0) AS MIN_TIME\n" +
            " from M_SQL_PLAN_CACHE where AVG_EXECUTION_TIME > ";

    private final String url;
    //private final long expensiveStatementsLength;

    public DataFetcher(String url) {//, long expensiveStatementsLengthMS) {
        this.url = url;
        //this.expensiveStatementsLength = TimeUnit.MILLISECONDS.toMicros(expensiveStatementsLengthMS);
    }

    public String fetchData() throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DriverManager.getConnection(url);
            statement = connection.createStatement();
            String data = getSystemOverviewData(statement);
            //data += getExpensiveStatements(statement);
            data += getTableSizeOverview(statement);
            data += getTotalViewsOverview(statement);
            data += getObjectsOverview(statement);
            return data;
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private String getSystemOverviewData(Statement statement) throws SQLException {
        ResultSet resultSet = null;
        final StringBuilder data = new StringBuilder();
        data.append("CPU Manufacturer\tCPU Model\tCPU Cores\tCPU Threads\tCPU Clock\tPhysical Memory\tSwap Memory");
        data.append("\tOS Name\tOS PPMS").append(LINE_SEPARATOR);
        try {
            resultSet = statement.executeQuery(SYSTEM_OVERVIEW);
            while (resultSet.next()) {
                for (int i = 1; i < 10; i++)
                    data.append(resultSet.getString(i)).append('\t');
                data.deleteCharAt(data.length() - 1).append(LINE_SEPARATOR);
            }
            return data.append(LINE_SEPARATOR).toString();
        } finally {
            if (null != resultSet)
                resultSet.close();
        }
    }

    private String getExpensiveStatements(Statement statement) throws SQLException {
        ResultSet resultSet = null;
        final StringBuilder data = new StringBuilder();
        data.append("STATEMENT STRING\tUSER NAME\tSCHEMA NAME\tAVG TIME (ms)\tMAX TIME (ms)\tMIN TIME (ms)");
        data.append(LINE_SEPARATOR);
        try {
            resultSet = statement.executeQuery(EXPENSIVE_STATEMENTS /*+ expensiveStatementsLength*/);
            while (resultSet.next()) {
                for (int i = 1; i < 7; i++)
                    data.append(resultSet.getString(i)).append('\t');
                data.deleteCharAt(data.length() - 1).append(LINE_SEPARATOR);
            }
            return data.append(LINE_SEPARATOR).toString();
        } finally {
            if (null != resultSet)
                resultSet.close();
        }
    }

    private String getObjectsOverview(Statement statement) throws SQLException {
        ResultSet resultSet = null;
        final StringBuilder data = new StringBuilder();
        data.append("SCHEMA/PACKAGE\tOBJECT TYPE\tCOUNT\tSIZE").append(LINE_SEPARATOR);
        try {
            resultSet = statement.executeQuery(OBJECTS_OBERVIEW);
            while (resultSet.next()) {
                for (int i = 1; i < 5; i++) {
                    String val = resultSet.getString(i);
                    data.append(null == val ? "" : val).append('\t');
                }
                data.deleteCharAt(data.length() - 1).append(LINE_SEPARATOR);
            }
            return data.append(LINE_SEPARATOR).toString();
        } finally {
            if (null != resultSet)
                resultSet.close();
        }
    }

    private String getTableSizeOverview(Statement statement) throws SQLException {
        ResultSet resultSet = null;
        final StringBuilder data = new StringBuilder();
        data.append("TABLE TYPE\tTOTAL SIZE").append(LINE_SEPARATOR);
        try {
            resultSet = statement.executeQuery(TABLE_SIZE_OVERVIEW);
            while (resultSet.next()) {
                for (int i = 1; i < 3; i++) {
                    String val = resultSet.getString(i);
                    data.append(null == val ? "" : val).append('\t');
                }
                data.deleteCharAt(data.length() - 1).append(LINE_SEPARATOR);
            }
            return data.append(LINE_SEPARATOR).toString();
        } finally {
            if (null != resultSet)
                resultSet.close();
        }
    }

    private String getTotalViewsOverview(Statement statement) throws SQLException {
        ResultSet resultSet = null;
        final StringBuilder data = new StringBuilder();
        data.append("VIEW TYPE\tTOTAL COUNT").append(LINE_SEPARATOR);
        try {
            resultSet = statement.executeQuery(TOTAL_VIEWS_OVERVIEW);
            while (resultSet.next()) {
                for (int i = 1; i < 3; i++) {
                    String val = resultSet.getString(i);
                    data.append(null == val ? "" : val).append('\t');
                }
                data.deleteCharAt(data.length() - 1).append(LINE_SEPARATOR);
            }
            return data.append(LINE_SEPARATOR).toString();
        } finally {
            if (null != resultSet)
                resultSet.close();
        }
    }
}
