package org.kr.intp.util.license;

import org.kr.intp.application.AppContext;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created bykron 08.09.2014.
 */
public class LicenseDetailsUpdater {

    private static final LicenseDetailsUpdater instance = new LicenseDetailsUpdater();
    public static LicenseDetailsUpdater getInstance() { return instance; }
    private LicenseDetailsUpdater() { }

    public void update(long time, int tables, String instance_id, int intpsize) {
        try {
            doVerification(time, tables, instance_id, intpsize);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void doVerification(long time, int tables, String instance_id, int intpSize) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.createStatement();
            if (!isFieldExists(statement))
                addField(statement);
            updateData(connection, time, tables, instance_id, intpSize);
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private boolean isFieldExists(Statement statement) throws SQLException {
        final String query = "select * from TABLE_COLUMNS where schema_name = '"+ schema +"' and table_name = 'RT_SERVER' " +
                "and column_name = 'LDETAILS'";
        ResultSet resultSet = null;
        try {
            resultSet = statement.executeQuery(query);
            return resultSet.next();
        } finally {
            if (null != resultSet)
                resultSet.close();
        }
    }

    private void addField(Statement statement) throws SQLException {
        final String query = "alter table " + schema + ".RT_SERVER add (LDETAILS nvarchar(512))";
        statement.execute(query);
    }

    private void updateData(Connection connection, long time, int tables, String instance_id, int intpSize) throws SQLException {
        final String sql = "update " + schema + ".rt_server set LDETAILS = ? where instance_id = ?";
        PreparedStatement statement = null;
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        final String date = sdf.format(new Date(time));
        try {
            statement = connection.prepareStatement(sql);
            final String arg = String.format("Expiration Date: %s; Active Projects: %d; In-Time Size: %d", date, tables, intpSize);
            statement.setString(1, arg);
            statement.setString(2, instance_id);
            statement.execute();
        } finally {
            if (null != statement)
                statement.close();
        }
    }

    private final String schema = AppContext.instance().getConfiguration().getIntpSchema();
}
