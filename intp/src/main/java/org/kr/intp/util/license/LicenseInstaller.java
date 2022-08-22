package org.kr.intp.util.license;

import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.DbUtils;

import java.sql.*;

/**
 * Created bykron 07.12.2016.
 */
public class LicenseInstaller {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private final Logger log = LoggerFactory.getLogger(LicenseInstaller.class);

    public LicenseInstaller() { }

    public boolean install(String licenseContent, String instanceId, String name, char type, int intpSize,
                        String schema, String dbHost, int dbPort, String dbUser, String dbPassowrd) throws Exception {
        log.debug("installing license: " + licenseContent);
        try {
            installKey(licenseContent, schema, instanceId, name, type,
                    dbHost, dbPort, dbUser, dbPassowrd, intpSize);
            return true;
        } catch (Exception e) {
            log.error("Error while installing license: " + e.getMessage(), e);
            throw e;
        }
    }

    private void installKey(String licenseKey, String schema, String instanceId, String name, char type,
                           String dbhost, int dbport, String dbuser, String dbpassword,
                           int intpSize) throws Exception {
        final String url = DbUtils.createSapJdbcUrl(dbhost, dbport, dbuser, dbpassword);
        try (Connection connection = DriverManager.getConnection(url)) {
            LicenseDbUtils.getInstance().checkMetadata(connection, schema);
            boolean isOldKeyInstalled = isOldKeyInstalled(connection, instanceId, schema);
            if (isOldKeyInstalled)
                updateKey(connection, licenseKey, schema, instanceId);
            else
                insertServerData(connection, licenseKey, schema, instanceId, name, type, dbhost, dbport,
                        intpSize);
        }
    }

    private boolean isOldKeyInstalled(Connection connection, String instanceId, String intpSchema) throws SQLException {
        final String query = String.format("select count(*) from %s.RT_SERVER where instance_id = ?", intpSchema);
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, instanceId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next())
                    return false;
                final int count = resultSet.getInt(1);
                return count > 0;
            }
        }
    }

    private int updateKey(Connection connection, String licenseKey, String schema, String instanceId) throws SQLException {
        final String query = String.format("update %s.RT_SERVER set intp_license_key = ? where instance_id = ?", schema);
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, licenseKey);
            statement.setString(2, instanceId);
            return statement.executeUpdate();
        }
    }

    private int insertServerData(Connection connection, String licenseKey, String schema, String instanceId,
                                 String name, char intpType, String dbHost, int dbport, int intpSize) throws Exception {
        final String hanaInstance = String.valueOf(dbport).substring(1,3);
        final String type = String.valueOf(intpType);
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("insert into ").append(schema).append(".RT_SERVER").append(LINE_SEPARATOR);
        stringBuilder.append("(ID, INSTANCE_ID, NAME, TYPE, STATUS, HANA_HOST, HANA_INSTANCE, INTP_LICENSE_KEY, INTP_SIZE_MB)");
        stringBuilder.append(LINE_SEPARATOR).append("values").append(LINE_SEPARATOR);
        stringBuilder.append("(?,?,?,?,?,?,?,?,?)");
        final String query = stringBuilder.toString();
        final int id = getNewId(connection, schema);

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setInt(1, id);
            statement.setString(2, instanceId);
            statement.setString(3, name);
            statement.setString(4, type);
            statement.setInt(5, 0);
            statement.setString(6, dbHost);
            statement.setString(7, hanaInstance);
            statement.setString(8, licenseKey);
            statement.setInt(9, intpSize);
            return statement.executeUpdate();
        }
    }

    private int getNewId(Connection connection, String schema) throws Exception {
        final String query = "select max(ID) from " + schema + ".RT_SERVER";
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            if (resultSet.next())
                return resultSet.getInt(1);
            throw new Exception("Cannot generate new ID for RT_SERVER table.");
        }
    }
}
