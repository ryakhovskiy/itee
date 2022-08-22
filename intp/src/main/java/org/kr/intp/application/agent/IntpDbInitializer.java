package org.kr.intp.application.agent;

import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import org.kr.intp.util.hash.HashingException;
import org.kr.intp.util.hash.HashingFactory;
import org.kr.intp.util.hash.IHashing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 *
 */
public class IntpDbInitializer {

    private final Logger log = LoggerFactory.getLogger(IntpDbInitializer.class);

    private final String dbUser;
    private final String schema;
    private final String genSchema;
    private final String instanceId;
    private final String host;
    private final char intpType;
    private final Map<String, String> config;

    IntpDbInitializer(IntpConfig config) {
        this.schema = config.getIntpSchema();
        this.genSchema = config.getIntpGenObjectsSchema();
        this.instanceId = config.getIntpInstanceId();
        this.dbUser = config.getDbUser();
        this.host = config.getDbHost();
        this.intpType = config.getIntpType();
        this.config = config.getPropertiesMap();
    }

    void checkHANAConfigData() {
        String intpHANAUserFromDb = null;
        String intpHostFromDb = null;

        Connection connection = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            preparedStatement = connection.prepareStatement(String.format("select HANA_USER, INTP_HOST from "
                    + "%s.RT_SERVER where INSTANCE_ID = ?", schema));
            preparedStatement.setString(1, instanceId);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                intpHANAUserFromDb = resultSet.getString(1);
                if (resultSet.wasNull()) intpHANAUserFromDb = null;
                intpHostFromDb = resultSet.getString(2);
                if (resultSet.wasNull()) intpHostFromDb = null;
            }
            resultSet.close();
            preparedStatement.close();

            log.info("update HANA_USER field in the RT_SERVER table...");
            preparedStatement = connection.prepareStatement(String.format("update %s.RT_SERVER set HANA_USER = ? "
                    + "where INSTANCE_ID = ?", schema));
            preparedStatement.setString(1, dbUser);
            preparedStatement.setString(2, instanceId);
            if (preparedStatement.executeUpdate() != 1) {
                log.warn("update HANA_USER field in the RT_SERVER table was not successful");
            } else {
                log.info("HANA_USER field in the RT_SERVER table was updated");
            }

            preparedStatement.close();

            if (intpHostFromDb != null) {
                if (!intpHostFromDb.equals(dbUser)) {
                    log.info("update HANA_HOST field in the RT_SERVER table...");
                    preparedStatement = connection.prepareStatement(String.format("update %s.RT_SERVER set HANA_HOST = ? "
                            + "where INSTANCE_ID = ?", schema));
                    preparedStatement.setString(1, host);
                    preparedStatement.setString(2, instanceId);
                    if (preparedStatement.executeUpdate() != 1) {
                        log.warn("update HANA_HOST field in the RT_SERVER table was not successful");
                    } else {
                        log.warn("HANA_HOST field in the RT_SERVER table was changed");
                    }
                    preparedStatement.close();
                }
            } else {
                log.info("update HANA_HOST field in the RT_SERVER table...");
                preparedStatement = connection.prepareStatement(String.format("update %s.RT_SERVER set HANA_HOST = ? "
                        + "where INSTANCE_ID = ?", schema));
                preparedStatement.setString(1, host);
                preparedStatement.setString(2, instanceId);
                if (preparedStatement.executeUpdate() != 1) {
                    log.warn("update HANA_HOST field in the RT_SERVER table was not successful");
                } else {
                    log.warn("HANA_HOST field in the RT_SERVER table was changed");
                }
                preparedStatement.close();
            }

            connection.close();
        } catch (SQLException e) {
            log.error("Error during update RT_SERVER table with INTP_HOST and HANA_USER", e);
        }
    }

    void checkIntpAdminUserPrivelegesForIntpSchema() {
        Connection connection = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        boolean allPrivilegesValid = false;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            preparedStatement = connection.prepareStatement(
                    "SELECT count(*) FROM (SELECT \"PRIVILEGE\""
                            + " FROM \"PUBLIC\".\"EFFECTIVE_PRIVILEGES\""
                            + " WHERE USER_NAME = ? and SCHEMA_NAME = ? and IS_VALID = 'TRUE'"
                            + " AND PRIVILEGE in ('DROP','INDEX','SELECT',"
                            + "'EXECUTE','INSERT','DELETE','ALTER','TRIGGER','UPDATE') GROUP BY \"PRIVILEGE\")"
                    );
            preparedStatement.setString(1, dbUser);
            preparedStatement.setString(2, schema);
            int countOfValidPrivileges = 0;
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                countOfValidPrivileges = resultSet.getInt(1);
            }

            if (countOfValidPrivileges == 9 && intpType == 'D') {
                allPrivilegesValid = true;
            }
            resultSet.close();
            preparedStatement.close();
            preparedStatement = connection.prepareStatement(
                    "SELECT count(*) FROM (SELECT \"PRIVILEGE\""
                            + " FROM \"PUBLIC\".\"EFFECTIVE_PRIVILEGES\""
                            + " WHERE USER_NAME = ? and SCHEMA_NAME = ? and IS_VALID = 'TRUE'"
                            + " AND PRIVILEGE in ('SELECT',"
                            + "'EXECUTE','INSERT','DELETE','UPDATE') GROUP BY \"PRIVILEGE\")"
                    );
            preparedStatement.setString(1, dbUser);
            preparedStatement.setString(2, schema);
            countOfValidPrivileges = 0;
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                countOfValidPrivileges = resultSet.getInt(1);
            }
            if (countOfValidPrivileges == 5 && intpType != 'D') {
                allPrivilegesValid = true;
            }

            resultSet.close();
            preparedStatement.close();
            preparedStatement = connection.prepareStatement(
                    "SELECT count(*) FROM (SELECT \"PRIVILEGE\""
                            + " FROM \"PUBLIC\".\"EFFECTIVE_PRIVILEGES\""
                            + " WHERE USER_NAME = ? and SCHEMA_NAME = ? and IS_VALID = 'TRUE'"
                            + " AND PRIVILEGE in ('CREATE ANY')  GROUP BY \"PRIVILEGE\")"
                    );
            preparedStatement.setString(1, dbUser);
            preparedStatement.setString(2, schema);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next() && (resultSet.getInt(1) == 1)) {
                allPrivilegesValid = true;
            }
            resultSet.close();
            preparedStatement.close();
            connection.close();
            if (!allPrivilegesValid) {
                log.error(String.format("Insufficient user %s privileges for schema %s",
                        dbUser, schema));
                System.exit(-1);
            }
        } catch (SQLException e) {
            log.error(String.format("Error during check the user %s privileges for schema %s",
                    dbUser, schema), e);
        }
    }

    void checkIntpAdminUserPrivelegesForIntpGenSchema() {
        Connection connection = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        boolean allPrivilegesValid = false;

        try {
            connection = ServiceConnectionPool.instance().getConnection();
            preparedStatement = connection.prepareStatement(
                    "SELECT count(*) FROM (SELECT \"PRIVILEGE\""
                            + " FROM \"PUBLIC\".\"EFFECTIVE_PRIVILEGES\""
                            + " WHERE USER_NAME = ? and SCHEMA_NAME = ? and IS_VALID = 'TRUE'"
                            + " AND PRIVILEGE in ('DROP','INDEX','SELECT',"
                            + "'EXECUTE','INSERT','DELETE','ALTER','TRIGGER','UPDATE') GROUP BY \"PRIVILEGE\")"
                    );
            preparedStatement.setString(1, dbUser);
            preparedStatement.setString(2, genSchema);
            int countOfValidPrivileges = 0;
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                countOfValidPrivileges = resultSet.getInt(1);
            }
            if (countOfValidPrivileges == 9 && intpType == 'D') {
                allPrivilegesValid = true;
            }

            resultSet.close();
            preparedStatement.close();
            preparedStatement = connection.prepareStatement(
                    "SELECT count(*) FROM (SELECT \"PRIVILEGE\""
                            + " FROM \"PUBLIC\".\"EFFECTIVE_PRIVILEGES\""
                            + " WHERE USER_NAME = ? and SCHEMA_NAME = ? and IS_VALID = 'TRUE'"
                            + " AND PRIVILEGE in ('SELECT',"
                            + "'EXECUTE','INSERT','DELETE','UPDATE') GROUP BY \"PRIVILEGE\")"
            );
            preparedStatement.setString(1, dbUser);
            preparedStatement.setString(2, genSchema);
            countOfValidPrivileges = 0;
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                countOfValidPrivileges = resultSet.getInt(1);
            }
            if (countOfValidPrivileges == 5 && intpType != 'D') {
                allPrivilegesValid = true;
            }

            resultSet.close();
            preparedStatement.close();
            preparedStatement = connection.prepareStatement(
                    "SELECT count(*) FROM (SELECT \"PRIVILEGE\""
                            + " FROM \"PUBLIC\".\"EFFECTIVE_PRIVILEGES\""
                            + " WHERE USER_NAME = ? and SCHEMA_NAME = ? and IS_VALID = 'TRUE'"
                            + " AND PRIVILEGE in ('CREATE ANY')  GROUP BY \"PRIVILEGE\")"
                    );
            preparedStatement.setString(1, dbUser);
            preparedStatement.setString(2, genSchema);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next() && (resultSet.getInt(1) == 1)) {
                allPrivilegesValid = true;
            }
            resultSet.close();
            preparedStatement.close();
            connection.close();
            if (!allPrivilegesValid) {
                log.error(String.format("Insufficient user %s privileges for schema %s",
                        dbUser, genSchema));
                System.exit(-1);
            }
        } catch (SQLException e) {
            log.error(String.format("Error during check the user %s privileges for schema %s",
                    dbUser, genSchema), e);
        }
    }

    /**
     * Persists the properties to the database.
     */
    void persistConfigurationToDatabase() {
        log.info("Persisting properties to database started.");
        IHashing md5 = HashingFactory.newHashingFactory().createHashing("MD5");
        final String query =
                String.format("upsert \"%s\".PROPERTIES values (?,?) with primary key", schema);
        try {
           storeMap(config, md5, query);
        } catch (Exception e) {
            log.error("Error: Could not persist properties to database.", e);
        }
        log.info("Persisting properties to database finished.");
    }

    private void storeMap(Map<String, String> properties, IHashing hashing, String query)
            throws SQLException, HashingException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.prepareStatement(query);
            for (Map.Entry<String, String> e : properties.entrySet()) {
                String key = e.getKey();
                String value = e.getValue();
                if ("db.user".equals(key) || "db.password".equals(key) || key.contains("password"))
                    continue;
                statement.setString(1, key);
                statement.setString(2, value);
                statement.executeUpdate();
            }
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection) {
                connection.close();
            }
        }
    }
}
