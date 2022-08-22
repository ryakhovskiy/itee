package org.kr.intp.util.license;

import org.kr.intp.App;
import org.kr.intp.IntpMessages;
import org.kr.intp.IntpServerInfo;
import org.kr.intp.application.AppContext;
import org.kr.intp.application.agent.IServer;
import org.kr.intp.application.agent.IntpServer;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.crypt.LicenseCrypter;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by kr on 16.12.13.
 */
public final class LicenseManager implements Runnable {

    private final IntpMessages messages = AppContext.instance().getIntpMessages();
    private final IntpConfig config = AppContext.instance().getConfiguration();
    private final String schema = config.getIntpSchema();
    private final Logger log = LoggerFactory.getLogger(IntpServer.class);
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final LicenseData data;
    private final boolean isProductionSystem;
    private final Closeable server;

    private LicenseManager(IServer server, IntpServerInfo serverInfo) {
        this.server = server;
        data = checkLicense(serverInfo);
        isProductionSystem = serverInfo.getType() == 'P';
        LicenseDetailsUpdater.getInstance().
                update(data.getExpirationTime(), data.getMaxTablesCount(),
                        serverInfo.getInstanceId(), serverInfo.getIntpSize());
    }

    /******************************************
     * new license key info:
     * 01- hana hardware key
     * 02- hana system id
     * 03- intp instance id
     * 04- intp in-Time Size
     * 05- intp type
     * 06- intp name
     * 07- expiration date
     * 08- tables count
     ******************************************/

    public static LicenseManager newInstance(IServer server, IntpServerInfo serverInfo) {
        return new LicenseManager(server, serverInfo);
    }

    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            checkLicense();
            try {
                long sleepMs = (long) (random.nextDouble() * 100000) + 100000L;
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    void testCheckLicense() {
        checkLicense();
    }

    private void checkLicense() {

        if (!isLicenseInvalid() && isProductionSystem) {
            stopServer();
        }

        if (isMaxTablesReached() && isProductionSystem) {
            stopServer();
        }

        if (isMaxInTimeSizeReached() && isProductionSystem) {
            stopServer();
        }
    }

    private boolean isMaxTablesReached() {
        try {
            int fact = getFactTables();
            String message;
            if (fact <= data.getMaxTablesCount() || data.getMaxTablesCount() < 0 || fact == -1 || fact == 0) {
                message = messages.getString("org.kr.intp.util.license.licensemanager.002",
                        "CURRENT ACTIVE PROJECTS : %d  ALLOWED: %d - c");
                message = String.format(message, fact, data.getMaxTablesCount());
                log.info(message);
            } else {
                message = messages.getString("org.kr.intp.util.license.licensemanager.003",
                        "LICENSE IS NOT VALID. PLEASE UPGRADE THE LICENSE! CURRENT ACTIVE PROJECTS : %d  INSTEAD OF : %d");
                message = String.format(message, fact, data.getMaxTablesCount());
                log.error(message);
                return true;
            }
        } catch (Exception e) {
            log.error("Error while checking tables count", e);
        }
        return false;
    }

    private boolean isMaxInTimeSizeReached() {
        try {
            int fact = getFactInTimeSize();
            if (fact <= data.getIntpSize() || fact == -1 || data.getIntpSize() < 0 || fact == 0) {
                String message = messages.getString("org.kr.intp.util.license.licensemanager.004",
                        "CURRENT In-Time Size : %d  ALLOWED: %d ");
                message = String.format(message, fact, data.getIntpSize());
                log.info(message);

            } else {
                String message = messages.getString("org.kr.intp.util.license.licensemanager.005",
                        "LICENSE IS NOT VALID. CURRENT IN-TIME Size : %d  MB INSTEAD OF : %d MB");
                message = String.format(message, fact, data.getIntpSize());
                log.error(message);
                return true;
            }
        } catch (Exception e) {
            log.error("Error while retrieving fact size", e);
        }
        return false;
    }

    private void stopServer() {
        try {
            server.close();
        } catch (Exception e) {
            String message = messages.getString("org.kr.intp.util.license.licensemanager.006",
                    "Error occurred while setting status : SetIntpStopped");
            log.error(message, e);
            System.exit(-1);
        }
    }

    private boolean isLicenseInvalid() {
        if (!data.isAcceptable() || data.getExpirationTime() < App.currentTimeMillis()) {
            String message = messages.getString("org.kr.intp.util.license.licensemanager.001",
                    "NOT A VALID LICENSE -code");
            log.error(message);
            return true;
        } else
            return false;
    }

    private LicenseData checkLicense(IntpServerInfo serverInfo) {
        try {
            Map<String, String> actualInfo = getDbLicenseInfo(serverInfo);
            Map<String, String> licenseKeyInfo = getIntpServerLicense(serverInfo);
            return checkLicense(actualInfo, licenseKeyInfo);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return new LicenseData(false, 10, Long.MAX_VALUE, Integer.MIN_VALUE);
        }
    }

    private Map<String, String> getDbLicenseInfo(IntpServerInfo serverInfo) throws SQLException {
        //select hardware_key, system_no from m_license
        final byte[] bb = new byte[] {115, 101, 108, 101, 99, 116, 32, 104, 97, 114, 100, 119, 97, 114,
                101, 95, 107, 101, 121, 44, 32, 115, 121, 115, 116, 101, 109, 95, 110, 111, 32, 102,
                114, 111, 109, 32, 109, 95, 108, 105, 99, 101, 110, 115, 101 };

        final String query = new String(bb, Charset.forName("UTF8"));
        final HashMap<String, String> data = new HashMap<>();
        try (Connection connection = ServiceConnectionPool.instance().getConnection();
             PreparedStatement statement = connection.prepareStatement(query);
             ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next()) {
                data.put("01", resultSet.getString(1));
                data.put("02", resultSet.getString(2));
            }
            data.put("03", serverInfo.getInstanceId());
            data.put("04", String.valueOf(serverInfo.getIntpSize()));
            data.put("05", String.valueOf(serverInfo.getType()));
            data.put("06", serverInfo.getName());

            return data;
        }
    }

    private Map<String, String> getIntpServerLicense(IntpServerInfo serverInfo) throws SQLException {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("select * from ");
        stringBuilder.append(config.getIntpSchema()).append(".");
        stringBuilder.append(new String(new byte[]{114, 116, 95, 115, 101, 114, 118, 101, 114})).append(" ");
        stringBuilder.append(new String(new byte[]{119, 104, 101, 114, 101, 32, 105, 110, 115, 116, 97, 110, 99}));
        stringBuilder.append(new String(new byte[]{101, 95, 105, 100, 32, 61, 32, 63, 32}));
        stringBuilder.append(new String(new byte[]{97, 110, 100, 32, 116, 121}));
        stringBuilder.append(new String(new byte[]{112, 101, 32, 61, 32, 63}));
        final String sql = stringBuilder.toString();
        ResultSet resultSet = null;
        try (Connection connection = ServiceConnectionPool.instance().getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, serverInfo.getInstanceId());
            statement.setString(2, String.valueOf(serverInfo.getType()));
            resultSet = statement.executeQuery();
            if (resultSet.next()) {
                final String license = resultSet.getString(new String(new byte[]{73, 78, 84, 80, 95, 76, 73, 67, 69, 78, 83, 69, 95, 75, 69, 89}));
                if (null == license || license.length() == 0)
                    return Collections.unmodifiableMap(new HashMap<String, String>());
                return parseData(license);
            }      }  finally {
            if (null != resultSet)
                resultSet.close();
        }
        return Collections.unmodifiableMap(new HashMap<String, String>());
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> parseData(String data) {
        try {
            data = data.replaceAll("\n", "");
            final LicenseCrypter crypter = LicenseCrypter.newInstance();
            final String info = crypter.decryptData(data);
            final ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(info, HashMap.class);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            return Collections.unmodifiableMap(new HashMap<String, String>());
        }
    }

    private LicenseData checkLicense(Map<String, String> actualInfo, Map<String, String> licenseKeyInfo) {
        if (actualInfo.size() != 6)
            return new LicenseData(false, Integer.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE);
        if (licenseKeyInfo.size() < 8)
            return new LicenseData(false, Integer.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE);

        for (Map.Entry<String, String> actualEntry : actualInfo.entrySet()) {
            final String key = actualEntry.getKey();
            if (!licenseKeyInfo.containsKey(key))
                return new LicenseData(false, 10, Long.MAX_VALUE, Integer.MIN_VALUE);
            if (!licenseKeyInfo.get(key).equals(actualEntry.getValue()))
                return new LicenseData(false, 10, Long.MAX_VALUE, Integer.MIN_VALUE);
        }
        final int _intpSize = Integer.valueOf(licenseKeyInfo.get("04"));
        final long expiration = Long.valueOf(licenseKeyInfo.get("07"));
        final int tables = Integer.valueOf(licenseKeyInfo.get("08"));
        return new LicenseData(true, tables, expiration, _intpSize);
    }

    public int getTablesCount() {
        return data.getMaxTablesCount();
    }

    private int getFactInTimeSize() throws SQLException {
        final String getMemoryQuery = String.format(
                "select sum(\"SIZE_IN_MB\") as \"SIZE_IN_MB\" from ("
                        + "select sum (MTAB.MEMORY_SIZE_IN_TOTAL)/(1024*1024) as \"SIZE_IN_MB\""
                        + " FROM %s.RT_ACTIVE_PROJECTS ACT "
                        + " INNER JOIN %s.RT_PROJECT_TABLES PTAB "
                        + " ON ACT.PROJECT_ID=PTAB.PROJECT_ID and ACT.VERSION=PTAB.VERSION and ACT.STATUS = 1"
                        + " INNER JOIN %s.RT_PROJECT_DEFINITIONS PDEF "
                        + " ON ACT.PROJECT_ID=PDEF.PROJECT_ID and ACT.VERSION=PDEF.VERSION "
                        + " INNER JOIN M_CS_TABLES MTAB "
                        + " ON MTAB.TABLE_NAME=PTAB.TABLE_NAME and MTAB.SCHEMA_NAME=PDEF.TABLE_SCHEMA "
                        + " GROUP BY PTAB.TABLE_NAME "
                        + " UNION "
                        + " select sum (MTAB.MEMORY_SIZE_IN_TOTAL)/(1024*1024) as \"SIZE_IN_MB\""
                        + " FROM %s.RT_PRECALCULATED_OBJECTS PTAB "
                        + " INNER JOIN %s.RT_ACTIVE_PROJECTS ACT "
                        + " ON ACT.PROJECT_ID=PTAB.PROJECT_ID and ACT.STATUS = 1 "
                        + " INNER JOIN  M_CS_TABLES MTAB ON "
                        + " MTAB.TABLE_NAME=PTAB.NAME and MTAB.SCHEMA_NAME=PTAB.SCHEMA "
                        + " GROUP BY PTAB.NAME )", schema, schema, schema, schema, schema);
        int factSize = -1;
        try (Connection connection = ServiceConnectionPool.instance().getConnection();
             PreparedStatement statement = connection.prepareStatement(getMemoryQuery);
             ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next()) {
                factSize = (int) resultSet.getDouble(1);
            }
        } catch (Exception e) {
            log.error("Error while getting fact in-time size", e);
            factSize = -1;
        }
        return factSize;
    }

    private int getFactTables() throws SQLException {
        final String getFactTablesQuery = String.format(
                "select count (*) as tables from ( "
                        + " select NAME FROM %s.RT_PRECALCULATED_OBJECTS PTAB "
                        + " INNER JOIN %s.RT_ACTIVE_PROJECTS ACT "
                        + " ON ACT.PROJECT_ID=PTAB.PROJECT_ID and ACT.STATUS = 1"
                        + " WHERE ACT.PROJECT_ID != 'ISM' AND ACT.PROJECT_ID != 'ESM'"
                        + " GROUP BY PTAB.NAME )", schema, schema);
        int factCount = -1;
        try (Connection connection = ServiceConnectionPool.instance().getConnection();
             PreparedStatement statement = connection.prepareStatement(getFactTablesQuery);
             ResultSet resultSet = statement.executeQuery()) {
            if (resultSet.next()) {
                factCount = resultSet.getInt(1);
            }
        } catch (Exception e) {
            log.error("Error while retrieving fact tables number", e);
            factCount = -1;
        }
        return factCount;
    }
}