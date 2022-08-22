package org.kr.intp.util.db;

/**
 * Created bykron 29.11.2016.
 */
public class DbUtils {

    public static String createSapJdbcUrl(String host, int port, String user, String password) {
        return String.format("jdbc:sap://%s:%d?user=%s&password=%s", host, port, user, password);
    }

}
