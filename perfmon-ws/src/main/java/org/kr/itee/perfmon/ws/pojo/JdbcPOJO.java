package org.kr.itee.perfmon.ws.pojo;

/**
 *
 */
public class JdbcPOJO {

    private final String driver;
    private final String host;
    private final int port;
    private final String user;
    private final String password;

    public JdbcPOJO(String driver, String host, int port, String user, String password) {
        this.driver = driver;
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
    }

    public String getDriver() {
        return driver;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getUrl() {
        //for SAP HANA only
        return String.format("jdbc:sap://%s:%d?user=%s&password=%s", host, port, user, password);
    }
}
