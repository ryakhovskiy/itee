package org.kr.db.loader.ui;

import org.kr.db.loader.ui.panels.RootFrame;
import org.kr.db.loader.ui.utils.IOUtils;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;

import javax.jms.Destination;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Created by kr on 5/5/2014.
 */
public class AppMain {

    public static final String SCHEMA = "INTP";

    public static final String APP_VERSION;

    static {
        String version;
        try (InputStream stream =
                     Thread.currentThread().getContextClassLoader().getResourceAsStream("perfmon.properties")) {
            Properties properties = new Properties();
            properties.load(stream);
            version = properties.getProperty("perfmon.version");
        } catch (Exception e) {
            version = "";
        }
        APP_VERSION = version;
    }

    private final static BrokerService broker = new BrokerService();
    public static final String BROKER_URL = "tcp://localhost:61616";
    private static long start;
    public static void setMonitorStarted() {
        start = currentTimeMillis();
    }
    public static long getMonitorStarted() {
        return start;
    }

    private static final long offset = getOffset();

    public static long currentTimeMillis() {
        return System.currentTimeMillis() + offset;
    }

    private static long getOffset() {
        final TimeZone tz = TimeZone.getDefault();
        return (-1) * tz.getOffset(System.currentTimeMillis());
    }

    public static void main(String... args) {
        try {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    init();
                }
            }).start();
            new RootFrame();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void init() {
        try {
            //startActiveMq();
            IOUtils.getInstance().cleanUpWorkingDir();
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    close();
                }
            }));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void startActiveMq() throws Exception {
        final TransportConnector connector = new TransportConnector();
        connector.setUri(new URI(BROKER_URL));
        broker.addConnector(connector);
        broker.start();
    }

    public static Destination[] getDestinations() throws Exception {
        ActiveMQDestination[] destinations = broker.getRegionBroker().getDestinations();
        List<Destination> destinationList = new ArrayList<Destination>();
        for (ActiveMQDestination d : destinations) {
            if (d.getDestinationTypeAsString().toLowerCase().equals("queue"))
                destinationList.add(d);
        }
        return destinationList.toArray(new Destination[destinationList.size()]);
    }

    public static void checkStoredProcedure(Connection connection) throws IOException, SQLException {
        final String resource =
                IOUtils.getInstance().getResourceAsString("org.kr.perfmon.md/sp_performance_info.sql");
        final String sql = resource.replace("$$SCHEMA$$", SCHEMA);
        Statement statement = null;
        try {
            statement = connection.createStatement();
            if (!isSchemaExists(statement, SCHEMA))
                statement.execute("create schema " + SCHEMA);
            if (!isProcedureExists(statement, SCHEMA, "SP_PERFORMANCE_INFO"))
                statement.execute(sql);
        } finally {
            if (null != statement)
                statement.close();
        }
    }

    private static boolean isSchemaExists(Statement statement, String schema) throws SQLException {
        final String sql = "select schema_name from schemas where schema_name = '" + schema + "'";
        return isNotEmptyResultSet(statement, sql);
    }

    private static boolean isProcedureExists(Statement statement, String schema, String procedure) throws SQLException {
        final String sql = "select procedure_name from PROCEDURES where schema_name = '" + schema +
                "' and procedure_name = '" + procedure + "'";
        return isNotEmptyResultSet(statement, sql);
    }

    private static boolean isNotEmptyResultSet(Statement statement, String sql) throws SQLException {
        ResultSet resultSet = null;
        try {
            resultSet = statement.executeQuery(sql);
            return resultSet.next();
        } finally {
            if (null != resultSet)
                resultSet.close();
        }
    }

    private static void close() {
        try {
            broker.stop();
            IOUtils.getInstance().cleanUpWorkingDir();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
