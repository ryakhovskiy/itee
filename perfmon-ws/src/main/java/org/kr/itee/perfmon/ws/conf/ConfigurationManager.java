package org.kr.itee.perfmon.ws.conf;

import org.kr.itee.perfmon.ws.Bootstrap;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.log4j.Logger;

import javax.jms.Destination;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 *
 */
public class ConfigurationManager {

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF8");

    private static final ConfigurationManager config = new ConfigurationManager();
    public static ConfigurationManager instance() { return config; }

    private static final long DEFAULT_INTERVAL_MS = 5000L;
    private static final String CONFIG_NAME = "perfmon.properties";

    private final Logger log = Logger.getLogger(ConfigurationManager.class);
    private final Properties properties = new Properties();

    private boolean exists = false;

    public ConfigurationManager() {
        try {
            loadConfig();
        } catch (IOException e) {
            log.error("Error while initializing configuration", e);
        }
    }

    private void loadConfig() throws IOException {
        log.info("loading config file...");
        Path path = Paths.get(Bootstrap.getHome(), "conf", CONFIG_NAME);
        if (loadFromFile(path))
            return;
        path = Paths.get(".", "conf", CONFIG_NAME);
        if (loadFromFile(path))
            return;
        path = Paths.get(Bootstrap.getHome(), CONFIG_NAME);
        if (loadFromFile(path))
            return;
        path = Paths.get(".", CONFIG_NAME);
        if (loadFromFile(path))
            return;
        properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(CONFIG_NAME));
        log.info("config loaded from resources");
    }

    private boolean loadFromFile(Path path) throws IOException {
        if (!Files.exists(path)) {
            log.trace("File does not exists: " + path.toAbsolutePath().toString());
            return false;
        }
        try (InputStreamReader reader = new InputStreamReader(new FileInputStream(path.toAbsolutePath().toString()), DEFAULT_CHARSET)) {
            properties.load(reader);
            log.info("config loaded from: " + path.toAbsolutePath().toString());
            return true;
        }
    }

    public boolean useCustomProperties() {
        return getBooleanProperty("use.custom.properties");
    }

    public boolean keepStatsInmemory() {
        return getBooleanProperty("keep.stats.inmemory");
    }

    public String[] getCustomProceduresList() {
        return getProperty("custom.procedures.list").split(";", -1);
    }

    public ProcedureConfiguration[] getProceduresConfigurations() {
        final String[] procs = getCustomProceduresList();
        if (null == procs || procs.length == 0)
            return new ProcedureConfiguration[0];
        final ProcedureConfiguration[] configs = new ProcedureConfiguration[procs.length];
        for (int i = 0; i < procs.length; i++)
            configs[i] = getProcedureConfiguration(procs[i]);
        return configs;
    }

    public ProcedureConfiguration getProcedureConfiguration(String name) {
        String key = String.format("%s.name", name);
        final String procName = getProperty(key);
        if (null == procName || procName.length() == 0)
            return null;
        key = String.format("%s.schema", name);
        final String schema = getProperty(key);
        final ProcedureConfiguration configuration = new ProcedureConfiguration(procName, schema);
        key = String.format("%s.interval.ms", name);
        if (containsProperty(key))
            configuration.setIntervalMS(getLongProperty(key));
        key = String.format("%s.jdbc.url", name);
        if (containsProperty(key))
            configuration.setUrl(getProperty(key));
        return configuration;
    }



    public Destination[] getDestinations() throws Exception {
        BrokerService broker = new BrokerService();
        ActiveMQDestination[] destinations = broker.getRegionBroker().getDestinations();
        List<Destination> destinationList = new ArrayList<>();
        for (ActiveMQDestination d : destinations) {
            if (d.getDestinationTypeAsString().toLowerCase().equals("queue"))
                destinationList.add(d);
        }
        return destinationList.toArray(new Destination[destinationList.size()]);
    }

    private boolean containsProperty(String key) {
        return properties.containsKey(key);
    }

    public boolean isJmsEnabled() {
        return getBooleanProperty("jms.enabled");
    }

    public int getServerPort() {
        return getIntProperty("restful.server.port");
    }

    public String getBrokerURL() {
        return getProperty("jms.broker.url");
    }

    public String getSchema() {
        return getProperty("default.schema");
    }

    public String getSubprocessMainClassName() {
        return getProperty("subproc.main.class");
    }

    public Locale getDecimalLocale() {
        if (!exists)
            return Locale.getDefault();
        final String sloc = properties.getProperty("decimal.format.locale");
        return new Locale(sloc);
    }

    public boolean isDumpRunLogEnabled() {
        return exists && getBooleanProperty("dump.run.log.enabled");
    }

    public String getNotificationRecipients() {
        return getProperty("notification.recipients");
    }

    public String getDumpRunLogTable() {
        if (!exists)
            return "";
        return properties.getProperty("dump.run.log.table");
    }

    private int getIntProperty(String name) {
        final String sval = getProperty(name);
        try {
            return Integer.parseInt(sval);
        } catch (Exception e){
            System.err.printf("Cannot convert property [%s] to int: [%s] %n", name, sval);
            return 0;
        }
    }

    private String getProperty(String key) {
        final String value = properties.getProperty(key);
        if (null == value) {
            System.out.printf("perfmon.properties does not contain [%s] key%n", key);
            return "";
        }
        return value;
    }

    private boolean getBooleanProperty(String key) {
        return getProperty(key).trim().equalsIgnoreCase("true");
    }

    private long getLongProperty(String key) {
        final String sval = getProperty(key);
        try {
            return Long.parseLong(sval);
        } catch (Exception e) {
            System.err.printf("Cannot parse property [%s], value [%s] cannot be cast to long: %s%nUsing default interval: %d", key, sval, e, DEFAULT_INTERVAL_MS);
            return DEFAULT_INTERVAL_MS;
        }
    }


}
