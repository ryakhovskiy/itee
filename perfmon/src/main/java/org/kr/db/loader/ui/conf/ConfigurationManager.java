package org.kr.db.loader.ui.conf;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 *
 */
public class ConfigurationManager {

    private static final long DEFAULT_INTERVAL_MS = 5000L;
    private static final String FILE = "perfmon.properties";

    private final Properties properties = new Properties();

    public ConfigurationManager() {
        if (!Files.exists(Paths.get(FILE)))
            return;
        try (FileInputStream stream = new FileInputStream(FILE)) {
            properties.load(stream);
        } catch (IOException e) {
            System.err.println("custom configuration is not provided");
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

    private boolean containsProperty(String key) {
        return properties.containsKey(key);
    }
}
