package org.kr.db.loader.ui.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Locale;
import java.util.Properties;

/**
 * Created bykron 18.08.2014.
 */
public class AppConfig {

    private static final String FILE = "mon.properties";

    private static final AppConfig instance = new AppConfig();

    public static AppConfig getInstance() {
        return instance;
    }

    private final File file = new File(FILE);
    private final boolean exists = file.exists();
    private final Properties properties = new Properties();

    private AppConfig() {
        if (!exists) {
            System.out.println("mon.properties does not exists... skipping");
            return;
        }
        try  (FileInputStream stream = new FileInputStream(file)) {
            properties.load(stream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isConfigExists() {
        return exists;
    }

    public Locale getDecimalLocale() {
        if (!exists)
            return Locale.getDefault();
        final String sloc = properties.getProperty("decimal.format.locale");
        return new Locale(sloc);
    }

    public boolean isDumpRunLogEnabled() {
        return exists && getBoolProperty("dump.run.log.enabled");
    }

    public String getDumpRunLogTable() {
        if (!exists)
            return "";
        return properties.getProperty("dump.run.log.table");
    }

    private long getLongProperty(String name) {
        final String sval = properties.getProperty(name);
        try {
            return Long.valueOf(sval);
        } catch (Exception e){
            System.err.printf("Cannot convert property [%s] to long: [%s] %n", name, sval);
            return 0;
        }
    }

    private int getIntProperty(String name) {
        final String sval = properties.getProperty(name);
        try {
            return Integer.valueOf(sval);
        } catch (Exception e){
            System.err.printf("Cannot convert property [%s] to int: [%s] %n", name, sval);
            return 0;
        }
    }

    private boolean getBoolProperty(String name) {
        final String sval = properties.getProperty(name);
        return sval.trim().toLowerCase().equals("true");
    }
}
