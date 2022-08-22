package org.kr.db.loader.ui.panels;

import org.kr.db.loader.ui.AppMain;
import org.kr.db.loader.ui.monitor.*;
import org.kr.db.loader.ui.utils.IOUtils;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by kr on 5/17/2014.
 */
public class MonitorPanel extends JPanel {

    private static final String HEADER = "Memory Allocators Monitor";

    private static final String TOOLTIP_OUTPUTFILE = "The name of the output file which is used to save the gathered statistics";
    private static final String TOOLTIP_TOPXVALS = "The number of top values (i.e. top X allocators of consuming memory)";
    private static final String TOOLTIP_MONITOR_INTERVAL = "The frequency of querying the database statistics";
    private static final String TOOLTIP_EXPENSIVE_ST_DURATION = "The minimal duration of the statement execution which is considered as “expensive”";
    private static final String TOOLTIP_AGE_SECONDS = "Max age of the monitor values (avg/max for the values not older than this age";

    private static final Long MONITOR_INTERVAL_DEFAULT = 5000L;
    private static final Long EXPENSIVE_STATEMENTS_LENGTH_DEFAULT = 5000L;
    private static final Integer TOP_X_VALUES_DEFAULT = 7;
    private static final Integer AGE_SECONDS_DEFAULT = 120;

    private static final String PANEL_NAME = "Performance monitor for SAP HANA";

    private final String defaultPropertiesFileName;

    private final JdbcPanel jdbcPanel = new JdbcPanel();
    private final JTextField filepathTextField;
    private final JButton startMonitorBtn = new JButton("Start Monitor");
    private final JButton stopMonitorBtn = new JButton("Stop Monitor");
    private final JSpinner topXvalsSpinner = new JSpinner();
    private final JSpinner ageSecondsSpinner = new JSpinner();
    private final JSpinner monitorIntervalSpinner = new JSpinner();
    private final JSpinner expensiveStatementLengthSpinner = new JSpinner();
    private final JSpinner powerConsumptionPortSpinner = new JSpinner();
    private final JSpinner powerConsumptionLengthSpinner = new JSpinner();
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);
    private Future monitorFuture;
    private MonitorManager monitorManager;
    private final JProgressBar memProgressBar = new JProgressBar(SwingConstants.HORIZONTAL);
    private final JProgressBar cpuProgressBar = new JProgressBar(SwingConstants.HORIZONTAL, 0, 100);
    private final JProgressBar connectionsProgressBar = new JProgressBar(SwingConstants.HORIZONTAL);
    private final DbLoaderPanel.RunningProcessesMonitor[] pMonitors = new DbLoaderPanel.RunningProcessesMonitor[2];

    public MonitorPanel(String defaultPropertiesFileName) {
        super();
        this.defaultPropertiesFileName = defaultPropertiesFileName;
        filepathTextField = new JTextField(getDefaultStatsFileName(defaultPropertiesFileName), 15);
        setLayout(new BoxLayout(this, BoxLayout.X_AXIS));
        setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createTitledBorder(PANEL_NAME),
                BorderFactory.createEmptyBorder(0, 0, 0, 0)));
        add(jdbcPanel);
        final JPanel allocatorMonitorPanel = new JPanel();
        add(allocatorMonitorPanel);
        addPerformanceCounterPanel();
        allocatorMonitorPanel.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createTitledBorder("Monitor parameters"),
                BorderFactory.createEmptyBorder(5, 0, 5, 0)));
        allocatorMonitorPanel.setLayout(new BoxLayout(allocatorMonitorPanel, BoxLayout.Y_AXIS));

        final JPanel filepathPanel = new JPanel();
        filepathPanel.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createCompoundBorder(),
                BorderFactory.createEmptyBorder(0,5,0,5)));
        final JLabel outputLabel = new JLabel("Output file: ");
        outputLabel.setLabelFor(filepathTextField);
        outputLabel.setToolTipText(TOOLTIP_OUTPUTFILE);
        filepathTextField.setToolTipText(TOOLTIP_OUTPUTFILE);
        final JLabel ageSecondsLabel = new JLabel("Monitor values AGE (seconds): ");
        ageSecondsLabel.setLabelFor(ageSecondsSpinner);
        ageSecondsLabel.setToolTipText(TOOLTIP_AGE_SECONDS);
        ageSecondsSpinner.setToolTipText(TOOLTIP_AGE_SECONDS);
        final JLabel topxLabel = new JLabel("TOP X values: ");
        topxLabel.setLabelFor(topXvalsSpinner);
        topxLabel.setToolTipText(TOOLTIP_TOPXVALS);
        topXvalsSpinner.setToolTipText(TOOLTIP_TOPXVALS);
        final JLabel monitorIntervalLabel = new JLabel("Monitor Query Interval (ms): ");
        monitorIntervalLabel.setToolTipText(TOOLTIP_MONITOR_INTERVAL);
        monitorIntervalSpinner.setToolTipText(TOOLTIP_MONITOR_INTERVAL);
        monitorIntervalLabel.setLabelFor(monitorIntervalSpinner);
        final JLabel expensivesLengthLabel = new JLabel("Expensive Statement Duration (ms): ");
        expensivesLengthLabel.setToolTipText(TOOLTIP_EXPENSIVE_ST_DURATION);
        expensivesLengthLabel.setLabelFor(expensiveStatementLengthSpinner);
        expensiveStatementLengthSpinner.setToolTipText(TOOLTIP_EXPENSIVE_ST_DURATION);
        final JLabel pcLengthLabel = new JLabel("Power Consumption Monitoring Duration (ms): ");
        pcLengthLabel.setToolTipText("Power Consumption Monitoring Duration (ms)");
        pcLengthLabel.setLabelFor(powerConsumptionLengthSpinner);
        powerConsumptionPortSpinner.setToolTipText("Power Consumption Monitoring Duration (ms)");
        final JLabel pcPortLabel = new JLabel("Power Consumption Monitor Port: ");
        pcPortLabel.setToolTipText("Power Consumption Monitor Port");
        pcPortLabel.setLabelFor(powerConsumptionPortSpinner);
        powerConsumptionPortSpinner.setToolTipText("Power Consumption Monitor Port");

        JLabel[] labels = new JLabel[] {outputLabel, ageSecondsLabel, topxLabel, monitorIntervalLabel,
                expensivesLengthLabel, pcLengthLabel, pcPortLabel};
        JComponent[] components = new JComponent[] {filepathTextField, ageSecondsSpinner, topXvalsSpinner,
                monitorIntervalSpinner, expensiveStatementLengthSpinner, powerConsumptionLengthSpinner,
                powerConsumptionPortSpinner};
        GridBagLayout gridbag = new GridBagLayout();
        filepathPanel.setLayout(gridbag);
        addLabelComponentRows(labels, components, filepathPanel);
        allocatorMonitorPanel.add(filepathPanel);

        final JPanel buttonPanel = new JPanel();
        buttonPanel.setLayout(new FlowLayout());

        stopMonitorBtn.setEnabled(false);
        buttonPanel.add(startMonitorBtn);
        buttonPanel.add(stopMonitorBtn);
        allocatorMonitorPanel.add(buttonPanel);

        startMonitorBtn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                tryStartMonitor();
            }
        });

        stopMonitorBtn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                stopMonitor(filepathTextField.getText(), false);
            }
        });

        monitorIntervalSpinner.setValue(MONITOR_INTERVAL_DEFAULT);
        expensiveStatementLengthSpinner.setValue(EXPENSIVE_STATEMENTS_LENGTH_DEFAULT);
        powerConsumptionLengthSpinner.setValue(10000);
        topXvalsSpinner.setValue(TOP_X_VALUES_DEFAULT);
        ageSecondsSpinner.setValue(AGE_SECONDS_DEFAULT);

        loadDefaults();
    }

    public void startMonitor() {
        tryStartMonitor();
    }

    public void stopMonitor() {
        stopMonitor(filepathTextField.getText(), true);
    }

    public void injectQueryStatsReference(DbLoaderPanel.RunningProcessesMonitor pointer) {
        pMonitors[0] = pointer;
    }

    public void injectUpdateStatsReference(DbLoaderPanel.RunningProcessesMonitor pointer) {
        pMonitors[1] = pointer;
    }

    private void addPerformanceCounterPanel() {
        final JPanel performanceCountersPanel = new JPanel();
        performanceCountersPanel.setPreferredSize(new Dimension(400, 0));
        add(performanceCountersPanel);
        performanceCountersPanel.add(connectionsProgressBar);
        performanceCountersPanel.add(memProgressBar);
        performanceCountersPanel.add(cpuProgressBar);

        connectionsProgressBar.setStringPainted(true);
        connectionsProgressBar.setString("Total connections: N/A");
        memProgressBar.setStringPainted(true);
        memProgressBar.setString("Memory Usage: N/A");
        cpuProgressBar.setStringPainted(true);
        cpuProgressBar.setString("CPU %: N/A");

        performanceCountersPanel.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createTitledBorder("Performance Counters"),
                BorderFactory.createEmptyBorder(40, 0, 40, 0)));
        performanceCountersPanel.setLayout(new BoxLayout(performanceCountersPanel, BoxLayout.Y_AXIS));
    }

    private void addLabelComponentRows(JLabel[] labels,
                                  JComponent[] textFields,
                                  Container container) {
        GridBagConstraints c = new GridBagConstraints();
        c.anchor = GridBagConstraints.EAST;
        int numLabels = labels.length;

        for (int i = 0; i < numLabels; i++) {
            c.gridwidth = GridBagConstraints.RELATIVE; //next-to-last
            c.fill = GridBagConstraints.NONE;      //reset to default
            c.weightx = 0.0;                       //reset to default
            container.add(labels[i], c);

            c.gridwidth = GridBagConstraints.REMAINDER;     //end row
            c.fill = GridBagConstraints.HORIZONTAL;
            c.weightx = 1.0;
            container.add(textFields[i], c);
        }
    }

    private void tryStartMonitor() {
        try {
            startMonitorBtn.setEnabled(false);
            stopMonitorBtn.setEnabled(true);
            AppMain.setMonitorStarted();
            final int topx = Integer.valueOf(topXvalsSpinner.getValue().toString());
            final long monitorFreqMS = getSpinnerValue(monitorIntervalSpinner);
            final long expensivesMS = getSpinnerValue(expensiveStatementLengthSpinner);
            final JProgressBar[] progressBars = new JProgressBar[]
                    { cpuProgressBar, memProgressBar, connectionsProgressBar };
            final String driver = jdbcPanel.getJdbcDriver();
            final String url = jdbcPanel.getJdbcUrl();
            final int age_seconds = (Integer)ageSecondsSpinner.getValue();
            String colName;
            if (defaultPropertiesFileName.startsWith("rt"))
                colName = "Real-Time";
            else
                colName = "In-Time";
            String serverwport = jdbcPanel.getServer();
            String server = serverwport.substring(0, serverwport.indexOf(":"));
            int port = Integer.valueOf(powerConsumptionPortSpinner.getValue().toString());
            long duration_ms = Long.valueOf(powerConsumptionLengthSpinner.getValue().toString());
            monitorManager = new MonitorManager(driver, url, server, port, duration_ms, topx, monitorFreqMS, age_seconds, expensivesMS,
                    progressBars, pMonitors, colName );
            monitorFuture = executorService.submit(monitorManager);
        } catch (Exception e) {
            e.printStackTrace();
            String message = e.getMessage();
            if ((null == message || message.trim().length() == 0) && e.getCause() != null)
                message = e.getCause().getMessage();
            if (null == message)
                message = "Unexpected Error";
            JOptionPane.showMessageDialog(this, message, HEADER, JOptionPane.ERROR_MESSAGE);
            startMonitorBtn.setEnabled(true);
            stopMonitorBtn.setEnabled(false);
        }
    }

    private long getSpinnerValue(JSpinner spinner) {
        final Object o = spinner.getValue();
        long val = 0L;
        if (o instanceof Long)
            val = (Long)o;
        else if (o instanceof Integer)
            val = (Integer)o;
        return val;
    }

    private void stopMonitor(String path, boolean silent) {
        startMonitorBtn.setEnabled(true);
        stopMonitorBtn.setEnabled(false);
        if (null == monitorManager || null == monitorFuture) {
            final String message = HEADER + " has not been started";
            System.out.println(message);
            if (!silent)
                JOptionPane.showMessageDialog(this, message, HEADER, JOptionPane.WARNING_MESSAGE);
            return;
        }
        System.out.println("Stopping monitor..." + path);
        try {
            monitorManager.shutdown();
            final List<String> data = monitorManager.getResults();
            if (data.size() == 0) {
                final String message = "No data to be saved";
                System.out.println(message);
                if (!silent)
                    JOptionPane.showMessageDialog(this, message, HEADER, JOptionPane.WARNING_MESSAGE);
                return;
            }
            IOUtils.getInstance().saveBinaryLines(path, data);
            final String message = "File saved [" + path + "]";
            System.out.println(message);
            if (!silent)
                JOptionPane.showMessageDialog(this, message, HEADER, JOptionPane.INFORMATION_MESSAGE);
        } catch (Exception e) {
            e.printStackTrace();
            if (!silent)
                JOptionPane.showMessageDialog(this, e.getMessage(), HEADER, JOptionPane.ERROR_MESSAGE);
        } finally {
            monitorManager = null;
            monitorFuture = null;
            System.out.println("Monitor stopped...");
        }
    }

    public void destroyPanel() {
        try {
            saveProperties();
        } catch (IOException e) {
            JOptionPane.showMessageDialog(this, e.getMessage(), "Memory Allocators Monitor", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void saveProperties() throws IOException {
        final File file = new File(defaultPropertiesFileName);
        FileOutputStream fileOutputStream = null;
        try {
            Properties properties = new Properties();
            properties.put("jdbc.driver", jdbcPanel.getJdbcDriver());
            properties.put("jdbc.server", jdbcPanel.getServer());
            properties.put("jdbc.user", jdbcPanel.getUser());
            properties.put("jdbc.pass", jdbcPanel.getPassword());
            properties.put("file.path", filepathTextField.getText());
            properties.put("top.x", topXvalsSpinner.getValue().toString());
            properties.put("monitor.interval", monitorIntervalSpinner.getValue().toString());
            properties.put("expensives.length", expensiveStatementLengthSpinner.getValue().toString());
            properties.put("pc.length", powerConsumptionLengthSpinner.getValue().toString());
            properties.put("pc.port", powerConsumptionPortSpinner.getValue().toString());
            properties.put("age.seconds", ageSecondsSpinner.getValue().toString());
            fileOutputStream = new FileOutputStream(file);
            properties.store(fileOutputStream, "Panel Defaults");
            fileOutputStream.flush();
        } finally {
            if (null != fileOutputStream)
                fileOutputStream.close();
        }
    }

    private void loadDefaults() {
        final File f = new File(defaultPropertiesFileName);
        if (!f.exists())
            return;
        final Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(defaultPropertiesFileName));
            jdbcPanel.setJdbcDriver(properties.getProperty("jdbc.driver"));
            jdbcPanel.setServer(properties.getProperty("jdbc.server"));
            jdbcPanel.setUser(properties.getProperty("jdbc.user"));
            jdbcPanel.setPassword(properties.getProperty("jdbc.pass"));
            //filepathTextField.setText(properties.getProperty("file.path"));
            topXvalsSpinner.setValue(Integer.valueOf(properties.getProperty("top.x", TOP_X_VALUES_DEFAULT.toString())));
            monitorIntervalSpinner.setValue(Long.valueOf(properties.getProperty("monitor.interval",
                    MONITOR_INTERVAL_DEFAULT.toString())));
            expensiveStatementLengthSpinner.setValue(Long.valueOf(properties.getProperty("expensives.length",
                    EXPENSIVE_STATEMENTS_LENGTH_DEFAULT.toString())));
            powerConsumptionLengthSpinner.setValue(Long.valueOf(properties.getProperty("pc.length", "10000")));
            powerConsumptionPortSpinner.setValue(Long.valueOf(properties.getProperty("pc.port", "8888")));
            ageSecondsSpinner.setValue(Integer.valueOf(properties.getProperty("age.seconds", "120")));
        } catch (IOException e) {
            JOptionPane.showMessageDialog(this, e.getMessage(), HEADER, JOptionPane.ERROR_MESSAGE);
        }
    }

    private static String getDefaultStatsFileName(String prefix) {
        return prefix.substring(0, 2) + "-statistics-" + getDateString() + ".pmf";
    }

    private static String getDateString() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        return simpleDateFormat.format(new Date(System.currentTimeMillis()));
    }
}
