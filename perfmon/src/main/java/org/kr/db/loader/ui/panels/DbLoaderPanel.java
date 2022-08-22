package org.kr.db.loader.ui.panels;

import org.kr.db.loader.ui.AppMain;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.sql.Timestamp;
import java.util.*;

/**
 * Created by kr on 28.04.2014.
 */
public class DbLoaderPanel extends JPanel {

    private static final String TOOLTIP_SCHEDULER_INTERVAL = "An interval in milliseconds pointing the frequency with which the SQL queries are executed";
    private static final String TOOLTIP_QUERIES_PER_INTERVAL = "Specifies how many SQL queries are executed per each interval";
    private static final String TOOLTIP_CONCURRENT_EXECUTORS = "Specifies how many concurrent (parallel) executors send queries to the DB";
    private static final String TOOLTIP_QUERY_TYPE = "Specifies the type of the query, which can be either [QUERY] (SQL statement) or [CALL] (stored procedure)";
    private static final String TOOLTIP_QUERY_FILE = "The source file containing SQL queries (separated by the line terminator symbol)";
    private static final String TOOLTIP_QUERY_FILE_ENCODING = "Encoding of the source file containing SQL queries";
    private static final String TOOLTIP_EXEC_TIME = "Specifies the time of the test execution. A zero (‘0’) in this field means that the test is time-unlimited";
    private static final String TOOLTIP_ROUND_ROBIN = "Specifies the strategy of handling a query file. If this checkbox is activated and all queries from the file are read, the queries will be continuously executed (until the Execution Time ends)";
    private static final String TOOLTIP_CONNECTION_POOL = "Reuses connections (when the checkbox is activated) or creates a new one for each query execution";
    private static final String TOOLTIP_TOTAL_PROCESSES_RUNNING = "Currently running processes";

    protected final GridBagConstraints gbc = new GridBagConstraints();

    private final ProcessController processController;
    private final String[] queryTypes = {"query", "call"};
    private final JdbcPanel jdbcPanel = new JdbcPanel();
    private final JSpinner schedulerIntervalMsSpinner = new JSpinner();
    private final JSpinner queriesPerIntervalSpinner = new JSpinner();
    private final JSpinner concurrentExecutorsSpinner = new JSpinner();
    private final JComboBox queryTypeCombobox = new JComboBox(queryTypes);
    private final JTextField queryFileTextField = new JTextField(30);
    private final JTextField queryFileEncTextField = new JTextField("UTF8");
    private final JSpinner execTimeSpinner = new JSpinner();
    private final JCheckBox roundRobinCheckBox = new JCheckBox("Round Robin", true);
    private final JCheckBox isConnectionPoolingEnabled = new JCheckBox("Connection Pooling Enabled", false);
    private final JSpinner totalProcessesRunningSpinner = new JSpinner();
    private final JButton addProcessButton = new JButton("Add Process");
    private final JButton removeProcessButton = new JButton("Remove Process");
    private final JButton removeAllProcessesButton = new JButton("Remove ALL");
    private final JButton openFileDialogButton = new JButton("...");

    private final String defaultsFileName;
    private final boolean useJmsTrace;

    private final ProcessSpinnerController processSpinnerController = new ProcessSpinnerController();

    public DbLoaderPanel(final String defaultsFileName, boolean useJmsTrace) throws IOException {
        super();
        this.defaultsFileName = defaultsFileName;
        processController = new ProcessController(processSpinnerController);
        init();
        loadDefaults();
        this.useJmsTrace = useJmsTrace;
    }

    private void init() {
        setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createEmptyBorder(),
                BorderFactory.createEmptyBorder(0, 0, 0, 0)));
        final JPanel rootContainer = new JPanel();
        rootContainer.setBorder(BorderFactory.createCompoundBorder(
                BorderFactory.createEmptyBorder(),
                BorderFactory.createEmptyBorder(0,0,0,0)));
        add(rootContainer);
        rootContainer.setLayout(new BoxLayout(rootContainer, BoxLayout.Y_AXIS));
        rootContainer.add(jdbcPanel);

        final JPanel gridPanel = new JPanel();
        rootContainer.add(gridPanel);
        gridPanel.setLayout(new GridBagLayout());
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.fill = GridBagConstraints.BOTH;

        openFileDialogButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                final JFileChooser fileChooser = new JFileChooser();
                fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
                fileChooser.setCurrentDirectory(new File("."));
                fileChooser.setMultiSelectionEnabled(false);
                int option = fileChooser.showDialog(DbLoaderPanel.this, "Select");
                switch (option) {
                    case JFileChooser.APPROVE_OPTION:
                        File file = fileChooser.getSelectedFile();
                        queryFileTextField.setText(file.getAbsolutePath());
                        break;
                    default:
                        break;
                }
            }
        });

        addComponentWithLabel(gridPanel, "Scheduler Interval (ms): ", schedulerIntervalMsSpinner, TOOLTIP_SCHEDULER_INTERVAL);
        schedulerIntervalMsSpinner.setToolTipText(TOOLTIP_SCHEDULER_INTERVAL);
        addComponentWithLabel(gridPanel, "Queries per interval: ", queriesPerIntervalSpinner, TOOLTIP_QUERIES_PER_INTERVAL);
        queriesPerIntervalSpinner.setToolTipText(TOOLTIP_QUERIES_PER_INTERVAL);
        addComponentWithLabel(gridPanel, "Concurrent Executors: ", concurrentExecutorsSpinner, TOOLTIP_CONCURRENT_EXECUTORS);
        concurrentExecutorsSpinner.setToolTipText(TOOLTIP_CONCURRENT_EXECUTORS);
        addComponentWithLabel(gridPanel, "Query Type: ", queryTypeCombobox, TOOLTIP_QUERY_TYPE);
        queryTypeCombobox.setToolTipText(TOOLTIP_QUERY_TYPE);
        addFileChooserLine(gridPanel);
        addComponentWithLabel(gridPanel, "Query File Encoding: ", queryFileEncTextField, TOOLTIP_QUERY_FILE_ENCODING);
        queryFileEncTextField.setToolTipText(TOOLTIP_QUERY_FILE_ENCODING);
        addComponentWithLabel(gridPanel, "Execution Time (seconds): ", execTimeSpinner, TOOLTIP_EXEC_TIME);
        execTimeSpinner.setToolTipText(TOOLTIP_EXEC_TIME);
        addComponent(gridPanel, roundRobinCheckBox);
        roundRobinCheckBox.setToolTipText(TOOLTIP_ROUND_ROBIN);
        addComponent(gridPanel, isConnectionPoolingEnabled);
        isConnectionPoolingEnabled.setToolTipText(TOOLTIP_CONNECTION_POOL);
        isConnectionPoolingEnabled.setVisible(false);
        addComponentWithLabel(gridPanel, "Total Processes Running: ", totalProcessesRunningSpinner, TOOLTIP_TOTAL_PROCESSES_RUNNING);
        addButtons(gridPanel);

        schedulerIntervalMsSpinner.setValue(1);
        queriesPerIntervalSpinner.setValue(1);
        concurrentExecutorsSpinner.setValue(1);
        execTimeSpinner.setValue(0);
        totalProcessesRunningSpinner.setValue(0);
        final JTextField field = ((JSpinner.NumberEditor)totalProcessesRunningSpinner.getEditor()).getTextField();
        field.setDisabledTextColor(Color.RED);
        totalProcessesRunningSpinner.setEnabled(false);
        totalProcessesRunningSpinner.setToolTipText(TOOLTIP_TOTAL_PROCESSES_RUNNING);

        addProcessButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                tryStartProcess(false);
            }
        });

        removeProcessButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                tryStopProcess();
            }
        });

        removeAllProcessesButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                tryStopAllProcesses(false);
            }
        });


    }

    private void addComponentWithLabel(final JPanel container, final String label,
                                       final Component component, final String labeltooltip) {
        final JLabel jlabel = new JLabel(label);
        jlabel.setToolTipText(labeltooltip);
        addComponentsLine(container, jlabel, component);
    }

    private void addComponentsLine(final JPanel container, final Component rightComponent,
                                   final Component leftComponent) {
        gbc.gridx = 0;
        container.add(rightComponent, gbc);
        gbc.gridx++;
        container.add(leftComponent, gbc);
        gbc.gridy++;
        gbc.gridx = 0;
    }

    private void addComponent(final JPanel container, final Component component) {
        gbc.gridx = 0;
        container.add(component, gbc);
        gbc.gridy++;
    }

    private void addFileChooserLine(JPanel container) {
        JPanel panel = new JPanel(new FlowLayout());
        panel.add(queryFileTextField, BorderLayout.CENTER);
        queryFileTextField.setToolTipText(TOOLTIP_QUERY_FILE);
        panel.add(openFileDialogButton, BorderLayout.WEST);
        openFileDialogButton.setToolTipText(TOOLTIP_QUERY_FILE);
        addComponentWithLabel(container, "Query File: ", panel, TOOLTIP_QUERY_FILE);
    }

    private void addButtons(JPanel container) {
        gbc.gridx = 1;
        final JPanel panel = new JPanel();
        container.add(panel, gbc);
        panel.add(addProcessButton);
        panel.add(removeProcessButton);
        panel.add(removeAllProcessesButton);
        gbc.gridy++;
        gbc.gridx = 0;
    }

    private void tryStartProcess(boolean silent) {
        try {
            final Properties properties = getPanelProperties();
            final boolean started = useJmsTrace ? processController.startProcess(properties, AppMain.BROKER_URL) :
                    processController.startProcess(properties);
            if (started) {
                if (!silent)
                    JOptionPane.showMessageDialog(this, "Process started", "Warning", JOptionPane.WARNING_MESSAGE);
            } else {
                JOptionPane.showMessageDialog(this, "Cannot start process...", "Warning", JOptionPane.WARNING_MESSAGE);
            }
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
        }
    }

    private void tryStopAllProcesses(boolean silent) {
        if (processController.stopAllProcesses()) {
            if (!silent)
                JOptionPane.showMessageDialog(this, "All Process are stopped", "Warning", JOptionPane.WARNING_MESSAGE);
        } else {
            JOptionPane.showMessageDialog(this, "Cannot stop processes...", "Warning", JOptionPane.WARNING_MESSAGE);
        }
    }

    private void tryStopProcess() {
        if (processController.stopProcess()) {
            JOptionPane.showMessageDialog(this, "Process stopped", "Warning", JOptionPane.WARNING_MESSAGE);
        } else {
            JOptionPane.showMessageDialog(this, "Cannot stop process...", "Warning", JOptionPane.WARNING_MESSAGE);
        }
    }

    private Properties getPanelProperties() {
        return getPanelProperties(true);
    }

    private Properties getPanelProperties(boolean withPassword) {
        Properties properties = new Properties();
        properties.put("jdbc.url", jdbcPanel.getJdbcUrl());
        properties.put("jdbc.driver", jdbcPanel.getJdbcDriver());
        properties.put("jdbc.server", jdbcPanel.getServer());
        properties.put("jdbc.user", jdbcPanel.getUser());
        properties.put("jdbc.pass", jdbcPanel.getPassword());
        properties.put("scheduler.interval.ms", schedulerIntervalMsSpinner.getValue().toString());
        properties.put("queries.per.interval", queriesPerIntervalSpinner.getValue().toString());
        properties.put("concurrent.executors", concurrentExecutorsSpinner.getValue().toString());
        properties.put("query.type", queryTypeCombobox.getSelectedItem());
        properties.put("query.file", queryFileTextField.getText());
        properties.put("query.file.encoding", queryFileEncTextField.getText());
        properties.put("round.robin", roundRobinCheckBox.isSelected() ? "true" : "false");
        properties.put("connection.pooling.enabled", isConnectionPoolingEnabled.isSelected() ? "true" : "false");
        properties.put("exec.time", execTimeSpinner.getValue().toString());
        return properties;
    }

    private void loadDefaults() throws IOException {
        File f = new File(defaultsFileName);
        if (!f.exists())
            return;
        Properties properties = new Properties();
        properties.load(new FileInputStream(defaultsFileName));
        jdbcPanel.setJdbcDriver(properties.getProperty("jdbc.driver"));
        jdbcPanel.setServer(properties.getProperty("jdbc.server"));
        jdbcPanel.setUser(properties.getProperty("jdbc.user"));
        jdbcPanel.setPassword(properties.getProperty("jdbc.pass"));
        schedulerIntervalMsSpinner.setValue(getIntProperty(properties, "scheduler.interval.ms", 1));
        queriesPerIntervalSpinner.setValue(getIntProperty(properties, "queries.per.interval", 1));
        concurrentExecutorsSpinner.setValue(getIntProperty(properties, "concurrent.executors", 1));
        queryTypeCombobox.setSelectedItem(properties.getProperty("query.type"));
        queryFileTextField.setText(properties.getProperty("query.file"));
        queryFileEncTextField.setText(properties.getProperty("query.file.encoding"));
        roundRobinCheckBox.setSelected(properties.getProperty("round.robin").equals("true"));
        isConnectionPoolingEnabled.setSelected(properties.getProperty("connection.pooling.enabled").equals("true"));
        execTimeSpinner.setValue(getIntProperty(properties, "exec.time", 0));
    }

    private void saveProperties() throws IOException {
        File file = new File(defaultsFileName);
        FileOutputStream fileOutputStream = null;
        try {
            Properties properties = getPanelProperties(false);
            fileOutputStream = new FileOutputStream(file);
            properties.store(fileOutputStream, "Panel Defaults");
            fileOutputStream.flush();
        } finally {
            if (null != fileOutputStream)
                fileOutputStream.close();
        }
    }

    private int getIntProperty(Properties properties, String name, int defaultValue) {
        if (!properties.containsKey(name))
            return defaultValue;
        final String value = properties.getProperty(name);
        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public void destroyPanel() {
        try {
            saveProperties();
            processController.destroyAll();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public RunningProcessesMonitor getRunningProcessMonitor() {
        return new RunningProcessesMonitor();
    }

    void toggleCollapse() {
        jdbcPanel.setVisible(!jdbcPanel.isVisible());
    }

    private int getRunningProcessesCount() {
        return processSpinnerController.getRunningProcessesCount();
    }

    public class RunningProcessesMonitor {
        private RunningProcessesMonitor() { }
        public int getRunningProcessesCount() { return DbLoaderPanel.this.getRunningProcessesCount(); }
    }

    class ProcessSpinnerController {

        private final Map<Long, Integer> processesRunning = new HashMap<Long, Integer>();
        private final Object locker = new Object();

        private ProcessSpinnerController() { }

        public void increase() {
            synchronized (locker) {
                final Integer value = (Integer) totalProcessesRunningSpinner.getValue() + 1;
                totalProcessesRunningSpinner.setValue(value);
                processesRunning.put(AppMain.currentTimeMillis(), value);
            }
        }

        public void decrease() {
            synchronized (locker) {
                final Integer value = (Integer) totalProcessesRunningSpinner.getValue() - 1;
                totalProcessesRunningSpinner.setValue(value);
                processesRunning.put(AppMain.currentTimeMillis(), value);
            }
        }

        public int getRunningProcessesCount() {
            return (Integer)totalProcessesRunningSpinner.getValue();
        }

        private Map<Long, Integer> getProcessesRunningStatistics() {
            synchronized (processesRunning) {
                return new HashMap<Long, Integer>(processesRunning);
            }
        }

        private void clearStatistics() {
            synchronized (processesRunning) {
                processesRunning.clear();
                processesRunning.put(AppMain.currentTimeMillis(), (Integer)totalProcessesRunningSpinner.getValue());
            }
        }
    }

    class ProcessesRunningStatisticsPointer {

        private ProcessesRunningStatisticsPointer() { }

        public java.util.List<String>  getProcessesRunningStatistics() {
            return prepareStats();
        }

        public void clearStatistics() {
            processSpinnerController.clearStatistics();
        }

        private java.util.List<String> prepareStats() {
            final String linesep = System.getProperty("line.separator");
            final Map<Long, Integer> statsMap = processSpinnerController.getProcessesRunningStatistics();
            Set<Long> keys = new TreeSet<Long>(statsMap.keySet());
            final StringBuilder stringBuilder = new StringBuilder();
            for (Long k : keys)
                stringBuilder.append(new Timestamp(k)).append('\t');
            if (stringBuilder.length() > 0)
                stringBuilder.deleteCharAt(stringBuilder.length() - 1).append(linesep);
            for (Long k : keys)
                stringBuilder.append(statsMap.get(k)).append('\t');
            return Collections.singletonList(stringBuilder.toString());
        }
    }

    public void addProcesses(int count) {
        for (int i = 0; i < count; i++)
            tryStartProcess(true);
    }

    public void stopProcesses() {
        tryStopAllProcesses(true);
    }
}
