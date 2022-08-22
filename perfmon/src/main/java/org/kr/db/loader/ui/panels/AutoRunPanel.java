package org.kr.db.loader.ui.panels;

import org.kr.db.loader.ui.AppMain;
import org.kr.db.loader.ui.pojo.AutoRunSpec;
import org.kr.db.loader.ui.pojo.Scenario;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created bykron 29.07.2014.
 */
public class AutoRunPanel extends JPanel {

    private static final String FILE = "specs.data";
    private final Map<String, AutoRunSpec> specs = new HashMap<String, AutoRunSpec>();

    private final JComboBox specsCombobox = new JComboBox();
    private final JTextField specNameTextField = new JTextField(20);

    private final JPanel itPanel = new JPanel();
    private final JSpinner itQueryProcPeriodTF = new JSpinner();
    private final JSpinner itUpdateProcPeriodTF = new JSpinner();
    private final JSpinner itQueryMaxProcTF = new JSpinner();
    private final JSpinner itUpdateMaxProcTF = new JSpinner();
    private final JSpinner itQueryProcCountTF = new JSpinner();
    private final JSpinner itUpdateProcCountTF = new JSpinner();
    private final JSpinner itTimeoutTF = new JSpinner();
    private final JCheckBox isItEnabled = new JCheckBox("Enabled", true);
    private final JProgressBar itProgressbar = new JProgressBar();

    private final JPanel rtPanel = new JPanel();
    private final JSpinner rtQueryProcPeriodTF = new JSpinner();
    private final JSpinner rtUpdateProcPeriodTF = new JSpinner();
    private final JSpinner rtQueryMaxProcTF = new JSpinner();
    private final JSpinner rtUpdateMaxProcTF = new JSpinner();
    private final JSpinner rtQueryProcCountTF = new JSpinner();
    private final JSpinner rtUpdateProcCountTF = new JSpinner();
    private final JSpinner rtTimeoutTF = new JSpinner();
    private final JCheckBox isRtEnabled = new JCheckBox("Enabled", true);
    private final JProgressBar rtProgressbar = new JProgressBar();

    private final JButton saveBtn = new JButton("Save Spec");
    private final JButton removeBtn = new JButton("Remove Spec");
    private final JButton startBtn = new JButton("Start Spec");
    private final JButton stopBtn = new JButton("Stop Spec");

    public AutoRunPanel() {
        super();
        try {
            specs.putAll(loadSpecsFromFile());
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this, "Cannot load specs: " + e.getMessage(), "ERROR",
                    JOptionPane.ERROR_MESSAGE);
        }
        init();
        reloadCombobox();
    }

    private void init() {
        setBorder(BorderFactory.createTitledBorder("Automatic run specification"));
        setLayout(null);
        JLabel label = new JLabel("Specification: ");
        add(label);
        int y = 30;
        label.setBounds(30, y, 80, 20);
        add(specsCombobox);
        specsCombobox.setBounds(120, y, 150, 20);

        y += 25;
        JSeparator separator = new JSeparator(SwingConstants.HORIZONTAL);
        add(separator);
        separator.setBounds(40, y, 220, 3);

        y += 5;
        label = new JLabel("Spec Name: ");
        add(label);
        label.setBounds(30, y, 80, 20);
        add(specNameTextField);
        specNameTextField.setBounds(120, y, 150, 20);

        y += 30;
        addRtPanel(this);
        rtPanel.setBounds(30, y, 400, 220);
        addItPanel(this);
        itPanel.setBounds(450, y, 400, 220);

        y += 250;
        add(removeBtn);
        removeBtn.setBounds(300, y, 120, 28);
        add(saveBtn);
        saveBtn.setBounds(450, y, 120, 28);
        y += 40;
        add(stopBtn);
        stopBtn.setBounds(300, y, 120, 28);
        add(startBtn);
        startBtn.setBounds(450, y, 120, 28);

        specsCombobox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                updateSpecValues();
            }
        });
        removeBtn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                removeSpec();
            }
        });
        saveBtn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                addSpec();
            }
        });
        startBtn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                startSpec();
            }
        });
        stopBtn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                stopSpec();
            }
        });
        isItEnabled.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                panelEnabledHandler(itPanel, isItEnabled);
            }
        });
        isRtEnabled.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                panelEnabledHandler(rtPanel, isRtEnabled);
            }
        });
        stopBtn.setEnabled(false);
    }

    private void addItPanel(JPanel container) {
        final GridBagLayout layout = new GridBagLayout();
        final GridBagConstraints c = new GridBagConstraints();
        c.anchor = GridBagConstraints.FIRST_LINE_START;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.insets = new Insets(3, 3, 0, 3);
        c.gridx = 0;
        c.gridy = 0;
        itPanel.setLayout(layout);
        container.add(itPanel);
        itPanel.setBorder(BorderFactory.createTitledBorder("InTime Scenario Parameters"));

        addLabeledComponent(itPanel, c, "Query Process [Add Period (ms)]: ", itQueryProcPeriodTF);
        addLabeledComponent(itPanel, c, "Query Processes Limit: ", itQueryMaxProcTF);
        addLabeledComponent(itPanel, c, "Query Processes [Add Batch Size]: ", itQueryProcCountTF);
        addLabeledComponent(itPanel, c, "Update Process [Add Period (ms)]: ", itUpdateProcPeriodTF);
        addLabeledComponent(itPanel, c, "Update Processes Limit: ", itUpdateMaxProcTF);
        addLabeledComponent(itPanel, c, "Update Processes [Add Batch Size]: ", itUpdateProcCountTF);
        addLabeledComponent(itPanel, c, "Scenario Timeout (ms): ", itTimeoutTF);
        addComponentsLine(itPanel, c, isItEnabled, itProgressbar);
        isItEnabled.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                panelEnabledHandler(itPanel, isItEnabled);
            }
        });
    }

    private void addRtPanel(JPanel container) {
        final GridBagLayout layout = new GridBagLayout();
        final GridBagConstraints c = new GridBagConstraints();
        c.insets = new Insets(3, 3, 0, 3);
        c.anchor = GridBagConstraints.FIRST_LINE_START;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.gridx = 0;
        c.gridy = 0;
        rtPanel.setLayout(layout);
        container.add(rtPanel);
        rtPanel.setBorder(BorderFactory.createTitledBorder("RealTime Scenario Parameters"));

        addLabeledComponent(rtPanel, c, "Query Process [Add Period (ms)]: ", rtQueryProcPeriodTF);
        addLabeledComponent(rtPanel, c, "Query Processes Limit: ", rtQueryMaxProcTF);
        addLabeledComponent(rtPanel, c, "Query Processes [Add Batch Size]: ", rtQueryProcCountTF);
        addLabeledComponent(rtPanel, c, "Update Process [Add Period (ms)]: ", rtUpdateProcPeriodTF);
        addLabeledComponent(rtPanel, c, "Update Processes Limit: ", rtUpdateMaxProcTF);
        addLabeledComponent(rtPanel, c, "Update Processes [Add Batch Size]: ", rtUpdateProcCountTF);
        addLabeledComponent(rtPanel, c, "Scenario Timeout (ms): ", rtTimeoutTF);
        addComponentsLine(rtPanel, c, isRtEnabled, rtProgressbar);

        isRtEnabled.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                panelEnabledHandler(rtPanel, isRtEnabled);
            }
        });
    }

    private void addLabeledComponent(JPanel container, GridBagConstraints c, String label, Component component) {
        final JLabel jlabel = new JLabel(label);
        jlabel.setLabelFor(component);
        addComponentsLine(container, c, jlabel, component);
    }

    private void addLabeledComponent(JPanel container, GridBagConstraints c,
                                     String l1, Component c1, String l2, Component c2) {
        final JLabel jLabel1 = new JLabel(l1);
        jLabel1.setLabelFor(c1);
        final JLabel jLabel2 = new JLabel(l2);
        jLabel2.setLabelFor(c2);
        addComponentsLine(container, c, new Component[] { jLabel1, c1, jLabel2, c2 });
    }

    private void addComponentsLine(JPanel container, GridBagConstraints c, Component left, Component right) {
        addComponentsLine(container, c, new Component[] { left, right });
    }

    private void addComponentsLine(JPanel container, GridBagConstraints c, Component[] components) {
        for (int i = 0; i < components.length; i++) {
            c.gridx = i;
            container.add(components[i], c);
        }
        c.gridx = 0;
        c.gridy++;
    }

    private void addSpec() {
        final String name = specNameTextField.getText();
        if (name.trim().length() == 0) {
            JOptionPane.showMessageDialog(this, "Spec Name cannot be empty!", "WARN", JOptionPane.WARNING_MESSAGE);
            return;
        }
        final boolean rtEnabled = isRtEnabled.isSelected();
        final boolean itEnabled = isItEnabled.isSelected();

        final int itQueryProcPeriod = (Integer)itQueryProcPeriodTF.getValue();
        final int itUpdateProcPeriod = (Integer)itUpdateProcPeriodTF.getValue();
        final int itTimemout = (Integer)itTimeoutTF.getValue();
        final int itQueryMaxProcesses = (Integer)itQueryMaxProcTF.getValue();
        final int itUpdateMaxProcesses = (Integer)itUpdateMaxProcTF.getValue();
        final int itQueryProcCount = (Integer)itQueryProcCountTF.getValue();
        final int itUpdateProcCount = (Integer)itUpdateProcCountTF.getValue();
        final Scenario itScenario = new Scenario(itQueryProcPeriod, itUpdateProcPeriod, itTimemout, itQueryMaxProcesses,
                itUpdateMaxProcesses, itQueryProcCount, itUpdateProcCount);

        final int rtQueryProcPeriod = (Integer)rtQueryProcPeriodTF.getValue();
        final int rtUpdateProcPeriod = (Integer)rtUpdateProcPeriodTF.getValue();
        final int rtTimemout = (Integer)rtTimeoutTF.getValue();
        final int rtQueryMaxProcesses = (Integer)rtQueryMaxProcTF.getValue();
        final int rtUpdateMaxProcesses = (Integer)rtUpdateMaxProcTF.getValue();
        final int rtQueryProcCount = (Integer)rtQueryProcCountTF.getValue();
        final int rtUpdateProcCount = (Integer)rtUpdateProcCountTF.getValue();
        final Scenario rtScenario = new Scenario(rtQueryProcPeriod, rtUpdateProcPeriod, rtTimemout, rtQueryMaxProcesses,
                rtUpdateMaxProcesses, rtQueryProcCount, rtUpdateProcCount);

        final AutoRunSpec autoRunSpec = new AutoRunSpec(name, itScenario, rtScenario, itEnabled, rtEnabled);
        specs.put(name, autoRunSpec);
        reloadCombobox();
        specsCombobox.setSelectedItem(name);
        JOptionPane.showMessageDialog(this, "Spec saved!", "Info", JOptionPane.INFORMATION_MESSAGE);
    }

    private void removeSpec() {
        final String name = specsCombobox.getSelectedItem().toString();
        if (null == name || name.length() == 0)
            return;
        if (specs.containsKey(name)) {
            specs.remove(name);
            JOptionPane.showMessageDialog(this, "Spec removed!", "Info", JOptionPane.INFORMATION_MESSAGE);
            reloadCombobox();
        } else {
            JOptionPane.showMessageDialog(this, "Spec does not exists!", "Warn", JOptionPane.WARNING_MESSAGE);
        }
    }

    private void updateSpecValues() {
        final Object selected = specsCombobox.getSelectedItem();
        if (null == selected)
            return;
        final String name = selected.toString();
        final AutoRunSpec spec = specs.get(name);
        if (null == spec) {
            JOptionPane.showMessageDialog(this, "Cannot find details for spec: " + name, "WARN",
                    JOptionPane.WARNING_MESSAGE);
            return;
        }

        specNameTextField.setText(name);
        isItEnabled.setSelected(spec.isItEnabled());
        panelEnabledHandler(itPanel, isItEnabled);
        isRtEnabled.setSelected(spec.isRtEnabled());
        panelEnabledHandler(rtPanel, isRtEnabled);
        final Scenario its = spec.getItScenario();
        itQueryProcPeriodTF.setValue(its.getQueryProcPeriod());
        itQueryMaxProcTF.setValue(its.getQueryMaxProcesses());
        itQueryProcCountTF.setValue(its.getQueryProcessesCount());
        itUpdateProcPeriodTF.setValue(its.getUpdateProcPeriod());
        itUpdateMaxProcTF.setValue(its.getUpdateMaxProcesses());
        itUpdateProcCountTF.setValue(its.getUpdateProcessesCount());
        itTimeoutTF.setValue(its.getTimeout());
        itProgressbar.setMinimum(0);
        itProgressbar.setValue(0);
        itProgressbar.setMaximum(its.getTimeout());

        final Scenario rts = spec.getRtScenario();
        rtQueryProcPeriodTF.setValue(rts.getQueryProcPeriod());
        rtQueryMaxProcTF.setValue(rts.getQueryMaxProcesses());
        rtQueryProcCountTF.setValue(rts.getQueryProcessesCount());
        rtUpdateProcPeriodTF.setValue(rts.getUpdateProcPeriod());
        rtUpdateMaxProcTF.setValue(rts.getUpdateMaxProcesses());
        rtUpdateProcCountTF.setValue(rts.getUpdateProcessesCount());
        rtTimeoutTF.setValue(rts.getTimeout());
        rtProgressbar.setMinimum(0);
        rtProgressbar.setValue(0);
        rtProgressbar.setMaximum(rts.getTimeout());
    }

    private void stopSpec() {
        final RootFrame rootFrame = getRootFrame();
        rootFrame.stopAutoSpec();
    }

    private void startSpec() {
        final Object ocurr = specsCombobox.getSelectedItem();
        if (null == ocurr) {
            JOptionPane.showMessageDialog(this, "No chosen scenario to run!", "WARN", JOptionPane.WARNING_MESSAGE);
            return;
        }
        final String name = ocurr.toString();
        final AutoRunSpec spec = specs.get(name);
        if (null == spec) {
            JOptionPane.showMessageDialog(this, "Cannot find scenario: " + name, "WARN", JOptionPane.WARNING_MESSAGE);
            return;
        }

        final RootFrame rootFrame = getRootFrame();
        startBtn.setEnabled(false);
        stopBtn.setEnabled(true);
        rootFrame.runAutoScenario(spec, new LaunchButtonEnabler(), new ProgressUpdater(rtProgressbar),
                new ProgressUpdater(itProgressbar));
    }

    private RootFrame getRootFrame() {
        long start = AppMain.currentTimeMillis();
        Component parent = this.getParent();
        RootFrame rootFrame = null;
        do {
            if (parent instanceof RootFrame)
                rootFrame = (RootFrame)parent;
            else if (AppMain.currentTimeMillis() - start > 3000)
                break;
            parent = parent.getParent();
        } while (null == rootFrame);
        if (null == rootFrame)
            throw new RuntimeException("Internal Error: Cannot find RootFrame");
        return rootFrame;
    }

    private void panelEnabledHandler(JPanel panel, JCheckBox enabledCheckBox) {
        boolean enabled = enabledCheckBox.isSelected();
        for (Component c : panel.getComponents())
            if (!c.equals(enabledCheckBox))
                c.setEnabled(enabled);
    }

    private void reloadCombobox() {
        specsCombobox.removeAllItems();
        String[] vals = specs.keySet().toArray(new String[specs.keySet().size()]);
        Arrays.sort(vals);
        for (String i : vals)
            specsCombobox.addItem(i);
        updateSpecValues();
        specsCombobox.repaint();
    }

    private void saveSpecsToFile() throws IOException {
        ObjectOutputStream writer = null;
        try {
            writer = new ObjectOutputStream(new FileOutputStream(FILE));
            writer.writeObject(specs);
            writer.flush();
        } finally {
            if (null != writer)
                writer.close();
        }
    }

    private Map<String, AutoRunSpec> loadSpecsFromFile() throws IOException, ClassNotFoundException {
        final File f = new File(FILE);
        if (!f.exists())
            return Collections.EMPTY_MAP;
        ObjectInputStream reader = null;
        try {
            reader = new ObjectInputStream(new FileInputStream(f));
            Object o = reader.readObject();
            return (Map<String, AutoRunSpec>)o;
        } finally {
            if (null != reader)
                reader.close();
        }
    }

    public void destroyPanel() {
        try {
            saveSpecsToFile();
        } catch (IOException ex) {
            JOptionPane.showMessageDialog(AutoRunPanel.this, "Cannot save specs: " + ex.getMessage(), "ERROR",
                    JOptionPane.ERROR_MESSAGE);
        }
    }

    public class LaunchButtonEnabler {
        private LaunchButtonEnabler() {}
        public void enableLaunchButton() {
            startBtn.setEnabled(true);
            stopBtn.setEnabled(false);
        }
    }

    public class ProgressUpdater {
        private final JProgressBar progressBar;
        private ProgressUpdater(JProgressBar progressBar) {
            this.progressBar = progressBar;
        }
        public void setProgress(int val) {
            progressBar.setValue(val);
        }
    }
}
