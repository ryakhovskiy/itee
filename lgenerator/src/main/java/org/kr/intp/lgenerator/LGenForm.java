package org.kr.intp.lgenerator;

import javax.swing.*;
import javax.swing.filechooser.FileFilter;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * Created bykron 29.11.2016.
 */
public class LGenForm extends JFrame {

    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(DATE_FORMAT);

    private static final String version;
    static {
        String _version;
        try (InputStream stream =
                     Thread.currentThread().getContextClassLoader().getResourceAsStream("lgenerator.properties")) {
            Properties properties = new Properties();
            properties.load(stream);
            _version = properties.getProperty("lgenerator.version");
        } catch (Exception e) {
            _version = "";
        }
        version = _version;
    }

    public LGenForm() {
        initComponents();
        this.setTitle("In-Time Server License Generator " + version);
        final Dimension dim = Toolkit.getDefaultToolkit().getScreenSize();
        setLocation(dim.width / 2 - this.getSize().width / 2, dim.height / 2 - this.getSize().height / 2);
        isHanaAppCheckBox.addActionListener((ActionEvent ev) -> isHanaAppCheckboxObserver());
        generateButton.addActionListener((e) -> generateLicense());
        addConnectionElementListeners();
        prepareFileChooser();
    }

    private void prepareFileChooser() {
        final JFileChooser fileChooser = new JFileChooser();
        fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
        fileChooser.setMultiSelectionEnabled(false);
        fileChooser.setDialogTitle("In-Time License File");
        fileChooser.setDialogType(JFileChooser.SAVE_DIALOG);
        fileChooser.setFileFilter(new FileFilter() {
            @Override public boolean accept(File f) { return f.isDirectory() || f.getName().endsWith(".lic"); }
            @Override public String getDescription() { return "In-Time License file|.lic"; }
        });
        saveFileButton.addActionListener((e) -> {
            final int returnVal = fileChooser.showSaveDialog(LGenForm.this);
            if (returnVal == JFileChooser.APPROVE_OPTION) {
                final File file = fileChooser.getSelectedFile();
                fileNameTextField.setText(file.getAbsolutePath());
            }
        });
    }

    private void addConnectionElementListeners() {
        //add listeners in case if jdbc driver is available.
        try {
            Class.forName("com.sap.db.jdbc.Driver");
            final HanaConnectionMonitor monitor =
                    new HanaConnectionMonitor(hanaHardwareKeyTextField, hanaSystemNoTextField, fileNameTextField, hanaHostTextField, hanaPortSpinner, hanaUserTextField, hanaPasswordField);
            hanaHostTextField.addFocusListener(monitor);
            hanaPortSpinner.addFocusListener(monitor);
            hanaUserTextField.addFocusListener(monitor);
            hanaPasswordField.addFocusListener(monitor);
        } catch (ClassNotFoundException e) { }
    }

    private void isHanaAppCheckboxObserver() {
        final boolean isHanaApp = isHanaAppCheckBox.isSelected();
        if (isHanaApp) {
            char t = parseIntpType();
            tablesSpinner.setValue(10);
            intpNameTextField.setText(t + "_PCS_Report_App");
            intpNameTextField.setEditable(false);
            tablesSpinner.setEnabled(false);
        } else {
            tablesSpinner.setValue(5);
            intpNameTextField.setText("N/A");
            intpNameTextField.setEditable(true);
            tablesSpinner.setEnabled(true);
        }
    }

    private void generateLicense() {
        final String license = generateLicenseKey();
        licenseKeyTextArea.setText(license);
        if (!exportToFileCheckBox.isSelected())
            return;
        final String filename = fileNameTextField.getText();
        saveToFile(filename, license);
    }

    private void saveToFile(String filename, String license) {
        if (null == license || license.isEmpty())
            return;
        try {
            Files.write(Paths.get(filename), license.getBytes("UTF8"),
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE);
        } catch (Exception e) {
            JOptionPane.showMessageDialog(null,
                    "Error while saving file: " + e.getMessage(),
                    "Error",
                    JOptionPane.ERROR_MESSAGE);
        }
    }

    private String generateLicenseKey() {
        final String intpInstanceId = intpInstanceIdTextField.getText();
        final int intpSize = parseIntpSize();
        if (0 == intpSize)
            return "";
        final char intpType = parseIntpType();
        final String intpName = intpNameTextField.getText();
        final long expiration = parseExpiration();
        if (0 == expiration)
            return "";
        final int tables = parseTablesCount();

        final String hardwareKey = hanaHardwareKeyTextField.getText();
        final String systemNo = hanaSystemNoTextField.getText();

        try {
            return LicenseGenerator.generate(intpInstanceId, intpName,
                            intpType, intpSize, expiration, tables, hardwareKey, systemNo);
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this,
                    "Error while generating license: " + e.getMessage(),
                    "Error",
                    JOptionPane.ERROR_MESSAGE);
            return "";
        }
    }

    private int parseTablesCount() {
        return (Integer)tablesSpinner.getValue();
    }

    private int parseIntpSize() {
        return (Integer)intpSizeSpinner.getValue();
    }

    private char parseIntpType() {
        return intpTypeCombobox.getSelectedItem().toString().charAt(0);
    }

    private long parseExpiration() {
        try {
            final String date = expirationTextField.getText();
            final Date expiration = simpleDateFormat.parse(date);
            return expiration.getTime();
        } catch (Exception e) {
            JOptionPane.showMessageDialog(this,
                    "Wrong date format, expected format: " + DATE_FORMAT,
                    "Error",
                    JOptionPane.ERROR_MESSAGE);
            return 0;
        }
    }

    //auto generated code
    /* ************************************/
    /* ************************************/
    /* ************************************/
    /* ************************************/
    /* ************************************/
    /* ************************************/
    /* ************************************/
    /* ************************************/
    /* ************************************/
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">
    private void initComponents() {

        inTimePanel = new javax.swing.JPanel();
        intpInstanceIdLabel = new javax.swing.JLabel();
        intpSizeLabel = new javax.swing.JLabel();
        intpTypeLabel = new javax.swing.JLabel();
        intpTypeCombobox = new javax.swing.JComboBox();
        intpNameTextField = new javax.swing.JTextField();
        intpNameLabel = new javax.swing.JLabel();
        expirationTextField = new javax.swing.JTextField();
        expirationLabel = new javax.swing.JLabel();
        tablesSpinner = new javax.swing.JSpinner();
        tablesLabel = new javax.swing.JLabel();
        isHanaAppCheckBox = new javax.swing.JCheckBox();
        intpSizeSpinner = new javax.swing.JSpinner();
        intpInstanceIdTextField = new javax.swing.JTextField();
        licenseKeyLabel = new javax.swing.JLabel();
        licenseKeyScrollPane = new javax.swing.JScrollPane();
        licenseKeyTextArea = new javax.swing.JTextArea();
        hanaHardwareKeyTextField = new javax.swing.JTextField();
        hanaHardwareKeyLabel = new javax.swing.JLabel();
        hanaSystemNoLabel = new javax.swing.JLabel();
        hanaSystemNoTextField = new javax.swing.JTextField();
        hintLabel = new javax.swing.JLabel();
        hanaPanel = new javax.swing.JPanel();
        hanaHostTextField = new javax.swing.JTextField();
        hanaPortSpinner = new javax.swing.JSpinner();
        hanaUserTextField = new javax.swing.JTextField();
        hanaPasswordField = new javax.swing.JPasswordField();
        hanaHostLabel = new javax.swing.JLabel();
        hanaPortLabel = new javax.swing.JLabel();
        hanaUserLabel = new javax.swing.JLabel();
        hanaPasswordLabel = new javax.swing.JLabel();
        buttonPanel = new javax.swing.JPanel();
        exitButton = new javax.swing.JButton();
        generateButton = new javax.swing.JButton();
        fileNameTextField = new javax.swing.JTextField();
        exportToFileCheckBox = new javax.swing.JCheckBox();
        saveFileButton = new javax.swing.JButton();

        setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);
        setResizable(false);

        inTimePanel.setBorder(javax.swing.BorderFactory.createTitledBorder("In-Time Parameters"));

        intpInstanceIdLabel.setText("InTP Instance ID");

        intpSizeLabel.setText("InTP Size (MB)");

        intpTypeLabel.setText("InTP Type");

        intpTypeCombobox.setModel(new javax.swing.DefaultComboBoxModel(new String[] { "D", "Q", "T", "P" }));

        intpNameTextField.setText("N/A");

        intpNameLabel.setText("InTP Name");

        expirationTextField.setText("2020-12-31");

        expirationLabel.setText("Expiration");

        tablesSpinner.setModel(new javax.swing.SpinnerNumberModel(10, 1, 10000, 1));

        tablesLabel.setText("Tables");

        isHanaAppCheckBox.setText("Is HANA Application");

        intpSizeSpinner.setModel(new javax.swing.SpinnerNumberModel(Integer.valueOf(10000), Integer.valueOf(10), null, Integer.valueOf(1)));

        intpInstanceIdTextField.setText("000");

        licenseKeyLabel.setText("License Key");

        licenseKeyTextArea.setEditable(false);
        licenseKeyTextArea.setColumns(20);
        licenseKeyTextArea.setLineWrap(true);
        licenseKeyTextArea.setRows(5);
        licenseKeyScrollPane.setViewportView(licenseKeyTextArea);

        hanaHardwareKeyLabel.setText("HANA Hardware Key");

        hanaSystemNoLabel.setText("HANA System No");

        hintLabel.setText("select hardware_key, system_no from m_license");

        hanaPanel.setBorder(javax.swing.BorderFactory.createTitledBorder("SAP HANA Connection"));

        hanaPortSpinner.setModel(new javax.swing.SpinnerNumberModel(30015, 1, 65536, 1));

        hanaHostLabel.setText("Host");

        hanaPortLabel.setText("Port");

        hanaUserLabel.setText("User");

        hanaPasswordLabel.setText("Password");

        javax.swing.GroupLayout hanaPanelLayout = new javax.swing.GroupLayout(hanaPanel);
        hanaPanel.setLayout(hanaPanelLayout);
        hanaPanelLayout.setHorizontalGroup(
                hanaPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                        .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, hanaPanelLayout.createSequentialGroup()
                                .addGroup(hanaPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                        .addComponent(hanaPasswordLabel)
                                        .addComponent(hanaHostLabel, javax.swing.GroupLayout.Alignment.TRAILING)
                                        .addComponent(hanaPortLabel, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.PREFERRED_SIZE, 22, javax.swing.GroupLayout.PREFERRED_SIZE)
                                        .addComponent(hanaUserLabel, javax.swing.GroupLayout.Alignment.TRAILING))
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                                .addGroup(hanaPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                        .addComponent(hanaPasswordField, javax.swing.GroupLayout.PREFERRED_SIZE, 133, javax.swing.GroupLayout.PREFERRED_SIZE)
                                        .addComponent(hanaUserTextField, javax.swing.GroupLayout.PREFERRED_SIZE, 133, javax.swing.GroupLayout.PREFERRED_SIZE)
                                        .addComponent(hanaPortSpinner, javax.swing.GroupLayout.PREFERRED_SIZE, 133, javax.swing.GroupLayout.PREFERRED_SIZE)
                                        .addComponent(hanaHostTextField, javax.swing.GroupLayout.PREFERRED_SIZE, 133, javax.swing.GroupLayout.PREFERRED_SIZE)))
        );
        hanaPanelLayout.setVerticalGroup(
                hanaPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                        .addGroup(hanaPanelLayout.createSequentialGroup()
                                .addGroup(hanaPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                                        .addComponent(hanaHostTextField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                                        .addComponent(hanaHostLabel))
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addGroup(hanaPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                                        .addComponent(hanaPortSpinner, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                                        .addComponent(hanaPortLabel))
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addGroup(hanaPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                                        .addComponent(hanaUserTextField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                                        .addComponent(hanaUserLabel))
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addGroup(hanaPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                                        .addComponent(hanaPasswordField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                                        .addComponent(hanaPasswordLabel))
                                .addGap(0, 0, Short.MAX_VALUE))
        );

        javax.swing.GroupLayout inTimePanelLayout = new javax.swing.GroupLayout(inTimePanel);
        inTimePanel.setLayout(inTimePanelLayout);
        inTimePanelLayout.setHorizontalGroup(
                inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                        .addGroup(javax.swing.GroupLayout.Alignment.LEADING, inTimePanelLayout.createSequentialGroup()
                                .addGap(28, 28, 28)
                                .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                                        .addComponent(licenseKeyLabel)
                                        .addComponent(intpInstanceIdLabel)
                                        .addComponent(intpTypeLabel)
                                        .addComponent(tablesLabel)
                                        .addComponent(intpSizeLabel))
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                                .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                        .addGroup(inTimePanelLayout.createSequentialGroup()
                                                .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                                        .addGroup(inTimePanelLayout.createSequentialGroup()
                                                                .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                                                                        .addGroup(inTimePanelLayout.createSequentialGroup()
                                                                                .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                                                                        .addGroup(inTimePanelLayout.createSequentialGroup()
                                                                                                .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                                                                                                        .addGroup(inTimePanelLayout.createSequentialGroup()
                                                                                                                .addComponent(intpSizeSpinner, javax.swing.GroupLayout.PREFERRED_SIZE, 164, javax.swing.GroupLayout.PREFERRED_SIZE)
                                                                                                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                                                                                                .addComponent(expirationLabel))
                                                                                                        .addGroup(inTimePanelLayout.createSequentialGroup()
                                                                                                                .addComponent(intpInstanceIdTextField, javax.swing.GroupLayout.PREFERRED_SIZE, 164, javax.swing.GroupLayout.PREFERRED_SIZE)
                                                                                                                .addGap(63, 63, 63)
                                                                                                                .addComponent(intpNameLabel)))
                                                                                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                                                                                        .addGroup(inTimePanelLayout.createSequentialGroup()
                                                                                                .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                                                                                        .addComponent(tablesSpinner, javax.swing.GroupLayout.PREFERRED_SIZE, 164, javax.swing.GroupLayout.PREFERRED_SIZE)
                                                                                                        .addComponent(isHanaAppCheckBox))
                                                                                                .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                                                                                        .addGroup(inTimePanelLayout.createSequentialGroup()
                                                                                                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                                                                                                .addComponent(hanaSystemNoLabel)
                                                                                                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                                                                                                        .addGroup(inTimePanelLayout.createSequentialGroup()
                                                                                                                .addGap(18, 18, 18)
                                                                                                                .addComponent(hanaHardwareKeyLabel)
                                                                                                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)))))
                                                                                .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                                                                        .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                                                                                                .addComponent(intpNameTextField)
                                                                                                .addComponent(expirationTextField, javax.swing.GroupLayout.DEFAULT_SIZE, 164, Short.MAX_VALUE))
                                                                                        .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING, false)
                                                                                                .addComponent(hanaSystemNoTextField, javax.swing.GroupLayout.Alignment.LEADING)
                                                                                                .addComponent(hanaHardwareKeyTextField, javax.swing.GroupLayout.Alignment.LEADING, javax.swing.GroupLayout.DEFAULT_SIZE, 162, Short.MAX_VALUE))))
                                                                        .addGroup(inTimePanelLayout.createSequentialGroup()
                                                                                .addGap(0, 0, Short.MAX_VALUE)
                                                                                .addComponent(hintLabel)))
                                                                .addGap(15, 15, 15))
                                                        .addGroup(inTimePanelLayout.createSequentialGroup()
                                                                .addComponent(intpTypeCombobox, javax.swing.GroupLayout.PREFERRED_SIZE, 164, javax.swing.GroupLayout.PREFERRED_SIZE)
                                                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)))
                                                .addComponent(hanaPanel, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                                        .addComponent(licenseKeyScrollPane)))
        );
        inTimePanelLayout.setVerticalGroup(
                inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                        .addGroup(inTimePanelLayout.createSequentialGroup()
                                .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                        .addGroup(inTimePanelLayout.createSequentialGroup()
                                                .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                                                        .addComponent(intpInstanceIdLabel)
                                                        .addComponent(intpInstanceIdTextField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                                                        .addComponent(intpNameLabel)
                                                        .addComponent(intpNameTextField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                                .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                                                        .addComponent(intpSizeLabel)
                                                        .addComponent(intpSizeSpinner, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                                                        .addComponent(expirationLabel)
                                                        .addComponent(expirationTextField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                                .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                                                        .addComponent(intpTypeLabel)
                                                        .addComponent(intpTypeCombobox, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                                                        .addComponent(hintLabel))
                                                .addGap(6, 6, 6)
                                                .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                                        .addComponent(tablesLabel)
                                                        .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                                                                .addComponent(tablesSpinner, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                                                                .addComponent(hanaHardwareKeyLabel)
                                                                .addComponent(hanaHardwareKeyTextField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)))
                                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                                .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                                                        .addComponent(hanaSystemNoLabel)
                                                        .addComponent(hanaSystemNoTextField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                                                        .addComponent(isHanaAppCheckBox))
                                                .addGap(0, 6, Short.MAX_VALUE))
                                        .addComponent(hanaPanel, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addGroup(inTimePanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                                        .addComponent(licenseKeyScrollPane, javax.swing.GroupLayout.PREFERRED_SIZE, 233, javax.swing.GroupLayout.PREFERRED_SIZE)
                                        .addComponent(licenseKeyLabel)))
        );

        exitButton.setText("Exit");
        exitButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                exitButtonActionPerformed(evt);
            }
        });

        generateButton.setText("Generate");
        generateButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                generateButtonActionPerformed(evt);
            }
        });

        fileNameTextField.setText("SYSTEM_ID.lic");

        exportToFileCheckBox.setSelected(true);
        exportToFileCheckBox.setText("Export to file:");

        saveFileButton.setText("...");

        javax.swing.GroupLayout buttonPanelLayout = new javax.swing.GroupLayout(buttonPanel);
        buttonPanel.setLayout(buttonPanelLayout);
        buttonPanelLayout.setHorizontalGroup(
                buttonPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                        .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, buttonPanelLayout.createSequentialGroup()
                                .addGap(25, 25, 25)
                                .addComponent(generateButton)
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                .addComponent(exportToFileCheckBox)
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.UNRELATED)
                                .addComponent(fileNameTextField, javax.swing.GroupLayout.PREFERRED_SIZE, 238, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addComponent(saveFileButton, javax.swing.GroupLayout.PREFERRED_SIZE, 23, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addComponent(exitButton)
                                .addContainerGap())
        );
        buttonPanelLayout.setVerticalGroup(
                buttonPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                        .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, buttonPanelLayout.createSequentialGroup()
                                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                .addGroup(buttonPanelLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                                        .addComponent(exitButton)
                                        .addComponent(generateButton)
                                        .addComponent(fileNameTextField, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                                        .addComponent(exportToFileCheckBox)
                                        .addComponent(saveFileButton))
                                .addContainerGap())
        );

        exitButton.getAccessibleContext().setAccessibleName("exitButton");

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
                layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                        .addGroup(layout.createSequentialGroup()
                                .addComponent(inTimePanel, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                                .addGap(0, 0, Short.MAX_VALUE))
                        .addComponent(buttonPanel, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
                layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                        .addGroup(layout.createSequentialGroup()
                                .addComponent(inTimePanel, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                                .addComponent(buttonPanel, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
        );

        pack();
    }// </editor-fold>

    private void exitButtonActionPerformed(java.awt.event.ActionEvent evt) {
        System.exit(0);
    }

    private void generateButtonActionPerformed(java.awt.event.ActionEvent evt) {
        // TODO add your handling code here:
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(LGenForm.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(LGenForm.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(LGenForm.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(LGenForm.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new LGenForm().setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify
    private javax.swing.JPanel buttonPanel;
    private javax.swing.JButton exitButton;
    private javax.swing.JLabel expirationLabel;
    private javax.swing.JTextField expirationTextField;
    private javax.swing.JCheckBox exportToFileCheckBox;
    private javax.swing.JTextField fileNameTextField;
    private javax.swing.JButton generateButton;
    private javax.swing.JLabel hanaHardwareKeyLabel;
    private javax.swing.JTextField hanaHardwareKeyTextField;
    private javax.swing.JLabel hanaHostLabel;
    private javax.swing.JTextField hanaHostTextField;
    private javax.swing.JPanel hanaPanel;
    private javax.swing.JPasswordField hanaPasswordField;
    private javax.swing.JLabel hanaPasswordLabel;
    private javax.swing.JLabel hanaPortLabel;
    private javax.swing.JSpinner hanaPortSpinner;
    private javax.swing.JLabel hanaSystemNoLabel;
    private javax.swing.JTextField hanaSystemNoTextField;
    private javax.swing.JLabel hanaUserLabel;
    private javax.swing.JTextField hanaUserTextField;
    private javax.swing.JLabel hintLabel;
    private javax.swing.JPanel inTimePanel;
    private javax.swing.JLabel intpInstanceIdLabel;
    private javax.swing.JTextField intpInstanceIdTextField;
    private javax.swing.JLabel intpNameLabel;
    private javax.swing.JTextField intpNameTextField;
    private javax.swing.JLabel intpSizeLabel;
    private javax.swing.JSpinner intpSizeSpinner;
    private javax.swing.JComboBox intpTypeCombobox;
    private javax.swing.JLabel intpTypeLabel;
    private javax.swing.JCheckBox isHanaAppCheckBox;
    private javax.swing.JLabel licenseKeyLabel;
    private javax.swing.JScrollPane licenseKeyScrollPane;
    private javax.swing.JTextArea licenseKeyTextArea;
    private javax.swing.JButton saveFileButton;
    private javax.swing.JLabel tablesLabel;
    private javax.swing.JSpinner tablesSpinner;
    // End of variables declaration
}
