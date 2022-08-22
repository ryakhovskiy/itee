package org.kr.db.loader.ui.panels;

import javax.swing.*;
import java.awt.*;

/**
 * Created bykron 11.06.2014.
 */
public class JdbcPanel extends JPanel {

    private static final String TOOLTIP_JDBC_DRIVER = "JDBC Driver Name";
    private static final String TOOLTIP_DBHOST_PORT = "Host and Port of the JDBC interface of SAP HANA";
    private static final String TOOLTIP_DBUSER = "The user name to connect to SAP HANA";
    private static final String TOOLTIP_DBPASS = "The password to connect to SAP HANA";

    private static final String HANA_DRIVER = "com.sap.db.jdbc.Driver";
    private static final String[] SUPPORTED_DRIVERS = new String[] {HANA_DRIVER};

    private final JLabel jdbcDriverLabel = new JLabel("JDBC Driver: ");
    private final JComboBox jdbcDriverCombobox = new JComboBox(SUPPORTED_DRIVERS);

    private final JLabel serverLabel = new JLabel("DB Host:Port: ");
    private final JTextField serverTextField = new JTextField("server:30015");

    private final JLabel userLabel = new JLabel("DB User: ");
    private final JTextField userTextField = new JTextField("system");

    private final JLabel passwordLabel = new JLabel("DB Password: ");
    private final JPasswordField passwordField = new JPasswordField("manager");


    public JdbcPanel() {
        super();
        GridBagLayout gridbag = new GridBagLayout();
        setBorder(BorderFactory.createTitledBorder("JDBC Data"));
        setLayout(gridbag);
        jdbcDriverLabel.setLabelFor(jdbcDriverCombobox);
        serverLabel.setLabelFor(serverTextField);
        userLabel.setLabelFor(userTextField);
        passwordLabel.setLabelFor(passwordField);

        jdbcDriverLabel.setToolTipText(TOOLTIP_JDBC_DRIVER);
        jdbcDriverCombobox.setToolTipText(TOOLTIP_JDBC_DRIVER);
        serverLabel.setToolTipText(TOOLTIP_DBHOST_PORT);
        serverTextField.setToolTipText(TOOLTIP_DBHOST_PORT);
        userLabel.setToolTipText(TOOLTIP_DBUSER);
        userTextField.setToolTipText(TOOLTIP_DBUSER);
        passwordLabel.setToolTipText(TOOLTIP_DBPASS);
        passwordField.setToolTipText(TOOLTIP_DBPASS);

        JLabel[] labels = {jdbcDriverLabel, serverLabel, userLabel, passwordLabel};
        JComponent[] textFields = {jdbcDriverCombobox, serverTextField, userTextField, passwordField};
        addLabelComponentRows(labels, textFields, this);
    }

    void setJdbcDriver(final String driver) {
        final String selected = getJdbcDriver();
        if (driver.equals(selected))
            return;
        boolean supported = false;
        for (String drv : SUPPORTED_DRIVERS)
            if (driver.equals(drv)) {
                supported = true;
                break;
            }
        if (!supported)
            throw new UnsupportedOperationException("Unsupported Driver: " + driver);
        jdbcDriverCombobox.setSelectedItem(driver);
    }

    String getJdbcDriver() {
        return jdbcDriverCombobox.getSelectedItem().toString();
    }

    void setServer(String server) {
        serverTextField.setText(server);
    }

    String getServer() {
        return serverTextField.getText();
    }

    void setUser(String user) {
        this.userTextField.setText(user);
    }

    String getUser() {
        return userTextField.getText();
    }

    void setPassword(String password) {
        passwordField.setText(password);
    }

    String getPassword() {
        return new String(passwordField.getPassword());
    }

    public String getJdbcUrl() {
        final String selected = getJdbcDriver();
        if (selected.equals(HANA_DRIVER))
            return String.format("jdbc:sap://%s?user=%s&password=%s", getServer(), getUser(), getPassword());
        else
            throw new UnsupportedOperationException("Unsupported Driver: " + selected);
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

}
