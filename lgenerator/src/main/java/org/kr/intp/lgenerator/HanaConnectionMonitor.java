package org.kr.intp.lgenerator;

import javax.swing.*;
import java.awt.*;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.sql.*;

/**
 * Created bykron 08.12.2016.
 */
public class HanaConnectionMonitor implements FocusListener {

    private final JTextField hanaHardwareKeyTextField;
    private final JTextField hanaSystemNoTextField;
    private final JTextField fileNameTextField;
    private final JTextField hanaHostTextField;
    private final JSpinner hanaPortSpinner;
    private final JTextField hanaUserTextField;
    private final JPasswordField hanaPasswordField;

    private String host;
    private int port;
    private String user;
    private String password;

    public HanaConnectionMonitor(JTextField hanaHardwareKeyTextField,
                                 JTextField hanaSystemNoTextField,
                                 JTextField fileNameTextField,
                                 JTextField hanaHostTextField,
                                 JSpinner hanaPortSpinner,
                                 JTextField hanaUserTextField,
                                 JPasswordField hanaPasswordField) {
        this.hanaHardwareKeyTextField = hanaHardwareKeyTextField;
        this.hanaSystemNoTextField = hanaSystemNoTextField;
        this.fileNameTextField = fileNameTextField;
        this.hanaHostTextField = hanaHostTextField;
        this.hanaPortSpinner = hanaPortSpinner;
        this.hanaUserTextField = hanaUserTextField;
        this.hanaPasswordField = hanaPasswordField;
        this.host = hanaHostTextField.getText();
        this.port = (Integer)hanaPortSpinner.getValue();
        this.user = hanaUserTextField.getText();
        this.password = new String(hanaPasswordField.getPassword());
    }

    /**
     * Invoked when a component gains the keyboard focus.
     *
     * @param e
     */
    @Override
    public void focusGained(FocusEvent e) {
        Object osource = e.getSource();
        if (osource == hanaHostTextField) {
            this.host = hanaHostTextField.getText();
        } else if (osource == hanaPortSpinner) {
            this.port = (Integer)hanaPortSpinner.getValue();
        } else if (osource == hanaUserTextField) {
            this.user = hanaUserTextField.getText();
        } else if (osource == hanaPasswordField) {
            this.password = new String(hanaPasswordField.getPassword());
        }
    }

    /**
     * Invoked when a component loses the keyboard focus.
     *
     * @param e
     */
    @Override
    public void focusLost(FocusEvent e) {
        final Object osource = e.getSource();
        if (osource == hanaHostTextField) {
            final String host = hanaHostTextField.getText();
            if (!host.equals(this.host)) {
                this.host = host;
                monitorConnection();
            }
        } else if (osource == hanaPortSpinner) {
            final int port = (Integer)hanaPortSpinner.getValue();
            if (port != this.port) {
                this.port = port;
                monitorConnection();
            }
        } else if (osource == hanaUserTextField) {
            final String user = hanaUserTextField.getText();
            if (!user.equals(this.user)) {
                this.user = user;
                monitorConnection();
            }
        } else if (osource == hanaPasswordField) {
            final String password = new String(hanaPasswordField.getPassword());
            if (!password.equals(this.password)) {
                this.password = password;
                monitorConnection();
            }
        }
    }

    private void monitorConnection() {
        if (null == host || host.trim().isEmpty()) return;
        if (port < 0 || 65536 < port) return;
        if (null == user || user.trim().isEmpty()) return;
        if (null != password && password.trim().isEmpty()) return;
        EventQueue.invokeLater(() -> monitorHanaConnection(host, port, user, password));
    }

    private void monitorHanaConnection(String host, int port, String user, String password) {
        final String url = String.format("jdbc:sap://%s:%d?user=%s&password=%s", host, port, user, password);
        try (Connection connection = DriverManager.getConnection(url);
             PreparedStatement statement =
                     connection.prepareStatement("select hardware_key, system_no, system_id from m_license");
             ResultSet set = statement.executeQuery()) {
            if (!set.next())
                return;
            final String hardwareKey = set.getString(1);
            final String systemNo = set.getString(2);
            final String systemId = set.getString(3);
            hanaHardwareKeyTextField.setText(hardwareKey);
            hanaSystemNoTextField.setText(systemNo);
            fileNameTextField.setText(systemId + ".lic");
        } catch (SQLException e) { /* connection info is wrong */ }
    }
}
