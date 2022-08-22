package org.kr.db.loader.ui.panels;

import org.kr.db.loader.ui.AppMain;
import org.kr.db.loader.ui.automon.AutoRunner;
import org.kr.db.loader.ui.pojo.AutoRunSpec;
import org.kr.db.loader.ui.utils.IOUtils;

import javax.swing.*;
import javax.swing.border.TitledBorder;
import java.awt.*;
import java.awt.event.*;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by kr on 28.04.2014.
 */
public class RootFrame extends JFrame {

    private final java.util.List<DbLoaderPanel> jdbcPanelContainers = new ArrayList<DbLoaderPanel>();
    private final java.util.List<TitledBorder> borders = new ArrayList<TitledBorder>();
    private volatile boolean collapsed = false;

    private final MonitorPanel rtMonitorPanel = new MonitorPanel("rtmon.properties");
    private final MonitorPanel itMonitorPanel = new MonitorPanel("itmon.properties");
    private final DbLoaderPanel rtqpDbLoaderPanel = new DbLoaderPanel("rtqp.properties", true);
    private final DbLoaderPanel rtupDbLoaderPanel = new DbLoaderPanel("rtup.properties", false);
    private final DbLoaderPanel itqpDbLoaderPanel = new DbLoaderPanel("itqp.properties", true);
    private final DbLoaderPanel itupDbLoaderPanel = new DbLoaderPanel("itup.properties", false);
    private final AutoRunner autoRunner = new AutoRunner(rtMonitorPanel, rtupDbLoaderPanel, rtqpDbLoaderPanel,
            itMonitorPanel, itupDbLoaderPanel, itqpDbLoaderPanel);

    public RootFrame() throws IOException {
        super("InTP Monitor " + AppMain.APP_VERSION);
        final MouseListener mouseListener = new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                toggleCollapseButton();
            }
        };
        final JPanel rootPanel = new JPanel();
        setContentPane(rootPanel);
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        rootPanel.setLayout(new BoxLayout(rootPanel, BoxLayout.Y_AXIS));

        final JTabbedPane rootTabbedPanel = new JTabbedPane();
        final JPanel realTimeMainPanel = new JPanel();
        realTimeMainPanel.setLayout(new BoxLayout(realTimeMainPanel, BoxLayout.Y_AXIS));
        final JPanel inTimeMainPanel = new JPanel();
        inTimeMainPanel.setLayout(new BoxLayout(inTimeMainPanel, BoxLayout.Y_AXIS));
        rootTabbedPanel.addTab("Real-Time", realTimeMainPanel);
        rootTabbedPanel.addTab("In-Time", inTimeMainPanel);

        final AutoRunPanel autoRunPanel = new AutoRunPanel();
        rootTabbedPanel.addTab("AutoRun", autoRunPanel);
        rootPanel.add(rootTabbedPanel);

        realTimeMainPanel.add(rtMonitorPanel);
        inTimeMainPanel.add(itMonitorPanel);
        final JPanel realTimePanel = new JPanel();
        final JPanel inTimePanel = new JPanel();
        realTimeMainPanel.add(realTimePanel);
        inTimeMainPanel.add(inTimePanel);

        final JPanel realTimeQueryPanel = new JPanel();
        realTimeQueryPanel.addMouseListener(mouseListener);
        realTimeQueryPanel.setLayout(new BoxLayout(realTimeQueryPanel, BoxLayout.Y_AXIS));
        final TitledBorder rqborder = BorderFactory.createTitledBorder("QUERY [-]");
        realTimeQueryPanel.setBorder(BorderFactory.createCompoundBorder(rqborder,
                BorderFactory.createEmptyBorder(0, 5, 0, 5)));
        borders.add(rqborder);

        realTimeQueryPanel.add(rtqpDbLoaderPanel);
        realTimePanel.add(realTimeQueryPanel);

        final JPanel realTimeUpdatePanel = new JPanel();
        realTimeUpdatePanel.addMouseListener(mouseListener);
        realTimeUpdatePanel.setLayout(new BoxLayout(realTimeUpdatePanel, BoxLayout.Y_AXIS));
        final TitledBorder ruborder = BorderFactory.createTitledBorder("UPDATE [-]");
        borders.add(ruborder);
        realTimeUpdatePanel.setBorder(BorderFactory.createCompoundBorder(ruborder,
                BorderFactory.createEmptyBorder(0,5,0,5)));

        realTimeUpdatePanel.add(rtupDbLoaderPanel);
        realTimePanel.add(realTimeUpdatePanel);

        final JPanel inTimeQueryPanel = new JPanel();
        inTimeQueryPanel.addMouseListener(mouseListener);
        inTimeQueryPanel.setLayout(new BoxLayout(inTimeQueryPanel, BoxLayout.Y_AXIS));
        final TitledBorder iqborder = BorderFactory.createTitledBorder("QUERY [-]");
        borders.add(iqborder);
        inTimeQueryPanel.setBorder(BorderFactory.createCompoundBorder(iqborder,
                BorderFactory.createEmptyBorder(0,5,0,5)));

        inTimeQueryPanel.add(itqpDbLoaderPanel);
        inTimePanel.add(inTimeQueryPanel);

        final JPanel inTimeUpdatePanel = new JPanel();
        inTimeUpdatePanel.addMouseListener(mouseListener);
        inTimeUpdatePanel.setLayout(new BoxLayout(inTimeUpdatePanel, BoxLayout.Y_AXIS));
        final TitledBorder iuborder = BorderFactory.createTitledBorder("UPDATE [-]");
        borders.add(iuborder);
        inTimeUpdatePanel.setBorder(BorderFactory.createCompoundBorder(iuborder,
                BorderFactory.createEmptyBorder(0,5,0,5)));

        inTimeUpdatePanel.add(itupDbLoaderPanel);
        inTimePanel.add(inTimeUpdatePanel);

        this.addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                super.windowClosing(e);
                rtqpDbLoaderPanel.destroyPanel();
                rtupDbLoaderPanel.destroyPanel();
                itqpDbLoaderPanel.destroyPanel();
                itupDbLoaderPanel.destroyPanel();
                itMonitorPanel.destroyPanel();
                rtMonitorPanel.destroyPanel();
                autoRunPanel.destroyPanel();
                try {
                    IOUtils.getInstance().cleanUpWorkingDir();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        });

        jdbcPanelContainers.add(rtqpDbLoaderPanel);
        jdbcPanelContainers.add(rtupDbLoaderPanel);
        jdbcPanelContainers.add(itqpDbLoaderPanel);
        jdbcPanelContainers.add(itupDbLoaderPanel);

        rtMonitorPanel.injectQueryStatsReference(rtqpDbLoaderPanel.getRunningProcessMonitor());
        rtMonitorPanel.injectUpdateStatsReference(rtupDbLoaderPanel.getRunningProcessMonitor());
        itMonitorPanel.injectQueryStatsReference(itqpDbLoaderPanel.getRunningProcessMonitor());
        itMonitorPanel.injectUpdateStatsReference(itupDbLoaderPanel.getRunningProcessMonitor());

        setLocation(100, 25);

        JPanel bottomButtonPanel = new JPanel();
        bottomButtonPanel.setLayout(new FlowLayout());
        JButton exitButton = new JButton("Exit");
        JButton aboutButton = new JButton("About");
        bottomButtonPanel.add(exitButton);
        bottomButtonPanel.add(aboutButton);
        add(bottomButtonPanel);

        exitButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                WindowEvent event = new WindowEvent(RootFrame.this, WindowEvent.WINDOW_CLOSING);
                Toolkit.getDefaultToolkit().getSystemEventQueue().postEvent(event);
            }
        });
        aboutButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                try {
                    new AboutDialog(RootFrame.this).setVisible(true);
                } catch (IOException e1) {
                    JOptionPane.showMessageDialog(RootFrame.this, e1.getMessage(), "ERROR", JOptionPane.ERROR_MESSAGE);
                }
            }
        });
        setResizable(false);
        pack();
        setVisible(true);
    }

    void toggleCollapseButton() {
        collapsed = !collapsed;
        for (DbLoaderPanel panel : jdbcPanelContainers)
            panel.toggleCollapse();
        for (TitledBorder border : borders) {
            if (collapsed)
                border.setTitle(border.getTitle().replace("-", "+"));
            else
                border.setTitle(border.getTitle().replace("+", "-"));
        }
        this.repaint();
        this.pack();
    }

    public void runAutoScenario(final AutoRunSpec autoRunSpec, final AutoRunPanel.LaunchButtonEnabler enabler,
                                final AutoRunPanel.ProgressUpdater rtUpdater,
                                final AutoRunPanel.ProgressUpdater itUpdater) {
        autoRunner.runAutoSpec(autoRunSpec, enabler, rtUpdater, itUpdater);
    }

    public void stopAutoSpec() {
        autoRunner.stopAutoSpec();
    }

    public static void main(String... args) {
        AppMain.main();
    }
}
