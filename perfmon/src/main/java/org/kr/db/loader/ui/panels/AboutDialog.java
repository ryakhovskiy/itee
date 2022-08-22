package org.kr.db.loader.ui.panels;

import org.kr.db.loader.ui.AppMain;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.IOException;
import java.net.URI;

/**
 * Created by kr on 5/21/2014.
 */
public class AboutDialog extends JDialog {

    private static final String[] DEVELOPERS = {
            "KR", "ryakhovskiy.developer@gmail.com"
    };
    private static final String TOOL_NAME = "InTP Monitor";
    private static final String COMPANY = "ORG KR";
    private static final String COMPANY_URI = "";
    private static final String LOGO_RES = "org/kr/db/loader/ui/logo.png";
    private static final String VERSION = AppMain.APP_VERSION;

    public AboutDialog(JFrame parent) throws IOException {
        super(parent, COMPANY, true);
        init();
        pack();
        setCenterLocation();
        setResizable(false);
    }

    private void init() {
        final Box box = Box.createVerticalBox();
        box.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
        box.add(Box.createGlue());

        final ImageIcon logo = createImageIcon(LOGO_RES);
        final JLabel companyLogoLabel = new JLabel(" ", logo, SwingConstants.CENTER);
        companyLogoLabel.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        companyLogoLabel.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                try {
                    Desktop.getDesktop().browse(new URI(COMPANY_URI));
                } catch (Exception ex) {
                    //skip
                }
            }
        });
        box.add(companyLogoLabel);

        for (int i = 0; i < DEVELOPERS.length; i += 2) {
            box.add(createDeveloperLabel(i));
        }

        box.add(new JLabel("<html><font size=3>" + TOOL_NAME + "</font></html>"));
        box.add(new JLabel("<html><font size=3>Version: " + VERSION + "</font></html>"));

        final JLabel companyLabel = new JLabel("<html><font size=3><a href=#>" + COMPANY + "</a></font></html>");
        companyLabel.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        companyLabel.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                try {
                    Desktop.getDesktop().browse(new URI(COMPANY_URI));
                } catch (Exception ex) {
                    //skip
                }
            }
        });
        box.add(companyLabel);

        box.add(new JLabel("<html><font size=3>Technology (c) 2014</font></html>"));
        box.add(Box.createGlue());
        getContentPane().add(box, "Center");

        final JPanel panel = new JPanel();
        final JButton okBtn = new JButton("Ok");
        panel.add(okBtn);
        getContentPane().add(panel, "South");

        okBtn.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                setVisible(false);
                AboutDialog.this.dispose();
            }
        });
    }

    private void setCenterLocation() {
        final Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
        final double screenwidth = screenSize.getWidth();
        final double screenheight = screenSize.getHeight();
        final Dimension dialogSize = this.getSize();
        final double dialogwidth = dialogSize.getWidth();
        final double dialogheight = dialogSize.getHeight();
        final int x = (int)(screenwidth / 2 - (dialogwidth / 2));
        final int y = (int)(screenheight / 2 - (dialogheight / 2));
        setLocation(x, y);
    }

    private JLabel createDeveloperLabel(final int devIndex) {
        final String data = String.format("<html><font size=3>%s, <a href=#>%s</a></font></html>",
                DEVELOPERS[devIndex], DEVELOPERS[devIndex + 1]);
        JLabel label = new JLabel(data);
        label.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
        label.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                try {
                    Desktop.getDesktop().mail(new URI("mailto:" + DEVELOPERS[devIndex + 1] + "?subject=" + TOOL_NAME));
                } catch (Exception ex) {
                    //skip
                }
            }
        });
        return label;
    }

    protected ImageIcon createImageIcon(String path) {
        java.net.URL imgURL = ClassLoader.getSystemResource(path);
        if (imgURL != null) {
            return new ImageIcon(imgURL);
        } else {
            System.err.println("Couldn't find file: " + path);
            return null;
        }
    }

    /**
     * Test About Dialog
     */
    public static void main(String... args) throws IOException {
        AboutDialog dialog = new AboutDialog(new JFrame());
        dialog.setVisible(true);
    }
}
