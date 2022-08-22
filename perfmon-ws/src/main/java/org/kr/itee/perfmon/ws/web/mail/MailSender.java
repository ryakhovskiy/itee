package org.kr.itee.perfmon.ws.web.mail;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

/**
 *
 */
public class MailSender {

    private final SMTP smtp;

    public MailSender(SMTP smtp) {
        this(smtp, null);
    }

    public MailSender(SMTP smtp, Proxy proxy) {
        setProxy(proxy);
        this.smtp = smtp;
    }

    private void setProxy(Proxy proxy) {
        if (null == proxy)
            return;
        System.setProperty("proxySet", "true");
        System.setProperty("socksProxyHost", proxy.host);
        System.setProperty("socksProxyPort", String.valueOf(proxy.port));
        System.setProperty("java.net.socks.username", proxy.user);
        System.setProperty("java.net.socks.password", proxy.password);
    }

    public void sendMail(String sender, String recipients, String subject, String body) throws MessagingException {
        Properties props = new Properties();
        props.put("mail.smtp.host", smtp.host);
        props.put("mail.smtp.port", smtp.port);
        props.put("mail.smtp.ssl.enable", true);
        props.put("mail.smtp.auth", true);
        props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.put("mail.smtp.socketFactory.fallback", false);
        props.put("mail.transport.protocol", "smtp");
        props.put("mail.debug", "true");

        Authenticator authenticator = new Authenticator() {
            private PasswordAuthentication passAuthentication = new PasswordAuthentication(smtp.user, smtp.password);
            @Override
            public PasswordAuthentication getPasswordAuthentication() {
                return passAuthentication;
            }
        };

        Session session = Session.getInstance(props, authenticator);
        MimeMessage message = new MimeMessage(session);
        message.setFrom(new InternetAddress(sender));
        String[] recipientsArray = recipients.split(",");

        InternetAddress[] address = new InternetAddress[recipientsArray.length];
        for (int i = 0; i < recipientsArray.length; i++)
            address[i] = new InternetAddress(recipientsArray[i]);

        message.setRecipients(Message.RecipientType.TO, address);
        message.setSubject(subject);
        message.setText(body);
        Transport.send(message);
    }

    public static class SMTP {
        private final String host;
        private final int port;
        private final String user;
        private final String password;
        public SMTP(String host, int port, String user, String password) {
            this.host = host;
            this.port = port;
            this.user = user;
            this.password = password;
        }
    }

    public static class Proxy {
        private final String host;
        private final int port;
        private final String user;
        private final String password;
        public Proxy(String host, int port, String user, String password) {
            this.host = host;
            this.port = port;
            this.user = user;
            this.password = password;
        }
    }
}

