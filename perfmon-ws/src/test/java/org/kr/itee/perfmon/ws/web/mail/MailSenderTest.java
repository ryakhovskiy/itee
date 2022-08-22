package org.kr.itee.perfmon.ws.web.mail;

import org.junit.Test;

/**
 *
 */
public class MailSenderTest {

    private final String proxyPassword = "xxx";

    @Test
    public void testSendMailWithProxy() throws Exception {
        final String proxyhost = "DESDC3632.atrema.kr.com";
        final int proxyPort = 8080;
        final String proxyUser = "kr";
        MailSender.Proxy proxy = new MailSender.Proxy(proxyhost, proxyPort, proxyUser, proxyPassword);

        final String smtpHost = "smtp.google.com";
        final int smtpPort = 465;
        final String smtpUser = "ryakhovskiy.developer";
        final String smtpPassword = "xxx";
        MailSender.SMTP smtp = new MailSender.SMTP(smtpHost, smtpPort, smtpUser, smtpPassword);

        MailSender sender = new MailSender(smtp, proxy);
        sender.sendMail("ryakhovskiy.developer@gmail.com", "ryakhovskiy.developer@gmail.com", "testemailsubject", "test email body");
    }

}