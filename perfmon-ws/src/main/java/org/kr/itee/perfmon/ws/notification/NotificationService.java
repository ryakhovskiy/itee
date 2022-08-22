package org.kr.itee.perfmon.ws.notification;

import org.kr.itee.perfmon.ws.conf.ConfigurationManager;
import org.kr.itee.perfmon.ws.pojo.MessagePOJO;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.kr.itee.perfmon.ws.utils.IOUtils;
import org.apache.log4j.Logger;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.URLDataSource;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

/**
 */
public class NotificationService {
    private final ConfigurationManager config = ConfigurationManager.instance();
    private final Logger log = Logger.getLogger(NotificationService.class);

    private static NotificationService ourInstance = new NotificationService();

    public static NotificationService getInstance() {
        return ourInstance;
    }

    private NotificationService() {
    }

    public boolean sendNotification(MessagePOJO message){
        Properties connectionProperties = new Properties();
        connectionProperties.put("mail.smtp.host", "mail.glbsnet.com");
        connectionProperties.put("mail.smtp.port", "25");
        connectionProperties.put("mail.smtp.auth", "false");

        Session session = Session.getInstance(connectionProperties);

        try {
            javax.mail.Message msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress("in-time.support@kr.de"));
            log.trace("Recipients: " + message.getRecipients());
            msg.setRecipients(Message.RecipientType.BCC,
                    InternetAddress.parse(message.getRecipients()));
            msg.setSubject(message.getHeading());

            // Create Message
            MimeMultipart multipart = new MimeMultipart("related");
            BodyPart messageBodyPart = new MimeBodyPart();
            messageBodyPart.setContent(message.getContent(), "text/html");
            multipart.addBodyPart(messageBodyPart);
            if(!addImages("image", 1,10, multipart)){
                log.error("Could not add images");
                return false;
            }
            msg.setContent(multipart);

            Transport tr = session.getTransport("smtp");
            log.trace("Trying to establish connection...");
            tr.connect();
            log.trace("Connection established...");
            msg.saveChanges();      // don't forget this
            log.trace("Sending message...");
            tr.sendMessage(msg, msg.getAllRecipients());
            log.trace("Message sent...");
            tr.close();
            log.trace("Transport Closed...");
            return true;

        } catch (MessagingException e) {
            log.error(e.getMessage());
            return false;
        } catch(Exception e1){
            log.error(e1.getMessage());
            return false;
        }
    }

    public boolean addImages(String prefix, int counterStart, int counterEnd, MimeMultipart multipart){
        ClassLoader classLoader = getClass().getClassLoader();
        for(int i = counterStart;i <= counterEnd; i++){
            BodyPart messageBodyPart = new MimeBodyPart();
            DataSource fds = new URLDataSource(classLoader.getResource("template_files/"+prefix + String.format("%03d", i) + ".png"));
            try {
                messageBodyPart.setDataHandler(new DataHandler(fds));
                messageBodyPart.setHeader("Content-ID", "<image" + String.format("%03d", i)+ ">");
                multipart.addBodyPart(messageBodyPart);
            } catch (MessagingException e) {
                log.error("Could not add image with id " + prefix + i);
                return false;
            }
        }
        return true;
    }

    public boolean sendMessageType(MessageType type){
        String template;

        DateFormat df = new SimpleDateFormat("dd.MM.yyyy");
        String currentDate = df.format(new Date());

        try {
            template = IOUtils.getInstance().getResourceAsString("template.html");
        } catch (IOException e) {
            log.error("Could not retrieve template file for email notification.");
            return false;
        }

        switch (type){
            case START:{
                template = template.replace("###DATE###",currentDate)
                        .replace("###HEADING###", "Load Generation on Demo Landscape started")
                        .replace("###CONTENT###", "the load generation on the demo landscape just started.");
                return sendNotification(new MessagePOJO(config.getNotificationRecipients(),
                        "Info: Load Generation on Demo Landscape started",
                        template));
            }
            case STOP: {
                template = template.replace("###DATE###",currentDate)
                        .replace("###HEADING###", "Load Generation on Demo Landscape stopped")
                        .replace("###CONTENT###", "the load generation on the demo landscape just stopped.");
                return sendNotification(new MessagePOJO(config.getNotificationRecipients(),
                        "Info: Load Generation on Demo Landscape stopped",
                        template));
            }
        }
        return false;
    }
}
