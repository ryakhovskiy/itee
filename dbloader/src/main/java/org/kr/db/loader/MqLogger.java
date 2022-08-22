package org.kr.db.loader;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;

import javax.jms.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created bykron 28.07.2014.
 */
public class MqLogger extends Thread {

    private static final ActiveMQConnectionFactory cfactory = new ActiveMQConnectionFactory();

    private static final String MSG_TEMPLATE = "%d\t%s\t%s\t%s";

    private final Logger log = Logger.getLogger(MqLogger.class);
    private final BlockingQueue<String> queue = new ArrayBlockingQueue<String>(1024);
    private final String amqname;

    private Connection connection;
    private Session session;
    private MessageProducer producer;
    private volatile boolean running = true;

    public MqLogger(String brokerUrl) {
        super("MQ_LOG");
        log.debug("Configuring MQ Logger on " + brokerUrl);
        cfactory.setBrokerURL(brokerUrl);
        String ip;
        try {
            InetAddress ia = InetAddress.getLocalHost();
            ip = ia.getHostAddress();
        } catch (UnknownHostException e) {
            log.error(e);
            ip = "127.0.0.1";
        }
        amqname = "adbl." + ip;
        log.debug("configuring queue: " + amqname);
    }

    public void run() {
        try {
            create();
            String msg;
            while (null != (msg = queue.take())) {
                log.debug("sending info: " + msg);
                final TextMessage message = session.createTextMessage();
                message.setLongProperty("ts", AppMain.currentTimeMillis());
                message.setText(msg);
                producer.send(message);
            }
        } catch (Exception e) {
            log.error(e);
        } finally {
            dispose();
        }
    }

    public void log(long start, long end, int size, String sql) {
        if (!running)
            return;
        final String sstart = new Timestamp(start).toString();
        final String sstop = new Timestamp(end).toString();
        final String msg = String.format(MSG_TEMPLATE, size, sstart, sstop, sql);
        try {
            queue.put(msg);
        } catch (InterruptedException e) {
            log.debug("INTERRUPTED");
            Thread.currentThread().interrupt();
        }
    }

    private void create() throws JMSException {
        connection = cfactory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Destination destination = session.createQueue(amqname);
        producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
    }

    private void dispose() {
        running = false;
        queue.clear();
        if (null != producer)
            try {
                producer.close();
            } catch (JMSException e) {
                log.error(e);
            }

        if (null != session)
            try {
                session.close();
            } catch (JMSException e) {
                log.error(e);
            }

        if (null != connection)
            try {
                connection.close();
            } catch (JMSException e) {
                log.error(e);
            }
    }
}
