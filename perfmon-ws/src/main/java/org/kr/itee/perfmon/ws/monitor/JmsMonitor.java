package org.kr.itee.perfmon.ws.monitor;

import org.kr.itee.perfmon.ws.conf.ConfigurationManager;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;

import javax.jms.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created bykron 28.07.2014.
 */
public class JmsMonitor implements Callable<List<String>> {

    private final Logger log = Logger.getLogger(JmsMonitor.class);
    private static final Object FAKE = new Object();
    private final ActiveMQConnectionFactory cfactory;
    private Connection connection;
    private Session session;
    private MessageConsumer[] consumers;
    private final ConcurrentHashMap<String, Object> messages = new ConcurrentHashMap<String, Object>();

    private final ConfigurationManager config = ConfigurationManager.instance();

    public JmsMonitor() {
        cfactory = new ActiveMQConnectionFactory(config.getBrokerURL());
    }

    @Override
    public List<String> call() throws Exception {
        if (!config.isJmsEnabled())
            return Collections.singletonList("JMS Monitor is disabled");
        try {
            connection = cfactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Destination[] destinations = config.getDestinations();
            final Thread[] threads = new Thread[destinations.length];
            consumers = new MessageConsumer[destinations.length];
            for (int i = 0; i < destinations.length; i++) {
                consumers[i] = session.createConsumer(destinations[i]);
                final int ii = i;
                threads[i] = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            receiveAll(consumers[ii]);
                        } catch (JMSException e) {
                            e.printStackTrace();
                        }
                    }
                });
                threads[i].start();
            }
            for (Thread t : threads)
                t.join();
            return new ArrayList<>(messages.keySet());
        } finally {
            dispose();
        }
    }

    private void receiveAll(MessageConsumer consumer) throws JMSException {
        Message message;
        while (null != (message = consumer.receiveNoWait())) {
            handleMessage(message);
        }
    }

    private void handleMessage(Message message) {
        if (!(message instanceof TextMessage))
            return;
        final TextMessage textMessage = (TextMessage)message;
        try {
            if (!textMessage.propertyExists("ts"))
                return;
            long ts = textMessage.getLongProperty("ts");
            if (ts < MonitorManager.getMonitorStarted())
                return;
            messages.putIfAbsent(textMessage.getText(), FAKE);
        } catch (JMSException e) {
            log.error(e);
        }
    }

    private void dispose() {
        if (null != consumers) {
            for (MessageConsumer consumer : consumers) {
                try {
                    consumer.close();
                } catch (JMSException e) {
                    log.error(e);
                }
            }
        }

        if (null != session)
            try {
                session.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }

        if (null != connection)
            try {
                connection.close();
            } catch (JMSException e) {
                log.error(e);
            }
    }
}
