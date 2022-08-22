package org.kr.intp.client;

import org.kr.intp.App;
import org.kr.intp.client.protocol.IntpMessage;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by kr on 18.12.13.
 */
public class ClientMessageHandler {

    public static ClientMessageHandler newInstance() {
        return new ClientMessageHandler();
    }

    private final Logger log = LoggerFactory.getLogger(ClientMessageHandler.class);
    private final ObjectMapper mapper = new ObjectMapper();

    private ClientMessageHandler() { }

    public String handleRequest(String request) {
        log.debug("request received: " + request);
        IntpMessage intpMessage = parseRequest(request);
        switch (intpMessage.getType()) {
            case STATUS:
                handleStatusRequest(intpMessage);
                break;
            case SHUTDOWN:
                handleShutdownRequest(intpMessage);
                break;
            case UNDEFINED:
                log.debug("UNDEFINED request received...");
                break;
            default:
                log.debug("Wrong request received...");
                break;
        }
        return prepareResponse(intpMessage);
    }

    private IntpMessage parseRequest(String request) {
        try {
            return mapper.readValue(request, IntpMessage.class);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return new IntpMessage();
        }
    }

    private IntpMessage handleStatusRequest(IntpMessage intpMessage) {
        Properties properties = intpMessage.getProperties();
        properties.put("comment", "status request received, handling...");
        properties.put("status", "alive");
        intpMessage.setProperties(properties);
        return intpMessage;
    }

    private IntpMessage handleShutdownRequest(IntpMessage intpMessage) {
        sendLazyShutdown();
        Properties properties = intpMessage.getProperties();
        properties.put("comment", "shutdown request received, handling...");
        properties.put("status", "shutting down");
        intpMessage.setProperties(properties);
        return intpMessage;
    }

    private void sendLazyShutdown() {
        Runnable r = new Runnable() {
            public void run() {
                try {
                    Thread.sleep(1200);
                } catch (InterruptedException ignored) {
                    log.trace(ignored.getMessage(), ignored);
                    Thread.currentThread().interrupt();
                }
                App.stop(); }
        };
        new Thread(r).start();
    }

    private String prepareResponse(IntpMessage message) {
        try {
            return mapper.writeValueAsString(message);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return message.getType().toString();
        }
    }
}
