package org.kr.itee.perfmon.ws.web.server;

import org.kr.itee.perfmon.ws.web.rest.PerfmonRSWS;
import com.sun.jersey.api.container.filter.GZIPContentEncodingFilter;
import com.sun.jersey.api.core.DefaultResourceConfig;
import com.sun.jersey.simple.container.SimpleServerFactory;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;

/**
 *
 */
public class Server {

    private final Logger log = Logger.getLogger(Server.class);
    private final String address;
    private volatile boolean running;
    private Closeable server;

    public Server(int port) {
        address = "http://localhost:" + port;
    }

    @SuppressWarnings("unchecked")
    public boolean start() {
        final DefaultResourceConfig resourceConfig = new DefaultResourceConfig(PerfmonRSWS.class);
        resourceConfig.getContainerResponseFilters().add(new GZIPContentEncodingFilter());
        try {
            server = SimpleServerFactory.create(address, resourceConfig);
            log.info("web server started");
            running = true;
        } catch (IOException e) {
            log.error("error while starting web server");
            server = null;
            running = false;
        }
        return running;
    }

    public boolean stop() {
        log.info("stopping web server...");
        if (null == server) {
            log.info("web server has not been initialized");
            running = false;
            return false;
        }
        try {
            server.close();
            log.info("web server stopped");
            running = false;
            return true;
        } catch (IOException e) {
            log.error("error while stopping web server: " + e.getMessage(), e);
            return false;
        } finally {
            server = null;
        }
    }

    public boolean isRunning() {
        return running;
    }
}
