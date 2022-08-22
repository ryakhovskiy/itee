package org.kr.itee.perfmon.ws;

import org.kr.itee.perfmon.ws.utils.IOUtils;
import org.kr.itee.perfmon.ws.web.server.Server;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 *
 */
public class Teardown implements Runnable {

    private final Logger log = Logger.getLogger(Teardown.class);
    private final Server server;

    public Teardown(Server s) {
        this.server = s;
    }

    @Override
    public void run() {
        server.stop();
        try {
            IOUtils.getInstance().cleanUpWorkingDir();
        } catch (IOException e) {
            log.error("Cannot cleanup working directory: " + e.getMessage(), e);
        }
    }
}
