package org.kr.itee.perfmon.ws.web;

import org.kr.itee.perfmon.ws.pojo.RequestPOJO;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class RequestHandler {

    private final Logger log = Logger.getLogger(RequestHandler.class);
    private final AtomicInteger counter = new AtomicInteger(0);
    private final Map<Integer, AutorunController> autorunControllers = new HashMap<>();
    private final RequestConverter converter = new RequestConverter();

    public RequestHandler() { }

    @SuppressWarnings("unchecked")
    public int startAutorun(String json) throws Exception {
        final int id = counter.incrementAndGet();
        final RequestPOJO request = converter.convertRequest(json);
        log.info("starting autorun job #" + id);
        synchronized (autorunControllers) {
            autorunControllers.put(id, new AutorunController(request));
        }
        final AutorunController controller = autorunControllers.get(id);
        if (null == controller)
            return -1;
        controller.start();
        return id;
    }

    public boolean stopAutorun(int id) throws Exception {
        if (log.isDebugEnabled())
            log.debug("stopping autorun job #" + id);
        AutorunController controller;
        synchronized (autorunControllers) {
            controller = autorunControllers.remove(id);
        }
        if (null == controller)
            return true;
        controller.stop();
        return true;
    }

    public void stopAll() throws Exception {
        log.info("stopping all autorun jobs");
        synchronized (autorunControllers) {
            for (Map.Entry<Integer, AutorunController> e : autorunControllers.entrySet()) {
                e.getValue().stop();
            }
            autorunControllers.clear();
        }
    }
}
