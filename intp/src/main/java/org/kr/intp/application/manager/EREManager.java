package org.kr.intp.application.manager;

import org.kr.intp.application.monitor.EREConvertor;
import org.kr.intp.application.monitor.EREMonitor;
import org.kr.intp.application.monitor.EREReader;
import org.kr.intp.application.pojo.job.EreJob;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.CloseableResource;
import org.kr.intp.util.thread.NamedThreadFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EREManager implements CloseableResource {

    private static final Map<String, EREManager> managers = new HashMap<String, EREManager>();

    public static EREManager getInstance(String projectId) {
        synchronized (managers) {
            if (managers.containsKey(projectId))
                return managers.get(projectId);
            else {
                final EREManager ereManager = new EREManager(projectId);
                managers.put(projectId, ereManager);
                return ereManager;
            }
        }
    }

    private final Logger log = LoggerFactory.getLogger(EREManager.class);

    private final String projectId;

    private final List<EREMonitor> monitors = new ArrayList<EREMonitor>();
    private final ExecutorService executor;

    private EREManager(String projectId) {
        this.projectId = projectId;
        init();
        final int threads = monitors.size() > 0 ? monitors.size() : 1;
        executor = Executors.newFixedThreadPool(threads, NamedThreadFactory.newInstance("ERE_" + projectId));
        log.debug("ERE Manager created: " + projectId);
    }

    private void init() {
        log.debug("initializing ERE Manager: " + projectId);
        try {
            final List<Map> ereProps = EREReader.getProjectEREs(projectId);
            for (Map map : ereProps) {
                final EreJob job = EREConvertor.getInstance().convert(map);
                monitors.add(new EREMonitor(job));
            }
        } catch (Exception e) {
            log.error("Error while initializing ERE Monitor", e);
        }
    }


    public void runMonitors() {
        log.debug("running ERE Monitors: " + projectId);
        for (EREMonitor monitor : monitors)
            executor.submit(monitor);
    }

    private void stopMonitors() {
        log.debug("stopping ERE Monitors: " + projectId);
        for (EREMonitor monitor : monitors)
            monitor.stop();
        executor.shutdownNow();
    }

    public void forceRT4allMonitors() {
        log.trace("forcing RT for all EREMonitors for the project: " + projectId);
        Runnable r = new Runnable() {
            @Override
            public void run() {
                for (EREMonitor m : monitors) {
                    m.runRT();
                }
            }
        };
        new Thread(r, projectId + "_RT_FORCER").start();
    }

    @Override
    public void close() {
        log.info("closing ERE Monnitors: " + projectId);
        stopMonitors();
        synchronized (managers) {
            managers.remove(projectId);
        }
    }
}
