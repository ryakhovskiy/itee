package org.kr.itee.perfmon.ws.monitor;


import org.kr.itee.perfmon.ws.conf.GlobalConfiguration;
import org.kr.itee.perfmon.ws.conf.ProcedureConfiguration;
import org.apache.log4j.Logger;

import java.sql.SQLException;

/**
 *
 */
public class CustomMonitor {

    private final Logger log = Logger.getLogger(CustomMonitor.class);
    private ProcedureConfiguration[] procedureConfigurations;
    private Thread[] threads;

    public CustomMonitor(String jdbcUrl, long intervalMS) {
        if (!GlobalConfiguration.isGlobalConfigActivated)
            return;
        this.procedureConfigurations = GlobalConfiguration.getCustomProceduresConfiguration();
        for (ProcedureConfiguration c : this.procedureConfigurations) {
            if (null == c)
                continue;
            if (null == c.getUrl())
                c.setUrl(jdbcUrl);
            if (0 == c.getIntervalMS())
                c.setIntervalMS(intervalMS);
        }
        this.threads = new Thread[this.procedureConfigurations.length];
    }

    public void start() {
        if (!GlobalConfiguration.isGlobalConfigActivated)
            return;
        for (int i = 0; i < procedureConfigurations.length; i++) {
            final ProcedureConfiguration config = procedureConfigurations[i];
            if (null == config)
                continue;
            Runnable r = new Runnable() {
                @Override
                public void run() {
                    final SPExecutor executor = new SPExecutor(config);
                    try {
                        while (!Thread.currentThread().isInterrupted()) {
                            executor.call();
                            Thread.sleep(config.getIntervalMS());
                        }
                    } catch (SQLException e) {
                        log.error(e);
                    } catch (InterruptedException e) {
                        log.info("Custom Executor is interrupted");
                    }
                }
            };
            threads[i] = new Thread(r);
            threads[i].start();
        }
    }

    public void stop() {
        if (!GlobalConfiguration.isGlobalConfigActivated)
            return;
        if (null == threads)
            return;
        for (Thread t : threads) {
            if (null != t)
                t.interrupt();
        }
    }
}

