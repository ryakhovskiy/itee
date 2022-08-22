package org.kr.intp.util.db.load.hana;

import org.kr.intp.application.monitor.HwResourceMonitor;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.load.DBMSLoadChecker;

/**
 * Created by kr on 31.03.2014.
 */
public class HanaDBMSLoadChecker extends DBMSLoadChecker {

    public static DBMSLoadChecker newInstance() {
        return new HanaDBMSLoadChecker();
    }

    private final Logger log = LoggerFactory.getLogger(HanaDBMSLoadChecker.class);
    private final boolean isTraceEnabled = log.isTraceEnabled();

    private HanaDBMSLoadChecker() { }

    @Override
    public boolean isLoadThresholdReached() {
        final int[] data = HwResourceMonitor.getHanaHwResourceMonitor().getCpuMemStats();
        final int cpu = data[0];
        final int mem = data[1];
        final boolean reached = cpu >= cpuThreshold || mem >= memThreshold;
        if (isTraceEnabled)
            log.trace("Threshold is reached: " + reached + "; cpu: " + cpu + "; mem: " + mem);
        return reached;
    }
}
