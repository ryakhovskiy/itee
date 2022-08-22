package org.kr.intp.util.db.load;

import org.kr.intp.application.AppContext;
import org.kr.intp.util.db.load.hana.HanaDBMSLoadChecker;

/**
 * Created by kr on 31.03.2014.
 */
public abstract class DBMSLoadChecker {

    protected static final int cpuThreshold = AppContext.instance().getConfiguration().getDbmsLoadCpuThreshold();
    protected static final int memThreshold = AppContext.instance().getConfiguration().getDbmsLoadMemoryThreshold();

    public static DBMSLoadChecker newHanaDBMSLoadChecker() {
        return HanaDBMSLoadChecker.newInstance();
    }

    public abstract boolean isLoadThresholdReached();

}
