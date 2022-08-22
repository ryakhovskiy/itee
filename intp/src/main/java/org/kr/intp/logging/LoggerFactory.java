package org.kr.intp.logging;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created bykron 23.11.2016.
 */
public class LoggerFactory {

    private static final ConcurrentMap<String, Logger> LOGGERS = new ConcurrentHashMap<>();

    public static Logger getLogger(Class<?> klass) {
        final String name = klass.getCanonicalName();
        return getLogger(name);
    }

    public static Logger getLogger(String name) {
        Logger logger = LOGGERS.get(name);
        if (null != logger)
            return logger;

        //does not exists
        logger = new Logger(name);
        //if another thread has created and put the logger earlier than current thread
        // then the earliest is returned
        final Logger earliest = LOGGERS.putIfAbsent(name, logger);
        if (null != earliest)
            return earliest;
        else
            return logger;
    }

}
