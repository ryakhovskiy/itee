package org.kr.intp.util.thread;

import org.kr.intp.logging.Level;
import org.kr.intp.logging.Logger;

/**
 *
 */
public class ThreadUtils {

    public static void logStackTrace(Logger log, Level level) {
        if (level.equals(Level.TRACE))
            log.trace("", new Throwable());
        if (level.equals(Level.DEBUG))
            log.debug("", new Throwable());
        if (level.equals(Level.INFO))
            log.info("", new Throwable());
        if (level.equals(Level.WARN))
            log.warn("", new Throwable());
        if (level.equals(Level.ERROR))
            log.error("", new Throwable());
    }
}
