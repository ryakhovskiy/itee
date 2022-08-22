package org.kr.intp.logging;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created bykron 23.11.2016.
 */
public abstract class BaseLogger extends Thread {

    protected final BlockingQueue<LogMessage> queue = new LinkedBlockingQueue<>();
    private final org.slf4j.Logger slf4jLogger = org.slf4j.LoggerFactory.getLogger(BaseLogger.class);

    public BaseLogger(boolean daemon) {
        this(daemon, "LOG");
    }

    public BaseLogger(boolean daemon, String name) {
        super(name);
        this.setDaemon(daemon);
    }

    public abstract void message(Level level, String name, String message);

    public abstract void message(Level level, String name, String format, Object arg);

    public abstract void message(Level level, String name, String format, Object arg1, Object arg2);

    public abstract void message(Level level, String name, String format, Object... arguments);

    public abstract void message(Level level, String name, String msg, Throwable t);

    protected void message(LogMessage message) {
        try {
            if (this.isInterrupted())
                return;
            queue.put(message);
        } catch (InterruptedException e) {
            slf4jLogger.debug("logging to an interrupted DbLogger: " + message);
            Thread.currentThread().interrupt();
        }
    }

}
