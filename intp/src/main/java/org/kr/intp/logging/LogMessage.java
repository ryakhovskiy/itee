package org.kr.intp.logging;

import java.sql.Timestamp;

/**
 * Created bykron 07.12.2016.
 */
public class LogMessage {

    protected final Level level;
    protected final String message;
    protected final String thread;
    protected final String name;
    protected final long timestamp = System.currentTimeMillis();
    protected final Throwable throwable;

    protected LogMessage(Level level, String name, String message, String thread, Throwable throwable) {
        this.level = level;
        this.name = name;
        this.message = message;
        this.thread = thread;
        this.throwable = throwable;
    }

    @Override
    public String toString() {
        return String.format("%s %s %s %s - %s",
                name, thread, new Timestamp(timestamp).toString(), level.toString(), message);
    }

}
