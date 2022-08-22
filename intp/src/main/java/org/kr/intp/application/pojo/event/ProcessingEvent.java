package org.kr.intp.application.pojo.event;

import java.sql.Timestamp;
import java.util.Arrays;

/**
 * Created bykron 21.08.2014.
 */
public class ProcessingEvent extends Event {

    private final long started;
    private final Object[] keys;

    protected ProcessingEvent(String uuid, long started, Object[] keys) {
        super(uuid);
        this.started = started;
        this.keys = keys;
    }

    public Timestamp getStartedTime() {
        return new Timestamp(started);
    }

    public Object[] getKeys() {
        synchronized (keys) {
            return Arrays.copyOf(keys, keys.length);
        }
    }

    @Override
    public EventType getEventType() {
        return EventType.PROCESSING;
    }
}
