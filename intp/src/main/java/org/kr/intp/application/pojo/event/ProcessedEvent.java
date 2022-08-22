package org.kr.intp.application.pojo.event;

import java.sql.Timestamp;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 28.10.13
 * Time: 13:01
 * To change this template use File | Settings | File Templates.
 */
public class ProcessedEvent extends Event {

    private final long started;
    private final long finished;
    private final Object[] keys;

    protected ProcessedEvent(String uuid, long started, long finished, Object[] keys) {
        super(uuid);
        this.started = started;
        this.finished = finished;
        this.keys = keys;
    }

    public Timestamp getStartedTime() {
        return new Timestamp(started);
    }

    public Timestamp getFinishedTime() {
        return new Timestamp(finished);
    }

    public Object[] getKeys() {
        synchronized (keys) {
            return Arrays.copyOf(keys, keys.length);
        }
    }

    public EventType getEventType() {
        return EventType.PROCESSED;
    }
}
