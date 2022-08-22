package org.kr.intp.application.pojo.event;

import java.sql.Timestamp;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 28.10.13
 * Time: 12:59
 * To change this template use File | Settings | File Templates.
 */
public class StartedEvent extends Event {

    private final long time;

    protected StartedEvent(String uuid, long time) {
        super(uuid);
        this.time = time;
    }

    public Timestamp getStartedTime() {
        return new Timestamp(time);
    }

    public EventType getEventType() {
        return EventType.STARTED;
    }

}
