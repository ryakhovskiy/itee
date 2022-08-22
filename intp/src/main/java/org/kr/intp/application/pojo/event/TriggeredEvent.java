package org.kr.intp.application.pojo.event;

import java.sql.Timestamp;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 28.10.13
 * Time: 11:59
 * To change this template use File | Settings | File Templates.
 */
public class TriggeredEvent extends Event {

    private long time;
    private long firstKeyTriggeredTime;

    protected TriggeredEvent(String uuid, long time) {
        this(uuid, time, 0);
    }

    protected TriggeredEvent(String uuid, long time, long firstKeyTriggeredTime) {
        super(uuid);
        this.time = time;
        this.firstKeyTriggeredTime = firstKeyTriggeredTime;
    }

    public Timestamp getTriggeredTime() {
        return new Timestamp(time);
    }

    public Timestamp getFirstKeyTriggeredTime() { return new Timestamp(firstKeyTriggeredTime); }

    public EventType getEventType() {
        return EventType.TRIGGERED;
    }

}
