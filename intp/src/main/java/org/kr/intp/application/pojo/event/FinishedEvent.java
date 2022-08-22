package org.kr.intp.application.pojo.event;

import java.sql.Timestamp;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 28.10.13
 * Time: 13:04
 * To change this template use File | Settings | File Templates.
 */
public class FinishedEvent extends Event {

    private final long time;

    protected FinishedEvent(String uuid, long time) {
        super(uuid);
        this.time = time;
    }

    public Timestamp getFinishedTime() {
        return new Timestamp(time);
    }

    public EventType getEventType() {
        return EventType.FINISHED;
    }

}
