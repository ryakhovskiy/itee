package org.kr.intp.application.pojo.event;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 28.10.13
 * Time: 12:58
 * To change this template use File | Settings | File Templates.
 */
public class WaitingEvent extends Event {

    protected WaitingEvent(String uuid) {
        super(uuid);
    }

    public EventType getEventType() {
        return EventType.WAITING;
    }

}
