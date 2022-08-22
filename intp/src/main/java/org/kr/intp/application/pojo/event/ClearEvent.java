package org.kr.intp.application.pojo.event;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 28.10.13
 * Time: 13:09
 * To change this template use File | Settings | File Templates.
 */
public class ClearEvent extends Event {

    protected ClearEvent(String uuid) {
        super(uuid);
    }

    public EventType getEventType() {
        return EventType.CLEAR;
    }

}
