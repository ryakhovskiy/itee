package org.kr.intp.application.pojo.event;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 28.10.13
 * Time: 12:01
 * To change this template use File | Settings | File Templates.
 */
public abstract class Event {

    private String uuid;

    protected Event(String uuid) {
        this.uuid = uuid;
    }

    public String getUuid() {
        return uuid;
    }

    public abstract EventType getEventType();

}
