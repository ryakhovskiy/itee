package org.kr.intp.application.pojo.event;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 17.09.13
 * Time: 22:57
 * To change this template use File | Settings | File Templates.
 */
public enum  EventType {

    TRIGGERED('T'),
    STARTED('S'),
    WAITING('W'),
    PROCESSING('B'),
    PROCESSED('P'),
    FINISHED('F'),
    ERROR('E'),
    CLEAR('C'),
    AGGREGATION('A');

    private final char type;
    private final int id;

    private EventType(char type) {
        this.type = type;
        this.id = EventStatus.getInstance().getStatusInfo(type).getId();
    }

    public char getType() { return type; }

    public int getId() { return  id; }

    public String toString() {
        return String.format("%d: %c", id, type);
    }
}
