package org.kr.intp.application.pojo.event;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 28.10.13
 * Time: 13:06
 * To change this template use File | Settings | File Templates.
 */
public class ErrorEvent extends Event {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private final Throwable exception;

    protected ErrorEvent(String uuid, Throwable e) {
        super(uuid);
        this.exception = e;
    }

    public String getError() {
        if (null == exception)
            return "---";
        Throwable e = exception;
        StringBuilder builder = new StringBuilder();
        do {
            builder.append(e.getMessage());
            builder.append(LINE_SEPARATOR);
        } while (null != (e = e.getCause()));
        return builder.toString();
    }

    public EventType getEventType() {
        return EventType.ERROR;
    }

}
