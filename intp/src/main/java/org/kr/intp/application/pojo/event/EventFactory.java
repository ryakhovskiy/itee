package org.kr.intp.application.pojo.event;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 28.10.13
 * Time: 13:11
 * To change this template use File | Settings | File Templates.
 */
public class EventFactory {

    public static EventFactory newInstance() {
        return new EventFactory();
    }

    private EventFactory() {}

    public Event newTriggeredEvent(String uuid, long time, long firstKeyTriggeredTime) {
        return new TriggeredEvent(uuid, time, firstKeyTriggeredTime);
    }

    public Event newTriggeredEvent(String uuid, long time) {
        return new TriggeredEvent(uuid, time);
    }

    public Event newWaitingEvent(String uuid) {
        return new WaitingEvent(uuid);
    }

    public Event newStartedEvent(String uuid, long time) {
        return new StartedEvent(uuid, time);
    }

    public Event newProcessedEvent(String uuid, long started, long finished, Object[] keys) {
        return new ProcessedEvent(uuid, started, finished, keys);
    }

    public Event newProcessingEvent(String uuid, long started, Object[] keys) {
        return new ProcessingEvent(uuid, started, keys);
    }

    public Event newFinishedEvent(String uuid, long time) {
        return new FinishedEvent(uuid, time);
    }

    public Event newErrorEvent(String uuid, Throwable e) {
        return new ErrorEvent(uuid, e);
    }

    public Event newClearEvent(String uuid) {
        return new ClearEvent(uuid);
    }
}
