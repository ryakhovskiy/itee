package org.kr.intp.application.pojo.event;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

/**
 * Created by kr on 1/12/14.
 */
public class AggregationEvent extends Event {

    private final Set<String> oldUuids;

    protected AggregationEvent(String newUuid, Set<String> oldUuids) {
        super(newUuid);
        this.oldUuids = oldUuids;
    }

    public Collection<String> getOldUuids() {
        return new ArrayList<String>(oldUuids);
    }

    @Override
    public EventType getEventType() {
        return EventType.AGGREGATION;
    }
}
