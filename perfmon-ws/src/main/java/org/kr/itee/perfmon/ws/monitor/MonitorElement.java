package org.kr.itee.perfmon.ws.monitor;

/**
 * Created by kr on 5/17/2014.
 */
public class MonitorElement {

    private final String component;
    private final String category;
    private final int depth;

    public MonitorElement(String component, String category, int depth) {
        this.component = component;
        this.category = category;
        this.depth = depth;
    }

    public String getComponent() { return component; }

    public String getCategory() {
        return category;
    }

    public int getDepth() {
        return depth;
    }

    @Override
    public int hashCode() {
        return 11 * component.hashCode() +
                13 * category.hashCode() +
                17 * depth;
    }

    @Override
    public boolean equals(Object other) {
        if (null == other)
            return false;
        if (this == other)
            return true;
        if (!(other instanceof MonitorElement))
            return false;
        final MonitorElement e = (MonitorElement)other;
        return  (e.category.equals(category) && e.component.equals(component) && e.depth == depth);
    }
}
