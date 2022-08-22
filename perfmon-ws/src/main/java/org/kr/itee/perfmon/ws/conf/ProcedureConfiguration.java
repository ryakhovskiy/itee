package org.kr.itee.perfmon.ws.conf;

/**
 *
 */
public class ProcedureConfiguration {

    private final String name;
    private final String schema;
    private long intervalMS;
    private String url;

    public ProcedureConfiguration(String name, String schema) {
        this.name = name;
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public String getSchema() {
        return schema;
    }

    public long getIntervalMS() {
        return intervalMS;
    }

    public String getUrl() {
        return url;
    }

    public void setIntervalMS(long intervalMS) {
        this.intervalMS = intervalMS;
    }

    public void setUrl(String url) {
        this.url = url;
    }

}
