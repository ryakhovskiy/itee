package org.kr.itee.perfmon.ws.pojo;

/**
 *
 */
public class RequestPOJO {

    private final AutorunPOJO itAutorunSpecification;
    private final AutorunPOJO rtAutorunSpecification;
    private final LoadPOJO itQueryLoadSpecification;
    private final LoadPOJO itUpdateLoadUpdateSpecification;
    private final MonitorPOJO itMonitorSpecification;
    private final LoadPOJO rtQueryLoadSpecification;
    private final LoadPOJO rtUpdateLoadUpdateSpecification;
    private final MonitorPOJO rtMonitorSpecification;

    public RequestPOJO(AutorunPOJO itAutorunSpecification, AutorunPOJO rtAutorunSpecification,
                       LoadPOJO itQueryLoadSpecification, LoadPOJO itUpdateLoadUpdateSpecification,
                       LoadPOJO rtQueryLoadSpecification, LoadPOJO rtUpdateLoadUpdateSpecification,
                       MonitorPOJO itMonitorSpecification, MonitorPOJO rtMonitorSpecification) {
        this.itAutorunSpecification = itAutorunSpecification;
        this.rtAutorunSpecification = rtAutorunSpecification;
        this.itQueryLoadSpecification = itQueryLoadSpecification;
        this.itUpdateLoadUpdateSpecification = itUpdateLoadUpdateSpecification;
        this.rtQueryLoadSpecification = rtQueryLoadSpecification;
        this.rtUpdateLoadUpdateSpecification = rtUpdateLoadUpdateSpecification;
        this.itMonitorSpecification = itMonitorSpecification;
        this.rtMonitorSpecification = rtMonitorSpecification;
    }

    public AutorunPOJO getItAutorunSpecification() {
        return itAutorunSpecification;
    }

    public AutorunPOJO getRtAutorunSpecification() {
        return rtAutorunSpecification;
    }

    public LoadPOJO getItQueryLoadSpecification() {
        return itQueryLoadSpecification;
    }

    public LoadPOJO getItUpdateLoadUpdateSpecification() {
        return itUpdateLoadUpdateSpecification;
    }

    public LoadPOJO getRtQueryLoadSpecification() {
        return rtQueryLoadSpecification;
    }

    public LoadPOJO getRtUpdateLoadSpecification() {
        return rtUpdateLoadUpdateSpecification;
    }

    public MonitorPOJO getItMonitorSpecification() {
        return itMonitorSpecification;
    }

    public MonitorPOJO getRtMonitorSpecification() {
        return rtMonitorSpecification;
    }
}
