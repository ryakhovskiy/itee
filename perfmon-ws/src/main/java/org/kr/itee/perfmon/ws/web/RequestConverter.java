package org.kr.itee.perfmon.ws.web;

import org.kr.itee.perfmon.ws.pojo.*;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class RequestConverter {

    private final Logger log = Logger.getLogger(RequestConverter.class);
    private final ObjectMapper mapper = new ObjectMapper();

    public RequestPOJO convertRequest(String json) {
        final Map<String, Object> requestMap = getRequestAsMap(json);
        if (!requestMap.containsKey("autorun") || !requestMap.containsKey("load"))
            throw new IllegalArgumentException("request does not contain load / autorun information");
        final Map<String, Object> autorun = getAutorunMap(requestMap);
        final Map<String, Object> load = getLoadMap(requestMap);

        final AutorunPOJO itAutorun = readAutorunSpecification("itar", autorun);
        final AutorunPOJO rtAutorun = readAutorunSpecification("rtar", autorun);

        final LoadPOJO itQueryLoad = readLoad("it", "query", load);
        final LoadPOJO itUpdateLoad = readLoad("it", "update", load);

        final LoadPOJO rtQueryLoad = readLoad("rt", "query", load);
        final LoadPOJO rtUpdateLoad = readLoad("rt", "update", load);

        final MonitorPOJO itMonitor = readMonitorSpecification("it", load);
        final MonitorPOJO rtMonitor = readMonitorSpecification("rt", load);

        return new RequestPOJO(itAutorun, rtAutorun, itQueryLoad, itUpdateLoad, rtQueryLoad, rtUpdateLoad,
                itMonitor, rtMonitor);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getRequestAsMap(String json) {
        try {
            return mapper.readValue(json, Map.class);
        } catch (IOException e) {
            log.error("Cannot convert request to json", e);
            return Collections.emptyMap();
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getAutorunMap(Map<String, Object> requestMap) {
        final Map<String, Object> autorun = (Map) requestMap.get("autorun");
        if (!autorun.containsKey("itar") || !autorun.containsKey("rtar"))
            throw new IllegalArgumentException("request does not contain autorun specifications (neither RT nor IT)");
        return autorun;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getLoadMap(Map<String, Object> requestMap) {
        final Map<String, Object> load = (Map) requestMap.get("load");
        if (!load.containsKey("it") || !load.containsKey("rt"))
            throw new IllegalArgumentException("request does not contain load specifications (neither RT nor IT)");
        return load;
    }


    @SuppressWarnings("unchecked")
    private AutorunPOJO readAutorunSpecification(String id, Map<String, Object> autorun) {
        if (!autorun.containsKey(id)) {
            log.warn("no autorun defined: " + id);
            return null;
        }
        final Map<String, Object> specMap = (Map) autorun.get(id);
        try {
            long queryProcessAdd = Long.parseLong(specMap.get("qpa").toString());
            int queryProcessLimit = Integer.parseInt(specMap.get("qpl").toString());
            int queryProcessBatchSize = Integer.parseInt(specMap.get("qpb").toString());
            long updateProcessAdd = Long.parseLong(specMap.get("upa").toString());
            int updateProcessLimit = Integer.parseInt(specMap.get("upl").toString());
            int updateProcessBatchSize = Integer.parseInt(specMap.get("upb").toString());
            long timeoutMS;
            timeoutMS = specMap.containsKey("tms") ? Long.parseLong(specMap.get("tms").toString()) : 0L;
            return new AutorunPOJO(queryProcessAdd, queryProcessLimit, queryProcessBatchSize,
                    updateProcessAdd, updateProcessLimit, updateProcessBatchSize, timeoutMS);
        } catch (Exception e) {
            log.error("Error while reading parsing request, autorun specification: " + id + "; " + e.getMessage() +
                    "\nspecification map:" + specMap, e);
            throw new IllegalArgumentException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private LoadPOJO readLoad(String key, String type, Map<String, Object> data) {
        if (!data.containsKey(key)) {
            log.warn("no load defined for " + key);
            return null;
        }
        Map<String, Object> itLoad = (Map) data.get(key);
        return readLoadSpecification(type, itLoad);
    }

    @SuppressWarnings("unchecked")
    private LoadPOJO readLoadSpecification(String id, Map<String, Object> load) {
        if (!load.containsKey(id)) {
            log.warn("no load defined: " + id);
            return null;
        }
        log.debug("parsing load: " + id);
        final Map<String, Object> specMap = (Map) load.get(id);
        final JdbcPOJO jdbcPOJO = readJdbcSpecification(specMap);
        final Map<String, Object> generalMap = (Map) specMap.get("gen");
        try {
            long schedulerIntervalMS = Long.parseLong(generalMap.get("si").toString());
            int queriesPerInterval = Integer.parseInt(generalMap.get("qpi").toString());
            int concurrentExecutors = Integer.parseInt(generalMap.get("ce").toString());
            String queryType = generalMap.get("qt").toString();
            String file = generalMap.get("fn").toString();
            Object oFileEncoding = generalMap.get("fe");
            String fileEncoding = oFileEncoding != null ? oFileEncoding.toString() : "UTF8";
            int executionTimeSeconds = Integer.parseInt(generalMap.get("et").toString());
            boolean roundRobin = generalMap.get("rr").toString().equalsIgnoreCase("true");
            return new LoadPOJO(jdbcPOJO, schedulerIntervalMS, queriesPerInterval, concurrentExecutors, queryType,
                    file, fileEncoding, executionTimeSeconds, roundRobin);
        } catch (Exception e) {
            log.error("Error while reading general values: " + load + "; " + e.getMessage(), e);
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private JdbcPOJO readJdbcSpecification(Map<String, Object> specMap) {
        if (!specMap.containsKey("jdbc")) {
            log.warn("no jdbc defined");
            return null;
        }
        log.debug("reading jdbc...");
        final Map<String, Object> jdbcMap = (Map) specMap.get("jdbc");
        try {
            String driver = jdbcMap.get("driver").toString();
            final String host = jdbcMap.get("host").toString();
            int port = Integer.parseInt(jdbcMap.get("port").toString());
            final String user = jdbcMap.get("user").toString();
            final String password = jdbcMap.get("password").toString();
            return new JdbcPOJO(driver, host, port, user, password);
        } catch (Exception e) {
            log.error("Error while reading parsing request, jdbc specification: " + e.getMessage() +
                    "\nspecification map:" + specMap, e);
            throw new IllegalArgumentException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private MonitorPOJO readMonitorSpecification(String key, Map<String, Object> specMap) {
        if (!specMap.containsKey(key)) {
            log.warn("no load for key " + key);
            return null;
        }
        specMap = (Map) specMap.get(key);
        if (!specMap.containsKey("mon")) {
            log.warn("no monitor defined");
            return null;
        }
        log.debug("reading monitor...");
        Map<String, Object> monitorMap = (Map) specMap.get("mon");
        try {
            JdbcPOJO jdbcPOJO = readJdbcSpecification(monitorMap);
            Map<String, Object> generalMap = (Map) monitorMap.get("gen");
            String file = generalMap.get("ofn").toString();
            int monitorAge = Integer.parseInt(generalMap.get("mva").toString());
            int topx = Integer.parseInt(generalMap.get("txv").toString());
            long queryIntervalMS = Long.parseLong(generalMap.get("mqi").toString());
            long expensiveStatementsDuration = Long.parseLong(generalMap.get("esd").toString());
            long powerConsumptionDuration = Long.parseLong(generalMap.get("pcd").toString());
            int powerConsumptionMonitorPort = Integer.parseInt(generalMap.get("pcp").toString());
            return new MonitorPOJO(jdbcPOJO, file,  monitorAge, topx, queryIntervalMS, expensiveStatementsDuration,
                    powerConsumptionDuration, powerConsumptionMonitorPort);
        } catch (Exception e) {
            log.error("error while parsing monitor data: " + specMap + "; " + e.getMessage(), e);
            return null;
        }
    }
}
