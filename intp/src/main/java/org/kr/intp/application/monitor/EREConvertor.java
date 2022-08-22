package org.kr.intp.application.monitor;

import org.kr.intp.application.pojo.job.EreJob;

import java.util.Map;

public class EREConvertor {

    private static final EREConvertor instance = new EREConvertor();

    public static EREConvertor getInstance() { return instance; }

    private EREConvertor() {}

    public EreJob convert(Map map) {
        final boolean enabled = convertToBoolean(map.get("enabled"));
        if (!enabled)
            return null;
        final String projectId = map.get("project_id").toString();
        final String name = map.get("name").toString();
        final int stats_age_sec = convertToInteger(map.get("stats_age_sec"));
        final int con_threshold = convertToInteger(map.get("connections_threshold"));
        final Map<String, Integer> users = (Map) map.get("users");
        users.put("all_users", con_threshold);
        final String rtObject = map.get("rt_object").toString();
        final int monitorFrequency = convertToInteger(map.get("monitor_frequency_ms"));
        final int modeMinRuntime = convertToInteger(map.get("mode_min_runtime_ms"));
        final int cpu = convertToInteger(map.get("cpu_threshold"));
        final int mem = convertToInteger(map.get("mem_threshold"));
        final double avg_ratio = convertToDouble (map.get("avg_ratio_threshold"));
        final double max_ratio = convertToDouble (map.get("max_ratio_threshold"));
        return new EreJob(projectId, name, stats_age_sec, rtObject, 0l, monitorFrequency,
                modeMinRuntime, cpu, mem, avg_ratio, max_ratio, users);
    }


    private int convertToInteger(Object o) {
        return Integer.parseInt(o.toString());
    }

    private double convertToDouble(Object o) {
        return Double.parseDouble(o.toString());
    }

    private boolean convertToBoolean(Object o) {
        return Boolean.parseBoolean(o.toString());
    }
}
