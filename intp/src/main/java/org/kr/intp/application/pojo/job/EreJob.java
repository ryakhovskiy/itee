package org.kr.intp.application.pojo.job;

import java.util.HashMap;
import java.util.Map;

/**
 * Created bykron 27.10.2014.
 */
public class EreJob {

    private final String projectId;
    private final String name;
    private final int stats_age_sec;
    private final String rt_object;
    private final long sla_millis;
    private final long monitor_frequency_ms;
    private final long mode_min_runtime_ms;
    private final int cpu_threshold;
    private final int mem_threshold;
    private final Map<String, Integer> connectionThresholds;
    private final double avg_ratio_threshold;
    private final double max_ratio_threshold;


    public EreJob(String projectId, String name, int stats_age_sec, String rt_object, long sla_millis,
                  long monitor_frequency_ms, long mode_min_runtime_ms, int cpu_threshold, int mem_threshold,
                  double avg_ratio_threshold, double max_ratio_threshold, Map<String, Integer> connectionThresholds) {
        this.projectId = projectId;
        this.name = name;
        this.stats_age_sec = stats_age_sec;
        this.rt_object = rt_object;
        this.sla_millis = sla_millis;
        this.monitor_frequency_ms = monitor_frequency_ms;
        this.mode_min_runtime_ms = mode_min_runtime_ms;
        this.cpu_threshold = cpu_threshold;
        this.mem_threshold = mem_threshold;
        this.connectionThresholds = connectionThresholds;
        this.avg_ratio_threshold = avg_ratio_threshold;
        this.max_ratio_threshold = max_ratio_threshold;
    }

    public String getProjectId() {
        return projectId;
    }

    public String getName() {
        return name;
    }

    public int getStats_age_sec() {
        return stats_age_sec;
    }

    public String getRt_object() {
        return rt_object;
    }

    public long getSla_millis() {
        return sla_millis;
    }

    public long getMonitor_frequency_ms() {
        return monitor_frequency_ms;
    }

    public long getMode_min_runtime_ms() {
        return mode_min_runtime_ms;
    }

    public int getCpu_threshold() {
        return cpu_threshold;
    }

    public int getMem_threshold() {
        return mem_threshold;
    }

    public Map<String, Integer> getConnectionThresholds() {
        return new HashMap<String, Integer>(connectionThresholds);
    }

    public double getAvg_ratio_threshold() {
        return avg_ratio_threshold;
    }

    public double getMax_ratio_threshold() {
        return max_ratio_threshold;
    }

    @Override
    public String toString() {
        return String.format("");
    }
}
