package org.kr.intp.application.agent;

import java.util.Map;

/**
 *
 */
public interface IResourceMonitor {

    Map<String, Integer> getActiveConnectionsStats();
    int[] getCpuMemStats();

}
