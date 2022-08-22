package org.kr.itee.perfmon.ws.monitor;

import java.util.List;

/**
 * Created by kr on 5/28/2014.
 */
public interface IMonitor extends Runnable {

    List<String> getResults();
    void shutdown();

}
