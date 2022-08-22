package org.kr.db.loader.ui.monitor;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by kr on 5/28/2014.
 */
public interface IMonitor extends Runnable {

    List<String> getResults();
    void shutdown();

}
