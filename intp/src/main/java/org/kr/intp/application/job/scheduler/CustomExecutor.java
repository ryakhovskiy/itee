package org.kr.intp.application.job.scheduler;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * Created by kr on 4/10/2015.
 */
public interface CustomExecutor {

    public Future schedule(Runnable task, long delay, long period);
    public Future schedule(Runnable task, long delay);
    public Future submit(Runnable task);
    public Future submit(Callable task);
    public void shutdown();
    public List<Runnable> shutdownNow();

}
