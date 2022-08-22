package org.kr.intp.application.job.scheduler;

import org.kr.intp.application.pojo.job.JobProperties;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.thread.NamedThreadFactory;

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by kr on 4/10/2015.
 */
public class PeriodicalExecutor implements CustomExecutor {

    private final Logger log = LoggerFactory.getLogger(PeriodicalExecutor.class);
    protected final ScheduledExecutorService executor;
    private final JobProperties.PeriodChoice periodChoice;

    public PeriodicalExecutor(String thread_prefix, int threads, JobProperties.PeriodChoice periodChoice) {
        this.executor = Executors.newScheduledThreadPool(threads, NamedThreadFactory.newInstance(thread_prefix));
        this.periodChoice = periodChoice;
    }

    public Future schedule(Runnable task, long delayMilliseconds, long periodInMilliseconds) {

        long periodSeconds = TimeUnit.MILLISECONDS.toSeconds(periodInMilliseconds);
        long delaySeconds = TimeUnit.MILLISECONDS.toSeconds(delayMilliseconds);

        switch (periodChoice) {
            case SECONDS:
                log.trace("scheduling task, delay: " + delaySeconds + "; period: " + periodSeconds + "; UoM: " + periodChoice);
                break;
            case HOUR:
                log.trace("scheduling task, period: " + TimeUnit.SECONDS.toHours(periodSeconds) + "; UoM: " + periodChoice);
                log.trace("scheduling task, delay: " + delaySeconds + "; period: " + periodSeconds + "; UoM: SECONDS");
                break;
            case DAY:
                log.trace("scheduling task, period: " + TimeUnit.DAYS.toHours(periodSeconds) + "; UoM: " + periodChoice);
                log.trace("scheduling task, delay: " + delaySeconds + "; period: " + periodSeconds + "; UoM: SECONDS");
                break;
            case WEEK:
                log.trace("scheduling task, period: " + TimeUnit.DAYS.toHours(periodSeconds)/7 + "; UoM: " + periodChoice);
                log.trace("scheduling task, delay: " + delaySeconds + "; period: " + periodSeconds + "; UoM: SECONDS");
                break;
            case MONTH:
                log.trace("scheduling task, period: " + TimeUnit.DAYS.toHours(periodSeconds)/30 + "; UoM: " + periodChoice);
                log.trace("scheduling task, delay: " + delaySeconds + "; period: " + periodSeconds + "; UoM: SECONDS");
                break;
            case YEAR:
                log.trace("scheduling task, period: " + TimeUnit.DAYS.toHours(periodSeconds)/365 + "; UoM: " + periodChoice);
                log.trace("scheduling task, delay: " + delaySeconds + "; period: " + periodSeconds + "; UoM: SECONDS");
                break;
            default:
                log.warn("wrong PeriodChoice");
                log.trace("scheduling task, delay: " + delaySeconds + "; period: " + periodSeconds + "; UoM: " + periodChoice);
        }
        return executor.scheduleAtFixedRate(task, delaySeconds, periodSeconds, TimeUnit.SECONDS);
    }

    public Future schedule(Runnable task, long delay) {
        return executor.schedule(task, delay, TimeUnit.MILLISECONDS);
    }

    public Future submit(Callable task) {
        return executor.submit(task);
    }

    public Future submit(Runnable task) {
        return executor.submit(task);
    }

    public void shutdown() {
        executor.shutdown();
    }

    public List<Runnable> shutdownNow() {
        return executor.shutdownNow();
    }



}
