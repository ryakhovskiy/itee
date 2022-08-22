package org.kr.intp.util.retry;

import org.kr.intp.application.AppContext;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.thread.NamedThreadFactory;

import java.util.concurrent.*;

/**
 * The class that helps to invoke a method with retries.
 */
public abstract class RetryingInvokerBase<T> implements RetryingInvoker<T>, AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(RetryingInvokerBase.class);
    private final boolean isTraceEnabled = log.isTraceEnabled();
    private final String name;
    private final ExecutorService executor;
    private final long initialPauseTimeMS;
    private final long callTimeoutMS;
    private final boolean isPauseTimeIncreasing;
    private long pauseTime;

    /**
     *
     * @param pauseTimeMS a pause time in milliseconds between retries
     * @param callTimeoutMS a maximum wait time for each invocation, <br>
     *                      if the invocation did not return a value within this period of time,
     *                      the attempt is considered as failed and invocation is interrupted.<br>
     *                      If this parameter is set to 0, means, that there is no time limit for invocation
     *
     */
    public RetryingInvokerBase(long pauseTimeMS, long callTimeoutMS) {
        this(null, pauseTimeMS, callTimeoutMS, "");
    }

    /**
     *
     * @param pauseTimeMS a pause time in milliseconds between retries
     * @param callTimeoutMS a maximum wait time for each invocation, <br>
     *                      if the invocation did not return a value within this period of time,
     *                      the attempt is considered as failed and invocation is interrupted.<br>
     *                      If this parameter is set to 0, means, that there is no time limit for invocation
     * @param name the name of the Invoker, helps to debug
     */
    public RetryingInvokerBase(long pauseTimeMS, long callTimeoutMS, String name) {
        this(null, pauseTimeMS, callTimeoutMS, name);
    }

    /**
     *
     * @param executor an executor which is used to invoke a method
     * @param pauseTimeMS a pause time in milliseconds between retries
     * @param callTimeoutMS a maximum wait time for each invocation, <br>
     *                      if the invocation did not return a value within this period of time,
     *                      the attempt is considered as failed and invocation is interrupted.<br>
     *                      If this parameter is set to 0, means, that there is no time limit for invocation
     */
    public RetryingInvokerBase(final ExecutorService executor, final long pauseTimeMS, final long callTimeoutMS) {
        this(executor, pauseTimeMS, callTimeoutMS, "");
    }

    /**
     *
     * @param executor an executor which is used to invoke a method
     * @param pauseTimeMS a pause time in milliseconds between retries
     * @param callTimeoutMS a maximum wait time for each invocation, <br>
     *                      if the invocation did not return a value within this period of time,
     *                      the attempt is considered as failed and invocation is interrupted.<br>
     *                      If this parameter is set to 0, means, that there is no time limit for invocation
     * @param name the name of the Invoker, helps to debug
     */
    public RetryingInvokerBase(final ExecutorService executor, final long pauseTimeMS, final long callTimeoutMS, String name) {
        this.executor = executor == null ?
                Executors.newSingleThreadExecutor(NamedThreadFactory.newInstance("RI")) : executor;
        this.initialPauseTimeMS = pauseTimeMS <= 0 ? 1 : pauseTimeMS;
        this.callTimeoutMS = callTimeoutMS;
        this.name = name;
        this.isPauseTimeIncreasing = AppContext.instance().getConfiguration().isDbHaPauseTimeIncreasing()
                || this.initialPauseTimeMS == 1;
    }

    /**
     * invokes a method of the {@link RetryingInvokable} with the specified maximum timeout.<br>
     * @param invokable    The {@link RetryingInvokable} to run.
     * @param invokeTimeoutMS Timeout for this call, if after certain amount of retries the time exceeds this timeout,
     *                        the invocation throws an exception
     */
    @Override
    public T invokeWithRetries(final RetryingInvokable<T> invokable, final long invokeTimeoutMS) {
        final Callable<T> callable = new Callable<T>() { //scalarization?
            @Override
            public T call() throws Exception {
                return invokable.invoke();
            }
        };
        final long start = System.currentTimeMillis();
        for (int retry = 0; ; retry++) {
            final Future<T> future = executor.submit(callable);
            try {
                if (callTimeoutMS > 0) {
                    if (isTraceEnabled)
                        log.trace(name + ": trying to invoke; timeout (ms): " + callTimeoutMS);
                    return future.get(callTimeoutMS, TimeUnit.MILLISECONDS);
                }
                else if (invokeTimeoutMS > 0) {
                    if (isTraceEnabled)
                        log.trace(name + ": trying to invoke; timeout (ms): " + invokeTimeoutMS);
                    return future.get(invokeTimeoutMS, TimeUnit.MILLISECONDS);
                }
                else {
                    if (isTraceEnabled)
                        log.trace(name + ": trying to invoke without timeout");
                    return future.get();
                }
            } catch (InterruptedException e) {
                log.warn("interrupted");
                return handleInterruption(e);
            } catch (Exception e) {
                if (e instanceof ExecutionException && e.getCause() instanceof Exception) {
                    e = (Exception) e.getCause();
                }
                final long duration = System.currentTimeMillis() - start;
                log.error(name + ": invocation failed, attempt #" +
                        retry + ", total duration (ms): " +
                        duration + " of " + invokeTimeoutMS + "; error: " + e.getMessage(), e);
                if (invokeTimeoutMS != 0 && invokeTimeoutMS < duration)
                    throw new RuntimeException("Invocation failed, aborted after try #" + retry, e);
                if (!isErrorRecoverable(e))
                    throw new RuntimeException("Invocation failed, attempt #" + retry + "; " + e.getMessage(), e);
                final boolean interrupted = sleep();
                if (interrupted)
                    return handleInterruption(e);
            }
        }
    }

    protected long getPauseTimeMS() {
        if (0 == pauseTime)
            pauseTime = initialPauseTimeMS;
        if (isPauseTimeIncreasing) {
            pauseTime = pauseTime << 1;
            if (pauseTime >= 1 << 16)
                pauseTime = initialPauseTimeMS;
        } 
        if (isTraceEnabled)
            log.trace(name + ": sleeping before retry (ms):" + pauseTime);
        return pauseTime;
    }

    private T handleInterruption(Throwable e) {
        throw new RuntimeException("Invocation interrupted", e);
        //TODO: or interrupted exception?
        //TODO: or return null?
    }

    private boolean sleep() {
        try {
            long pauseTime = getPauseTimeMS();
            Thread.sleep(pauseTime);
            return false;
        } catch (InterruptedException e) {
            return true;
        }
    }

    @Override
    public void close() throws Exception {
        executor.shutdownNow();
    }
}
