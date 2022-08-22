package org.kr.intp.util.retry;

public interface RetryingInvoker<T> {

    /**
     * Retries if invocation fails.
     *
     * @param invokeTimeoutMS Timeout for this call
     * @param invokable    The {@link RetryingInvokable} to run.
     * @return an object of type T
     */
    T invokeWithRetries(final RetryingInvokable<T> invokable, final long invokeTimeoutMS) throws Exception;

    boolean isErrorRecoverable(Throwable e);

}
