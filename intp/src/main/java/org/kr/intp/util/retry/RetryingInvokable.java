package org.kr.intp.util.retry;

public interface RetryingInvokable<T> {

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    T invoke() throws Exception;
}