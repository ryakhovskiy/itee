package org.kr.intp.util.retry;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class RetryingInvokerBaseTest {

    @Test
    public void testBase() throws Exception {
        RetryingInvokable<String> invokable = new RetryingInvokable<String> () {
            @Override
            public String invoke() throws Exception {
                return "success";
            }
        };
        RetryingInvokerBase<String> invokerBase = new RetryingInvokerBase<String>(1050L, 3000L) {
            @Override
            public boolean isErrorRecoverable(Throwable e) {
                return true;
            }
        };
        String res = invokerBase.invokeWithRetries(invokable, 30_000L);

        assertEquals(res, "success");
    }

    @Test
    public void test20Attempts() {
        final AtomicInteger counter = new AtomicInteger(0);
        RetryingInvokable<String> invokable = new RetryingInvokable<String> () {
            @Override
            public String invoke() throws Exception {
                if (counter.incrementAndGet() != 20)
                    throw new Exception("failed");
                return "success";
            }
        };
        RetryingInvokerBase<String> invokerBase = new RetryingInvokerBase<String>(1L, 3000L, "test") {
            @Override
            public boolean isErrorRecoverable(Throwable e) {
                return true;
            }
        };
        String res = invokerBase.invokeWithRetries(invokable, 0);

        assertTrue(counter.get() == 20);
        assertEquals(res, "success");
    }

    @Test
    public void testZeros() {
        final AtomicInteger counter = new AtomicInteger(0);
        RetryingInvokable<String> invokable = new RetryingInvokable<String> () {
            @Override
            public String invoke() throws Exception {
                if (counter.incrementAndGet() != 10)
                    throw new Exception("failed");
                return "success";
            }
        };
        RetryingInvokerBase<String> invokerBase = new RetryingInvokerBase<String>(0, 0, "test") {
            @Override
            public boolean isErrorRecoverable(Throwable e) {
                return true;
            }
        };
        String res = invokerBase.invokeWithRetries(invokable, 50000L);

        assertTrue(counter.get() == 10);
        assertEquals(res, "success");
    }
}