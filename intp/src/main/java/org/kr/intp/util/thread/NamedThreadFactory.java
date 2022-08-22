package org.kr.intp.util.thread;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 8/25/13
 * Time: 6:39 PM
 * To change this template use File | Settings | File Templates.
 */
public final class NamedThreadFactory implements ThreadFactory {

    private final AtomicInteger counter = new AtomicInteger(0);
    private final String name;

    public static NamedThreadFactory newInstance(String name) {
        return new NamedThreadFactory(name);
    }

    private NamedThreadFactory(String name) {
        this.name = name + "_";
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, name + counter.incrementAndGet());
        t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                System.err.printf("%s: %s", t.getName(), e.getMessage());
            }
        });
        return t;
    }
}
