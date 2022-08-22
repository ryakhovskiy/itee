package org.kr.db.loader;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by kr on 21.01.14.
 */
public class NamedThreadFactory implements ThreadFactory {

    public static NamedThreadFactory newInstance(String name) {
        return new NamedThreadFactory(name);
    }

    private final String name;
    private final AtomicInteger counter = new AtomicInteger(0);

    private NamedThreadFactory(String name) {
        if (null == name)
            throw new IllegalArgumentException("name cannot be null");
        this.name = name;
    }

    @Override
    public Thread newThread(Runnable r) {
        if (null == r)
            throw new IllegalArgumentException("Runnable cannot be null");
        return new Thread(r, name + "_" + counter.incrementAndGet());
    }
}
