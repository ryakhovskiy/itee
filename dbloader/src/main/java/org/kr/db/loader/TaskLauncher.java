package org.kr.db.loader;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by kr on 21.01.14.
 */
public class TaskLauncher implements Runnable {

    private static final boolean ROUND_ROBIN_ENABLED = AppConfig.getInstance().isRoundRobinEnabled();
    private static final int QUERIES_PER_INTERVAL = AppConfig.getInstance().getQueriesPerInterval();
    private final Logger log = Logger.getLogger(TaskLauncher.class);
    private final BlockingQueue<String> runQueue;
    private final String[] queries;
    private final AtomicInteger currentIndex = new AtomicInteger(0);
    private final Object stopNotifier;


    public TaskLauncher(BlockingQueue<String> runQueue, Object stopNotifier) throws IOException {
        this.runQueue = runQueue;
        this.queries = readQueries();
        this.stopNotifier = stopNotifier;
    }

    public void run() {
        try {
            log.trace("running task");
            for (int i = 0; i < QUERIES_PER_INTERVAL; i++) {
                int index = getIndex();
                runQueue.put(queries[index]);
            }
        } catch (InterruptedException e) {
            log.debug("interrupted");
            Thread.currentThread().interrupt();
        }
    }

    private int getIndex() throws InterruptedException {
        int index = currentIndex.getAndIncrement();
        if (index >= queries.length && !ROUND_ROBIN_ENABLED) {
            synchronized (stopNotifier) {
                stopNotifier.notifyAll();
            }
        }
        else {
            while (index >= queries.length)
                index -= queries.length;
        }
        return index;
    }

    private String[] readQueries() throws IOException {
        final List<String> data = new ArrayList<String>();
        final String file = AppConfig.getInstance().getQueryFile();
        final Charset encoding = Charset.forName(AppConfig.getInstance().getQueryFileEncoding());
        BufferedReader reader = null;
        String line = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), encoding));
            while (null != (line = reader.readLine())) {
                line = line.trim();
                if (line.length() > 0)
                    data.add(line);
            }
            return data.toArray(new String[1]);
        } finally {
            if (null != reader)
                reader.close();
        }
    }

}
