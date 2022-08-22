package org.kr.intp.application.job.scheduler;

import org.kr.intp.application.pojo.job.JobProperties;
import junit.framework.TestCase;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created bykron 10.06.2015.
 */
public class PeriodicalExecutorTest extends TestCase {

    public void testScheduleSECONDS() throws Exception {
        doTest(JobProperties.PeriodChoice.SECONDS);
        doTest(JobProperties.PeriodChoice.DAY);
    }

    private void doTest(JobProperties.PeriodChoice periodChoice) throws InterruptedException {
        final PeriodicalExecutor executor = new PeriodicalExecutor("test_pe", 1, periodChoice);

        final AtomicBoolean done = new AtomicBoolean(false);

        final Runnable task = new Runnable() {
            public void run() {
                System.out.println("done");
                done.set(true);
            }
        };
        executor.schedule(task, 3000, 10);
        final long start = System.currentTimeMillis();
        while (!done.get()) {
            Thread.sleep(50);
        }
        final long duration = System.currentTimeMillis() - start;
        executor.shutdown();
        System.out.println(duration);
    }
}