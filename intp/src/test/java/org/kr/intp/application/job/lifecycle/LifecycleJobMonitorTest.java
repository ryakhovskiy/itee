package org.kr.intp.application.job.lifecycle;

import org.kr.intp.IntpTestBase;
import org.kr.intp.application.job.JobInitializator;
import org.kr.intp.application.manager.DbApplicationManager;

import static org.mockito.Mockito.mock;


/**
 * Created by kr on 30.01.14.
 */
public class LifecycleJobMonitorTest extends IntpTestBase {

    public void testNewInstance() throws Exception {
        DbApplicationManager dbApplicationManager = mock(DbApplicationManager.class);
        JobInitializator jobInitializator = new JobInitializator();
        Thread t = new Thread(LifecycleJobMonitor.newInstance(jobInitializator, dbApplicationManager));
        t.start();
        Thread.sleep(10 * 1000);
        t.interrupt();
        t.join();
    }
}
