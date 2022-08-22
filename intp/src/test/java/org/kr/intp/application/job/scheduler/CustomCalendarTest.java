package org.kr.intp.application.job.scheduler;

import org.kr.intp.application.pojo.job.JobPropertiesTest;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.config.IntpFileConfig;
import org.kr.intp.util.thread.NamedThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CustomCalendarTest extends JobPropertiesTest {

    private final IntpConfig config = new IntpConfig(IntpFileConfig.getResourceInstance().getPropertiesMap());
    private final ScheduledExecutorService serviceScheduledExecutorService = Executors.newScheduledThreadPool(1, NamedThreadFactory.newInstance("STS"));

    public void testCalendar() throws InterruptedException {
        serviceScheduledExecutorService.scheduleAtFixedRate(CustomCalendar.getInstance(), 10, 10, TimeUnit.SECONDS);
        Thread.sleep(1000000);
    }

}