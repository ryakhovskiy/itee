package org.kr.intp.application.job.scheduler;

import org.kr.intp.application.pojo.job.JobProperties;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.config.IntpFileConfig;

import java.util.HashMap;
import java.util.Map;

public class CalendarExecutorTest extends CustomCalendarTest {

    private static final Runnable task = new Runnable() {
        @Override
        public void run() {
            System.out.println("task launched");
        }
    };
    private final IntpConfig config = new IntpConfig(IntpFileConfig.getResourceInstance().getPropertiesMap());

    public void testCalendarExecutor() throws InterruptedException {
        CalendarExecutor calendarExecutor = new CalendarExecutor("CLT", 2, JobProperties.PeriodChoice.SECONDS);

        Map<String, Boolean> flags = new HashMap<String, Boolean>();
        flags.put("ISFIRSTDAYOFACCOUNTINGPERIOD", null);
        flags.put("ISFIRSTDAYOFBIWEEKLYPERIOD", null);
        flags.put("ISFIRSTDAYOFCALENDARMONTH", null);
        flags.put("ISFIRSTDAYOFCALENDARYEAR", null);
        flags.put("ISFIRSTDAYOFFISCALYEAR", null);
        flags.put("ISFIRSTDAYOFPAYROLLPERIOD", true);
        flags.put("ISINDIAHOLIDAY", null);
        flags.put("ISLASTDAYOFACCOUNTINGPERIOD", null);
        flags.put("ISLASTDAYOFBIWEEKLYPERIOD", null);
        flags.put("ISLASTDAYOFCALENDARMONTH", null);
        flags.put("ISLASTDAYOFCALENDARYEAR", null);
        flags.put("ISLASTDAYOFFISCALYEAR", null);
        flags.put("ISLASTDAYOFPAYROLLPERIOD", null);
        flags.put("ISUSHOLIDAY", null);
        flags.put("ISWORKDAY", null);

        calendarExecutor.schedule(flags, task);
        Thread.sleep(1000000);
    }


    public void testPeriodicalExecutor() throws InterruptedException {
        PeriodicalExecutor periodicalExecutor = new PeriodicalExecutor("PET", 2, JobProperties.PeriodChoice.SECONDS);
        periodicalExecutor.schedule(task, 1);
        Thread.sleep(2000);

        periodicalExecutor.schedule(task, 1, 2);
        Thread.sleep(10 * 1000);


    }

}