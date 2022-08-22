package org.kr.intp.application.job.scheduler;

import org.kr.intp.application.pojo.job.JobProperties;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.thread.NamedThreadFactory;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by kr on 4/10/2015.
 */
public class CalendarExecutor extends PeriodicalExecutor {

    private static final long PERIOD = TimeUnit.HOURS.toMillis(4); //4 HOURs
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1,
            NamedThreadFactory.newInstance("CLNDR"));
    private final Logger log = LoggerFactory.getLogger(CalendarExecutor.class);

    public CalendarExecutor(String thread_prefix, int threads, JobProperties.PeriodChoice periodChoice) {
        super(thread_prefix, threads, periodChoice);
    }

    public Future schedule(Map<String, Boolean> calendarInfo, final Runnable task) {
        return executorService.scheduleAtFixedRate(new Scheduler(calendarInfo, task), 0, PERIOD, TimeUnit.MILLISECONDS);
    }

    private class Scheduler implements Runnable {
        private final Map<String, Boolean> calendarInfo;
        private final Runnable task;
        private volatile boolean todayLaunched;
        private int dayOfMonth = 0;

        private Scheduler(Map<String, Boolean> calendarInfo, Runnable task) {
            this.task = task;
            this.calendarInfo = calendarInfo;
            this.dayOfMonth = Calendar.getInstance().get(Calendar.DAY_OF_MONTH);
        }

        public void run() {
            try {
                if (dayOfMonth != Calendar.getInstance().get(Calendar.DAY_OF_MONTH)) {
                    this.dayOfMonth = Calendar.getInstance().get(Calendar.DAY_OF_MONTH);
                    todayLaunched = false;
                }
                if (isLaunchRequired()) {
                    CalendarExecutor.super.executor.submit(task);
                    this.todayLaunched = true;
                }
            } catch (SQLException e) {
                log.error(e.getMessage());
            }
        }

        private boolean isLaunchRequired() throws SQLException {
            if (todayLaunched)
                return false;
            return CustomCalendar.getInstance().isCurrentDateFlagged(calendarInfo);
        }
    }
}
