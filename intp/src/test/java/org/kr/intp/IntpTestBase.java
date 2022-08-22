package org.kr.intp;

import org.kr.intp.application.AppContext;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.config.IntpFileConfig;
import org.kr.intp.util.db.TimeController;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 8/24/13
 * Time: 9:23 PM
 * To change this template use File | Settings | File Templates.
 */
public class IntpTestBase extends TestCase {

    static {
        int i = 1 << 16;

        try {
            IntpConfig config = new IntpConfig(IntpFileConfig.getResourceInstance().getPropertiesMap());
            IntpMessages messages = new IntpMessages(Locale.getDefault());
            AppContext.instance().setConfiguration(config);
            AppContext.instance().setIntpMessage(messages);
            loadLog4jTestConfig();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * loads log4j.properties file located in resources directory for test configuration
     * @throws IOException
     */
    private static void loadLog4jTestConfig() throws IOException {
        InputStream inputStream = null;
        try {
            inputStream = ClassLoader.getSystemResourceAsStream("log4j.properties");
            PropertyConfigurator.configure(inputStream);
        } finally {
            if (null != inputStream)
                inputStream.close();
        }
    }

    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public IntpTestBase(String testName) {
        super(testName);
    }

    /**
     * Create the test case
     */
    public IntpTestBase() {
        super();
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(IntpTestBase.class);
    }

    public void testReentrantLock() throws InterruptedException {



        final ReentrantLock lock = new ReentrantLock();
        final Condition wait = lock.newCondition();
        final CountDownLatch latch = new CountDownLatch(1);

        Runnable r1 = new Runnable() {
            @Override
            public void run() {
                try {
                    lock.lock();
                    latch.countDown();
                    System.out.printf("r1 wait...%n");
                    wait.await();
                    System.out.printf("r1 done!%n");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            }
        };

        Runnable r2 = new Runnable() {
            @Override
            public void run() {
                try {
                    latch.await();
                    lock.lock();
                    wait.signal();
                    System.out.printf("r2 done!%n");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            }
        };
        Thread t2 = new Thread(r2);
        Thread t1 = new Thread(r1);

        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }

    /**
     * example of using native java scheduler. may be used with scheduleAtFixedRate to repeat task execution
     */
    public void testNativeScheduler() throws InterruptedException {
        final int delaySeconds = 3;
        final int awaitingSeconds = 5;
        final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        final AtomicBoolean isTaskExecuted = new AtomicBoolean(false);
        final Runnable task = new Runnable() {
            @Override
            public void run() {
                isTaskExecuted.set(true);
                System.out.println("Task executed at: " +
                        new Date(TimeController.getInstance().getServerUtcTimeMillis()));
            }
        };

        final Calendar todayDate = new GregorianCalendar();
        todayDate.setTimeInMillis(TimeController.getInstance().getServerUtcTimeMillis());
        final int year = todayDate.get(Calendar.YEAR);
        final int month = todayDate.get(Calendar.MONTH);
        final int day = todayDate.get(Calendar.DAY_OF_MONTH);
        final int hour = todayDate.get(Calendar.HOUR_OF_DAY);
        final int minute = todayDate.get(Calendar.MINUTE);
        final int second = todayDate.get(Calendar.SECOND) + delaySeconds;
        final Date startTime = new GregorianCalendar(year, month, day, hour, minute, second).getTime();
        System.out.println("Expected start time: " + startTime);
        final long delay = startTime.getTime() - TimeController.getInstance().getServerUtcTimeMillis();
        executorService.schedule(task, delay, TimeUnit.MILLISECONDS);
        executorService.awaitTermination(awaitingSeconds, TimeUnit.SECONDS);
        executorService.shutdown();
        assert isTaskExecuted.get() : "Task has not been executed!";
    }

    public void testInterruptionOfWaitingThread() throws InterruptedException {
        Producer p = new Producer();
        p.start();
        Consumer c = new Consumer(p);
        c.start();
        c.join(3000);
        c.interrupt();
        c.join();
        Thread.sleep(3000);
    }

    class Consumer extends Thread {

        Producer p;

        public Consumer(Producer p) {
            this.p = p;
        }

        public void run() {
            while (!Thread.currentThread().isInterrupted())
                performJob();
        }

        private void performJob() {
            String produced = null;
            try {
                produced = p.produce();
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                Thread.currentThread().interrupt();
            }
            System.out.println("consumed: " + produced);
        }

    }

    class Producer extends Thread {

        private Object sync = new Object();
        private String template = "message #";
        private int counter = 0;
        private String message = "";

        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    Thread.currentThread().interrupt();
                }
                synchronized (sync) {
                    message = template + counter++;
                    sync.notifyAll();
                }
            }
        }


        public String produce() throws InterruptedException {
            synchronized (sync) {
                sync.wait();
                return message;
            }
        }

    }

}
