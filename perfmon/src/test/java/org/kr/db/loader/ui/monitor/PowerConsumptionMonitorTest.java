package org.kr.db.loader.ui.monitor;

import junit.framework.TestCase;

import java.io.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PowerConsumptionMonitorTest extends TestCase {

    public void testGetStats() throws Exception {
        final long duration = TimeUnit.SECONDS.toMillis(15);

        PowerConsumptionMonitor monitor = new PowerConsumptionMonitor("192.168.1.4", 9999, duration);
        new Thread(monitor).start();
        Thread.sleep(duration - 500);
        List<String> results = monitor.getResults();
        System.out.println(results);

        File f = new File("c:\\etc\\pc_out.csv");
        PrintWriter writer = new PrintWriter(new FileOutputStream(f));
        for (String line : results)
            writer.println(line);
        writer.flush();
        writer.close();
        System.out.println("File written");
    }

    public void testGetStatsError() throws Exception {
        PowerConsumptionMonitor monitor = new PowerConsumptionMonitor("kkk", 8888, 0);
        new Thread(monitor).start();
        Thread.sleep(500);
        List<String> results = monitor.getResults();
        System.out.println(results);
    }
}