package org.kr.intp.application.monitor;

import org.kr.intp.IntpTestBase;
import org.kr.intp.application.AppContext;

import java.util.Arrays;

public class HwResourceMonitorTest extends IntpTestBase {



    public void testMonitor() throws Exception {
        HwResourceMonitor hwResourceMonitor = HwResourceMonitor.getHanaHwResourceMonitor();
        int[] stats = hwResourceMonitor.getCpuMemStats();
        System.out.println(Arrays.toString(stats));
        Thread t = new Thread(hwResourceMonitor);
        t.start();

        stats = hwResourceMonitor.getCpuMemStats();
        System.out.println(Arrays.toString(stats));
        Thread.sleep(AppContext.instance().getConfiguration().getHwMonitorFrequencyMS() * 2);

        stats = hwResourceMonitor.getCpuMemStats();
        System.out.println(Arrays.toString(stats));
        Thread.sleep(AppContext.instance().getConfiguration().getHwMonitorFrequencyMS() * 2);

        stats = hwResourceMonitor.getCpuMemStats();
        System.out.println(Arrays.toString(stats));

        hwResourceMonitor.stop();
        t.join();
    }
}