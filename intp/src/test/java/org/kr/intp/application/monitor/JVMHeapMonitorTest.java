package org.kr.intp.application.monitor;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created bykron 06.12.2016.
 */
public class JVMHeapMonitorTest {

    private final Random random = ThreadLocalRandom.current();

    @Test
    public void getStatistics() throws Exception {
        JVMHeapMonitor JVMHeapMonitor = new JVMHeapMonitor();
        JVMHeapMonitor.start();
        JVMHeapMonitor.Statistics cleanStats = JVMHeapMonitor.getStatistics();
        //create garbage
        int count = random.nextInt(1 << 22) + (1 << 24);
        List<String> strings = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String hex = Double.toHexString(random.nextDouble());
            strings.add(hex);
        }
        //make sure that garbage was actually created
        // and not optimized by compiler
        int index = random.nextInt(count);
        System.out.println(strings.size());
        System.out.println(strings.get(index));
        JVMHeapMonitor.Statistics newStats = JVMHeapMonitor.getStatistics();
        index = random.nextInt(count);
        System.out.println(strings.size());
        System.out.println(strings.get(index));
        strings = null;
        JVMHeapMonitor.gc();
        Thread.sleep(150);
        JVMHeapMonitor.Statistics newCleanStats = JVMHeapMonitor.getStatistics();

        System.out.println(cleanStats);
        System.out.println(newStats);
        System.out.println(newCleanStats);
    }
}