package org.kr.intp.application.job;

import org.kr.intp.IntpTestBase;
import org.kr.intp.application.pojo.job.ProjectKeyType;
import org.kr.intp.application.pojo.job.ProjectTableKey;

import java.sql.SQLException;
import java.util.*;

public class KeysSorterTest extends IntpTestBase {

    public void testConnection() throws SQLException {

    }

    public void testSort() throws Exception {

        ProjectTableKey[] keys = new ProjectTableKey[4];

        keys[0] = new ProjectTableKey("ZZZ", 1, 1, "Z", "Z1", ProjectKeyType.STRING, "NVARCHAR(18)", 0, true, false, true);
        keys[1] = new ProjectTableKey("ZZZ", 2, 1, "Z", "Z2", ProjectKeyType.STRING, "NVARCHAR(18)", 0, true, false, false);
        keys[2] = new ProjectTableKey("ZZZ", 3, 1, "Z", "Z3", ProjectKeyType.STRING, "NVARCHAR(18)", 0, true, false, true);
        keys[3] = new ProjectTableKey("ZZZ", 4, 1, "Z", "Z4", ProjectKeyType.STRING, "NVARCHAR(18)", 0, false, false, false);

        KeysSorter sorter = new KeysSorter(keys);

        List<Object[]> data = new ArrayList<Object[]>();

        Random random = new Random();
        for (int i = 0; i < 10000; i++) {
            int year = random.nextInt(10) + 2000;
            int period = random.nextInt(16) + 1;
            String z1 = String.valueOf(year);
            String z2 = "ASD" + String.valueOf(random.nextDouble());
            String z3 = String.valueOf(period);
            if (z3.length() == 1)
                z3 = "00" + z3;
            if (z3.length() == 2)
                z3 = "0" + z3;
            String z4 = "sfadsf" + String.valueOf(random.nextDouble());
            data.add(new Object[] {z1, z2, z3, z4});
        }
        long start = System.currentTimeMillis();
        Map<String, List<Object[]>> sorted = sorter.sort(data);
        long duration = System.currentTimeMillis() - start;
        sortChecker(sorted);
        System.out.println(duration);
    }

    private void sortChecker(Map<String, List<Object[]>> sorted) throws InterruptedException {
        int z1 = 0;
        for (String key : sorted.keySet()) {
            Thread.sleep(10);
            int z3 = 1;
            List<Object[]> lo = sorted.get(key);
            System.out.println("--------------------------------------------------------------");
            for (Object[] o : lo) {
                System.out.println(Arrays.toString(o));
                int o1 = Integer.valueOf(o[0].toString());
                int o3 = Integer.valueOf(o[2].toString());
                assert o1 >= z1 : String.format("ERROR: o1: %d; z1: %d %n", o1, z1);
                assert o3 >= z3 : String.format("ERROR: o3: %d; z3: %d %n", o3, z3);
                z1 = o1;
                z3 = o3;
                Thread.sleep(1);
            }
        }
    }
}