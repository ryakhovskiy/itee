package org.kr.db.loader;

import java.util.Arrays;

/**
 */
public class AppMainDummy {

    public static void main(String... args) throws InterruptedException {
        System.out.printf("in args: %s%n", Arrays.toString(args));
        long sleep = 1000;
        if (args.length > 0) {
            try {
                sleep = Long.parseLong(args[0]);
            } catch (Exception e) {
                System.err.printf("Cannot cast 1st argument to long: %s%n", e.getMessage());
            }
        }
        System.out.println("sleep time: " + sleep);
        Thread.sleep(sleep);
        System.exit(0);
    }

}
