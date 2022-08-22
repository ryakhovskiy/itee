package org.kr.itee.perfmon.ws.utils;

import java.util.TimeZone;

/**
 * Created by kr on 5/15/2016.
 */
public class Timer {

    private static final long offset = getOffset();

    public static long currentTimeMillis() {
        return System.currentTimeMillis() + offset;
    }

    private static long getOffset() {
        final TimeZone tz = TimeZone.getDefault();
        return (-1) * tz.getOffset(System.currentTimeMillis());
    }

}
