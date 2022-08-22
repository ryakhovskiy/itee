package org.kr.intp.util.db.load.hana;

import junit.framework.TestCase;

/**
 * Created by kr on 31.03.2014.
 */
public class HanaDBMSLoadCheckerTest extends TestCase {

    public void testIsLoadThresholdReached() throws Exception {
        final boolean isReached = HanaDBMSLoadChecker.newInstance().isLoadThresholdReached();
        System.out.println(isReached);
    }

}
