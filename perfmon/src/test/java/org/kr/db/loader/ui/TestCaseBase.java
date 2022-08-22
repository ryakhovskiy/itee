package org.kr.db.loader.ui;

import junit.framework.TestCase;

/**
 * Created by kr on 5/17/2014.
 */
public class TestCaseBase extends TestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        System.out.printf("%n---------------------------------%nStarting Test: %s%n", this.getClass().getName());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.out.printf("%n---------------------------------%nShutting down Test: %s%n", this.getClass().getName());
    }

    public void testString() {

        String url1 = "jdbc:sap://10.118.38.2:30215?user=intpadmin&password=xxx";
        String url2 = "jdbc:sap://10.118.38.2:30215?user=intpadmin&password=xxx";

        System.out.println(url1.length());
        System.out.println(url2.length());

    }

}
