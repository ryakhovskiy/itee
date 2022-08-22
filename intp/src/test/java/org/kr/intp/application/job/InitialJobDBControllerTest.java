package org.kr.intp.application.job;

import org.kr.intp.util.db.meta.IntpMetadataManager;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 */
public class InitialJobDBControllerTest {

    @BeforeClass
    public static void setup() {
        IntpMetadataManager.checkIntpMetadata();
    }

    @Test
    public void testNotifyStarted() throws Exception {
        InitialJobDBController controller = new InitialJobDBController("INTP");
        boolean res = controller.notifyStarted("ISM");
        assertTrue(res);
        res = controller.isProjectInitialized("ISM");
        assertFalse(res);
    }

    @Test
    public void testNotifyFinished() throws Exception {
        InitialJobDBController controller = new InitialJobDBController("INTP");
        boolean res = controller.notifyFinished("ISM");
        assertTrue(res);
        res = controller.isProjectInitialized("ISM");
        assertTrue(res);

        //reset status
        res = controller.notifyStarted("ISM");
        assertTrue(res);
    }

    @Test
    public void testNonExistingProjectIsInitialized() throws Exception {
        InitialJobDBController controller = new InitialJobDBController("INTP");
        boolean initialized = controller.isProjectInitialized("bla" + System.currentTimeMillis());
        assertFalse(initialized);
    }

}