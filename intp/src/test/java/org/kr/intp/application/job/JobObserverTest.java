package org.kr.intp.application.job;

import org.kr.intp.util.db.meta.IntpMetadataManager;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class JobObserverTest {

    @BeforeClass
    public static void setupClass() {
        IntpMetadataManager.checkIntpMetadata();
    }

    @Test
    public void testTryStartDeltaInitialWasNotInitialized() throws Exception {
        final String projectId = "ISM";
        final String schema = "INTP";
        final JobObserver observer = JobObserver.getInstance(projectId);
        final InitialJobDBController controller = new InitialJobDBController(schema);
        boolean notified = controller.notifyStarted(projectId);
        assertTrue(notified);
        boolean initialized = controller.isProjectInitialized(projectId);
        assertFalse(initialized);

        boolean startAllowed = observer.tryStartDelta();
        assertFalse(startAllowed);
    }

}