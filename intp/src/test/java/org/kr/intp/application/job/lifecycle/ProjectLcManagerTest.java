package org.kr.intp.application.job.lifecycle;

import org.kr.intp.application.job.JobInitializator;
import org.kr.intp.application.manager.DbApplicationManager;
import junit.framework.TestCase;

import static org.mockito.Mockito.mock;

public class ProjectLcManagerTest extends TestCase {

    public void testRunLC() throws Exception {
        JobInitializator initializator = mock(JobInitializator.class);
        DbApplicationManager applicationManager = mock(DbApplicationManager.class);
        ProjectLcManager lcManager = new ProjectLcManager(initializator, applicationManager, "DINMM");
        lcManager.runLC();

        Thread.sleep(10000);

        lcManager.close();
    }
}