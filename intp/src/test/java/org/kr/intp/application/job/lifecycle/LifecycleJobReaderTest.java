package org.kr.intp.application.job.lifecycle;

import org.kr.intp.application.pojo.job.LifecycleJob;
import junit.framework.TestCase;

import java.util.Arrays;

public class LifecycleJobReaderTest extends TestCase {

    public void testGetActiveLifecycleJobs() throws Exception {
        LifecycleJob[] d = LifecycleJobReader.getInstance().getActiveLifecycleJobs();
        System.out.println(Arrays.toString(d));
    }

    public void testGetActiveLifecycleJobsWithProjectId() throws Exception {
        final String projectId = "DINMM";
        LifecycleJob[] d = LifecycleJobReader.getInstance().getActiveLifecycleJobs(projectId);
        System.out.println(Arrays.toString(d));
    }
}