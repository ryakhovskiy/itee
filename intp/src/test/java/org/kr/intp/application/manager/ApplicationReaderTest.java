package org.kr.intp.application.manager;

import org.kr.intp.application.pojo.job.Application;
import org.kr.intp.application.pojo.job.ApplicationJob;
import org.kr.intp.application.pojo.job.JobProperties;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ApplicationReaderTest  {

    @Test
    public void testGetApplication() throws Exception {
        ApplicationReader applicationReader = ApplicationReader.getInstance();
        Application application = applicationReader.getApplication("FIRI", 0);
        System.out.println(application);

        assertNotNull(application);

        ApplicationJob[] jobs = application.getApplicationJobs();

        assertNotNull(jobs);
        assertTrue(jobs.length == 1);
        ApplicationJob job = jobs[0];
        assertNotNull(job);
        JobProperties properties = job.getInitialProperties();
        assertNotNull(properties);
        JobProperties.ConnectionType type = properties.getConnectionType();
        assertNotNull(type);
    }
}