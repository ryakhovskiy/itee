package org.kr.itee.perfmon.ws.os;

import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

/**
 *
 */
public class ProcessControllerTest {

    @Test
    public void testStartProcess() throws IOException, InterruptedException {


        ProcessController controller = new ProcessController();
        Properties properties = new Properties();
        properties.setProperty("query.file", "../queries/it_query.txt");
        properties.setProperty("round.robin", "false");
        properties.setProperty("query.file.encoding", "UTF8");
        properties.setProperty("jdbc.pass", "xxx");
        properties.setProperty("jdbc.server", "10.118.38.2:30215");
        properties.setProperty("connection.pooling.enabled", "false");
        properties.setProperty("query.type", "query");
        properties.setProperty("exec.time", "120");
        properties.setProperty("jdbc.url", "jdbc:sap://10.118.38.2:30215?user=RTREPORT&password=xxx");
        properties.setProperty("jdbc.user", "RTREPORT");
        properties.setProperty("scheduler.interval.ms", "250");
        properties.setProperty("jdbc.driver", "com.sap.db.jdbc.Driver");
        properties.setProperty("concurrent.executors", "50");
        properties.setProperty("queries.per.interval", "1");

        controller.startProcess(properties);

        Thread.sleep(2000L);

        controller.destroyAll();
        Thread.sleep(2000L);
    }

}