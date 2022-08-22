package org.kr.intp.application.manager;

import org.kr.intp.IntpServerInfo;
import org.kr.intp.application.agent.IntpServer;
import org.kr.intp.application.monitor.EreMdManipulator;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.config.IntpFileConfig;
import junit.framework.TestCase;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EREManagerTest extends TestCase {

    private static final String PROJECT_ID = "AUTOTST" + System.currentTimeMillis();
    private final IntpConfig config = new IntpConfig(IntpFileConfig.getResourceInstance().getPropertiesMap());

    private final EreMdManipulator ereMdManipulator = new EreMdManipulator();

    public void testEreManager() throws Exception {
        final String name = "AUTOTST, Material Management";
        final boolean enabled = true;
        final int age = 120;
        final String rtobj = "rtobj";
        final long monitorFrequency = 5000;
        final long modeMinRuntime = 60000;
        final int cpu = 90;
        final int mem = 90;
        final double avg_ratio = 2.3;
        final double max_ratio = 1.2;
        final Map<String, Integer> users = new HashMap<String, Integer>();
        users.put("all_users", 30);

        ereMdManipulator.addEREParams(PROJECT_ID, name, enabled, age, rtobj, monitorFrequency, modeMinRuntime,
                cpu, mem, avg_ratio, max_ratio, users);

        IntpServer server = new IntpServer(new IntpServerInfo("003", "int_dev", 'D', 4455, 10000), config);
        server.start();

        final EREManager manager = EREManager.getInstance(PROJECT_ID);
        manager.runMonitors();
        Thread.sleep(10 * 1000);
        manager.close();
        server.close();
        ereMdManipulator.deleteERE(PROJECT_ID);
    }

    public void testEreManagerSintetyc() throws IOException {
        final String json = "{\"project_id\":\"DINMM\",\"name\":\"EREZMM\",\"enabled\":true,\"it_object\":\"test\",\"rt_object\":\"test\",\"monitor_frequency_ms\":\"3000\",\"mode_min_runtime_ms\":60000,\"stats_age_sec\":300,\"cpu_threshold\":101,\"mem_threshold\":45,\"connections_threshold\":50,\"avg_ratio_threshold\":3,\"max_ratio_threshold\":5,\"users\":{}}";
        final ObjectMapper mapper = new ObjectMapper();
        final Map map = mapper.readValue(json, Map.class);
        final boolean enabled = (Boolean) map.get("enabled");
        final String projectId = map.get("project_id").toString();
        final String name = map.get("name").toString();
        final int stats_age_sec = (Integer) map.get("stats_age_sec");
        final Integer con_threshold = (Integer) map.get("connections_threshold");
        final Map<String, Integer> users = (Map) map.get("users");
        users.put("all_users", con_threshold);
        final String rtObject = map.get("rt_object").toString();
        final int monitorFrequency = (Integer) map.get("monitor_frequency_ms");
        final int modeMinRuntime = (Integer) map.get("mode_min_runtime_ms");
        final int cpu = (Integer) map.get("cpu_threshold");
        final int mem = (Integer) map.get("mem_threshold");
        final double avg_ratio = Double.parseDouble (map.get("avg_ratio_threshold").toString());
        final double max_ratio = Double.parseDouble (map.get("max_ratio_threshold").toString());
//        final EREMonitor monitor = new EREMonitor(projectId, name, stats_age_sec, rtObject, 0l, monitorFrequency,
//                modeMinRuntime, cpu, mem, avg_ratio, max_ratio, users);
    }

    public void testDINMM() throws Exception {
        EREManager manager = null;
        IntpServer server = null;
        try {
            server = new IntpServer(new IntpServerInfo("003", "int_dev", 'D', 4455, 10000), config);
            server.start();
            manager = EREManager.getInstance("DINMM");
        } finally {
            if (null != manager)
                manager.close();
            if (null != server)
                server.close();
        }
    }

}