package org.kr.intp.application.monitor;

import org.kr.intp.application.pojo.job.EreJob;
import junit.framework.TestCase;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Map;

public class EREConvertorTest extends TestCase {

    public void testConvert() throws Exception {
        final String json = "{\"project_id\":\"DINMM\",\"name\":\"EREZMM\",\"enabled\":true,\"it_object\":\"test\",\"rt_object\":\"test\",\"monitor_frequency_ms\":\"3000\",\"mode_min_runtime_ms\":60000,\"stats_age_sec\":300,\"cpu_threshold\":101,\"mem_threshold\":45,\"connections_threshold\":50,\"avg_ratio_threshold\":3,\"max_ratio_threshold\":5,\"users\":{}}";
        final ObjectMapper mapper = new ObjectMapper();
        final Map map = mapper.readValue(json, Map.class);

        final EreJob job = EREConvertor.getInstance().convert(map);
        System.out.println(job);
    }
}