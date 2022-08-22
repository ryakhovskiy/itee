package org.kr.intp.application.monitor;

import org.kr.intp.application.AppContext;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created bykron 22.08.2014.
 */
public class EreMdManipulator {

    private final String SCHEMA = AppContext.instance().getConfiguration().getIntpSchema();
    private final ObjectMapper mapper = new ObjectMapper();

    public EREMonitor createEREMonitor(String projectId, String name, boolean enabled, int age, String rtObject, long sla,
                                       long monitorFrequency, long modeMinRuntime, int cpu, int mem,
                                       double avg_ratio, double max_ratio, Map<String, Integer> users) throws SQLException, IOException {
        addEREParams(projectId, name, enabled, age, rtObject, monitorFrequency, modeMinRuntime, cpu, mem,
                avg_ratio, max_ratio, users);
        return new EREMonitor(projectId, name, age, rtObject, sla, monitorFrequency, modeMinRuntime, cpu, mem,
                    avg_ratio, max_ratio, users);
    }

    public void addEREParams(String projectId, String name, boolean enabled, int age, String rtObject,
                             long monitorFrequency, long modeMinRuntime, int cpu, int mem,
                             double avg_ratio, double max_ratio, Map<String ,Integer> users)
            throws SQLException, IOException {
        final String sql = "insert into " + SCHEMA + ".ERE_PARAMS values (?,?,?,?)";
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.prepareStatement(sql);
            final Map<String, Object> paramsMap = new HashMap<String, Object>();
            paramsMap.put("project_id", projectId);
            paramsMap.put("name", name);
            paramsMap.put("enabled", enabled);
            paramsMap.put("stats_age_sec", age);
            final Integer con_threshold = users.remove("all_users");
            paramsMap.put("connections_threshold", con_threshold);
            paramsMap.put("users", users);
            paramsMap.put("rt_object", rtObject);
            paramsMap.put("monitor_frequency_ms", monitorFrequency);
            paramsMap.put("mode_min_runtime_ms", modeMinRuntime);
            paramsMap.put("cpu_threshold", cpu);
            paramsMap.put("mem_threshold", mem);
            paramsMap.put("avg_ratio_threshold", avg_ratio);
            paramsMap.put("max_ratio_threshold", max_ratio);

            final String params = mapper.writeValueAsString(paramsMap);
            statement.setString(1, projectId);
            statement.setString(2, name);
            statement.setInt(3, enabled ? 1 : 0);
            statement.setString(4, params);
            statement.execute();

        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    public void deleteERE(String projectId) throws SQLException {
        final String eresql = "delete from " + SCHEMA + ".ERE_PARAMS where PROJECT_ID = ?";
        Connection connection = null;
        PreparedStatement ereStmnt = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            ereStmnt = connection.prepareStatement(eresql);
            ereStmnt.setString(1, projectId);
            ereStmnt.execute();
        } finally {
            if (null != ereStmnt)
                ereStmnt.close();
            if (null != connection)
                connection.close();
        }
    }

}
