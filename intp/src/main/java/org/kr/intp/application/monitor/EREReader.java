package org.kr.intp.application.monitor;

import org.kr.intp.application.AppContext;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created bykron 14.08.2014.
 */
public class EREReader {

    public static List<Map> getProjectEREs(String projectId) throws IOException, SQLException {
        return new EREReader().getEREs(projectId);
    }

    private static final String SCHEMA = AppContext.instance().getConfiguration().getIntpSchema();

    private static final ObjectMapper mapper = new ObjectMapper();

    private final Logger log = LoggerFactory.getLogger(EREReader.class);
    private final boolean isTraceEnabled = log.isTraceEnabled();

    private EREReader() { }

    private List<Map> getEREs(String projectId) throws SQLException, IOException {
        log.trace("reading ERE for " + projectId);
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.prepareStatement(GET_ERE_INFOs);
            statement.setString(1, projectId);
            resultSet = statement.executeQuery();
            final List<Map> list = new ArrayList<Map>();
            while (resultSet.next()) {
                final String json = resultSet.getString(1);
                final Map props = mapper.readValue(json, Map.class);
                list.add(props);
                if (isTraceEnabled)
                    log.trace("ERE Props read: " + props);
            }
            return list;
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private static final String GET_ERE_INFOs =
            "select json from " + SCHEMA + ".ERE_PARAMS where PROJECT_ID = ? and enabled = 1";

}
