package org.kr.intp.application.agent;

import org.kr.intp.application.AppContext;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.*;

/**
 * Created by kr on 7/10/2014.
 */
public class IntpStatusModifier {

    private static final String SCHEMA = AppContext.instance().getConfiguration().getIntpSchema();
    private static final IntpStatusModifier instance = new IntpStatusModifier();

    public static IntpStatusModifier getInstance() {
        return instance;
    }

    private final Logger log = LoggerFactory.getLogger(IntpStatusModifier.class);

    private IntpStatusModifier() {}

    public void setIntpStarted(final String instanceId) {
        log.debug("setting instance status");
        final String sql = String.format("call \"%s\".\"SET_RUNNING\"(?)", SCHEMA);
        try (Connection connection = ServiceConnectionPool.instance().getConnection()) {
            checkProcedureExists(connection);
            try (CallableStatement statement = connection.prepareCall(sql)) {
                statement.setString(1, instanceId);
                statement.execute();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot change INTP status in the RT_SERVER table", e);
        }
    }

    public void setIntpStopped(final String instanceId) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            final String query = String.format("update %s.rt_server set status = 0 where instance_id = ?", SCHEMA);
            statement = connection.prepareStatement(query);
            statement.setString(1, instanceId);
            statement.executeUpdate();
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void checkProcedureExists(final Connection connection) throws SQLException {
        final boolean procedureExists = isProcedureExists(connection);
        if (procedureExists)
            return;
        throw new SQLException("Procedure SET_RUNNING is not found");
    }

    private boolean isProcedureExists(final Connection connection) throws SQLException {
        final String procName = "SET_RUNNING";
        final String sql = "select PROCEDURE_OID from procedures where schema_name = ? and procedure_name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, SCHEMA);
            statement.setString(2, procName);
            try (ResultSet set = statement.executeQuery()) {
                return set.next();
            }
        }
    }
}
