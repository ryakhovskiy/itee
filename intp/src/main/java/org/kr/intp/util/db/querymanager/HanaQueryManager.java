package org.kr.intp.util.db.querymanager;

import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by kr on 7/7/2014.
 */
public abstract class HanaQueryManager {

    private final ServiceConnectionPool serviceConnectionManager = ServiceConnectionPool.instance();

    private HanaQueryManager() {

    }

    public void updateServerStatus(boolean isRunning) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {

        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

}
