package org.kr.intp.util.db.meta.hana;

import org.kr.intp.util.db.meta.StatementManager;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by kr on 12/24/13.
 */
public class HanaStatementManager extends StatementManager {

    public HanaStatementManager() throws SQLException {
        super();
    }

    public HanaStatementManager(Connection connection) throws SQLException {
        super(connection);
    }
}
