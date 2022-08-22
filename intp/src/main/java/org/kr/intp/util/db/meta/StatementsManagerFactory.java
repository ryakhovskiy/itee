package org.kr.intp.util.db.meta;

import org.kr.intp.util.db.meta.hana.HanaStatementManager;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by kr on 12/24/13.
 */
public class StatementsManagerFactory {

    public static StatementManager newHanaStatementManager() throws SQLException {
        return new HanaStatementManager();
    }

    public static StatementManager newHanaStatementManager(Connection connection) throws SQLException {
        return new HanaStatementManager(connection);
    }

    private StatementsManagerFactory() {

    }

}
