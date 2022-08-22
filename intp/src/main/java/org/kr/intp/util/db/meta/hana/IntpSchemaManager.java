package org.kr.intp.util.db.meta.hana;

import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by kr on 12/24/13.
 */
public class IntpSchemaManager extends HanaDbMetadataManager {

    private final Logger log = LoggerFactory.getLogger(IntpSchemaManager.class);

    public IntpSchemaManager() throws SQLException {
        super();
    }

    private void createMetadata() throws SQLException {
        createSchema();
        createStatementsTable();
    }

    private void createStatementsTable() throws SQLException {
        final String tableName = "RT_STATEMENTS";
        if (isTableExists(tableName))
            return;
        final String sql =
                String.format("CREATE ROW TABLE %s.%s (projectId NVARCHAR(30), NAME NVARCHAR(100), VERSION INTEGER, STATEMENT NVARCHAR(4000), PRIMARY KEY ( NAME, VERSION ))"
                        , schema, tableName);
        Statement statement = null;
        try {
            statement = connection.createStatement();
            statement.execute(sql);
            log.debug("RT_STATEMENTS table created");
        } finally {
            if (null != statement)
                statement.close();
        }
    }

    private void createSchema() throws SQLException {
        //schema could be created only once.
        synchronized (IntpSchemaManager.class) {
            if (isSchemaExists())
                return;
            log.debug(String.format("creating schema %s...", schema));
            final String sql = "create schema " + schema;
            Statement statement = null;
            try {
                statement = connection.createStatement();
                statement.execute(sql);
                log.debug("schema created [" + schema + "]");
            } finally {
                if (null != statement)
                    statement.close();
            }
        }
    }

    public void close() {
        if (null != connection)
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
    }

}
