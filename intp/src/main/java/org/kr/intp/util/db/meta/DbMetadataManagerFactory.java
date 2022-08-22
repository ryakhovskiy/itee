package org.kr.intp.util.db.meta;

import org.kr.intp.util.db.meta.hana.HanaDbMetadataManager;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by kr on 12/24/13.
 */
public class DbMetadataManagerFactory {

    public static DbMetadataManager newHanaDbMetadataManager(Connection connection) throws SQLException {
        return new HanaDbMetadataManager(connection);
    }

    private DbMetadataManagerFactory() { }
}
