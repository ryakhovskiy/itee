package org.kr.intp.util.db.project;

import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.meta.DbMetadataManager;
import org.kr.intp.util.db.meta.ObjectType;
import org.kr.intp.util.db.meta.hana.HanaDbMetadataManager;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import org.kr.intp.util.io.QueryReader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 */
public class IntpProjectManager {

    private static final Logger logger = LoggerFactory.getLogger(IntpProjectManager.class);

    static {
        try {
            DbMetadataManager dbMetadataManager = null;
            try {
                dbMetadataManager = new HanaDbMetadataManager();
            } finally {
                if (null != dbMetadataManager)
                    dbMetadataManager.close();
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks whether all In-Time projects which are shipped with the In-Time server by default exist in the database.
     */
    public static void checkIntpProjects() {
        try {
            new IntpProjectManager().checkProjects();
        } catch (Exception e) {
            String message = "Could not check default In-Time projects." + e.getMessage();
            logger.error(message, e);
            throw new RuntimeException(e);
        }
    }

    private void checkProjects() throws SQLException, IOException {
        DbMetadataManager metadataManager = null;
        Connection connection = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            metadataManager = new HanaDbMetadataManager(connection);
            checkProjects(metadataManager);
        } finally {
            if (null != metadataManager)
                metadataManager.close();
            if (null != connection)
                connection.close();
        }
    }

    /**
     * This method defines all project files that have to be processed. It expects a @see DbMetadataManager
     *
     * @param metadataManager the given @see DbMetadataManager
     * @throws SQLException
     * @throws IOException
     */
    private void checkProjects(DbMetadataManager metadataManager) throws SQLException, IOException {
        logger.debug("Checking default In-Time projects...");
        handleProjectQueryFile(metadataManager, "org/kr/intp/db/projects/di-general.sql");
        handleProjectQueryFile(metadataManager, "org/kr/intp/db/projects/di-index-server-monitor.sql");
        handleProjectQueryFile(metadataManager, "org/kr/intp/db/projects/di-executed-statement-monitor.sql");

    }

    private void handleProjectQueryFile(DbMetadataManager metadataManager, String resource) throws IOException, SQLException {
        for (QueryReader.Query query : new QueryReader(resource)) {
            // If the "object" is not a statement and does not exist, it will be created
            if (!query.getType().equals(ObjectType.STATEMENT.toString()) && !metadataManager.isObjectExists(query.getSchema(), query.getName(), ObjectType.valueOf(query.getType()))) {
                logger.info(String.format("creating object %s: %s.%s; v. %d", query.getType(), query.getSchema(), query.getName(), query.getVersion()));
                createNewObject(metadataManager, query);
            }
            // If the "object" exists in the database, check the repository, whether the repository version is outdated
            else {
                final int newVersion = query.getVersion();
                final int currentVersion = metadataManager.getMdObjectVersion(query.getName(), ObjectType.valueOf(query.getType()));
                logger.info("Object exists: " + query.getName() + "; v. " + currentVersion + "; new version: " + newVersion);
                if (currentVersion < newVersion) {
                    if (query.getType().equals(ObjectType.STATEMENT.toString())) {
                        logger.info("Updating project configuration for " + query.getName() + ".");
                        createNewObject(metadataManager, query);
                    } else {
                        logger.debug("re-creating object, old version: " + currentVersion + "; new version: " + newVersion);
                        metadataManager.dropObject(query.getSchema(), query.getName(), ObjectType.valueOf(query.getType()));
                        createNewObject(metadataManager, query);
                    }
                }
            }
        }
    }

    private void createNewObject(DbMetadataManager metadataManager, QueryReader.Query query) throws SQLException {
        metadataManager.executeSql(query.getQuery());
        metadataManager.saveObject(query.getName(), ObjectType.valueOf(query.getType()), query.getVersion());
    }
}
