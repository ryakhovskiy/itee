package org.kr.intp.client.db;

import org.kr.intp.application.AppContext;
import org.kr.intp.application.manager.DbApplicationManager;
import org.kr.intp.client.protocol.DbRequest;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created bykron 12.06.2014.
 */
public class DatabaseRequestHandler implements Runnable {

    private static final IntpConfig CONFIG = AppContext.instance().getConfiguration();
    private static final long SLEEP_TIMEOUT_MS = 10000;
    private static final String REQUEST_QUERY = "select top 1 REQUEST from \"" +
            CONFIG.getIntpSchema() + "\".\"RT_RR\"";

    private final Logger log = LoggerFactory.getLogger(DatabaseRequestHandler.class);
    private static final boolean traceEmptyRequests = false;
    private DbApplicationManager dbApplicationManager;

    public DatabaseRequestHandler(DbApplicationManager dbApplicationManager) {
        this.dbApplicationManager = dbApplicationManager;
    }

    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted())
                monitor();
        } finally {
            close();
        }
    }

    private void initialize() {
        dbApplicationManager.init();
    }

    private void monitor() {
        try {
            log.info("awaiting next request");
            final DbRequest request = getRequest();
            log.debug("new request received, processing: " + request);
            handleRequest(request);
        } catch (InterruptedException e) {
            log.debug("DatabaseRequestHandler has been interrupted");
            Thread.currentThread().interrupt();
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            tryRestart();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void tryRestart() {
        log.trace("trying to restart...");
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(SLEEP_TIMEOUT_MS);
                Connection conn = ServiceConnectionPool.instance().getConnection();
                conn.close();
                //re-initialize;
                close();
                initialize();
                return;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.debug("DatabaseRequestHandler has been interrupted");
            } catch (SQLException e) {
                log.trace("Database is unreachable: ", e);
            }
        }
    }

    private DbRequest getRequest() throws SQLException, InterruptedException, IOException {
        while (true) {
            Connection connection = null;
            PreparedStatement requestStatement = null;
            ResultSet set = null;
            try {
                connection = ServiceConnectionPool.instance().getConnection();
                requestStatement = connection.prepareStatement(REQUEST_QUERY);
                set = requestStatement.executeQuery();
                if (set.next()) {
                    final String request = set.getString(1);
                    return DbRequest.createRequestFromString(request);
                } else {
                    if (traceEmptyRequests)
                        log.trace("no requests yet");
                    Thread.sleep(250);
                }
            } finally {
                if (null != set)
                    set.close();
                if (null != requestStatement)
                    requestStatement.close();
                if (null != connection)
                    connection.close();
            }
        }
    }

    private void handleRequest(final DbRequest request) throws Exception {
        switch (request.getDbRequestCommand()) {
            case ACTIVATE:
                dbApplicationManager.activate(request.getProjectId(), request.getVersion());
                break;
            case DEACTIVATE:
                dbApplicationManager.deactivate(request.getProjectId());
                break;
            case START:
                dbApplicationManager.start(request.getProjectId());
                break;
            case STOP:
                dbApplicationManager.stop(request.getProjectId());
                break;
            default:
                throw new UnsupportedOperationException("Unsupported request type: " + request.getDbRequestCommand());
        }
    }

    private void close() {

    }
}
