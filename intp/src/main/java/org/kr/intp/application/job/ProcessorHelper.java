package org.kr.intp.application.job;

import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.TimeController;
import org.kr.intp.util.db.pool.IDataSource;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;

public class ProcessorHelper {

    private final Logger log = LoggerFactory.getLogger(ProcessorHelper.class);
    private final boolean isTraceEnabled = log.isTraceEnabled();
    private final IDataSource dataSource;
    private final String preProcessingSql;
    private final String postProcessingSql;

    public ProcessorHelper(String schema, IDataSource dataSource, String projectId, JobType type) {
        this.dataSource = dataSource;
        this.preProcessingSql = String.format("call \"%s\".\"%s_%s_PREPROC\"(?, ?)", schema, projectId, type);
        this.postProcessingSql = String.format("call \"%s\".\"%s_%s_POSTPROC\"(?, ?)", schema, projectId, type);
    }

    public void invokePreProcessing(String jobId, long time) throws SQLException {
        if (isTraceEnabled)
            log.trace("invoking pre-processing for " + jobId);
        final long duration = invokeStoredProcedure(preProcessingSql, jobId, time);
        if (isTraceEnabled)
            log.trace(jobId + " pre-processing done in (ms)" + duration);
    }

    public void invokePostProcessing(String jobId, long time) throws SQLException {
        if (isTraceEnabled)
            log.trace("invoking post-processing for " + jobId);
        final long duration = invokeStoredProcedure(postProcessingSql, jobId, time);
        if (isTraceEnabled)
            log.trace(jobId + " post-processing done in (ms)" + duration);
    }

    private long invokeStoredProcedure(String callableSql, String jobId, long time) throws SQLException {
        final long start = TimeController.getInstance().getServerUtcTimeMillis();
        Connection connection = null;
        CallableStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareCall(callableSql);
            statement.setString(1, jobId);
            statement.setTimestamp(2, new Timestamp(time));
            statement.execute();
            return TimeController.getInstance().getServerUtcTimeMillis() - start;
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

}
