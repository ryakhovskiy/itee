package org.kr.intp.application.job;

import org.kr.intp.application.pojo.event.Event;
import org.kr.intp.application.pojo.job.JobProperties;
import org.kr.intp.util.db.pool.DataSourceFactory;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import com.sap.db.jdbcext.DataSourceSAP;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class KeysProcessorTest {

    private static final long keyProcessingExecutionMaxDurationMS = 120_000L;
    private static final long keyProcessingTotalTimeoutMS = 600_000L;
    private static final long keyProcessingRetryPauseMS = 5_000L;

    private static final String CREATE_TEST_PROC =
            "create procedure intp.test_proc(in job_id nvarchar(64), in job_ts timestamp)\n" +
                    "LANGUAGE SQLSCRIPT\n" +
                    "SQL SECURITY INVOKER AS\n" +
                    "BEGIN\n" +
                    "\tselect 1 from dummy;" +
                    "END";
    private static final String COUNT_TEST_PROC = "select count(*) from procedures where schema_name = 'INTP' " +
            "and procedure_name = 'TEST_PROC'";
    private static final String DROP_TEST_PROC = "drop procedure intp.test_proc";
    private static final String CALL_TEST_PROC = "call intp.test_proc(?,?)";

    @BeforeClass
    public static void setupClass() throws SQLException {
        Connection conn = ServiceConnectionPool.instance().getConnection();
        createProc(conn);
        conn.close();
    }

    private static boolean testProcExists(Connection conn) throws SQLException {
        Statement statement = null;
        ResultSet set = null;
        try {
            statement = conn.createStatement();
            set = statement.executeQuery(COUNT_TEST_PROC);
            if (!set.next())
                return false;
            int count = set.getInt(1);
            return  (count == 1);
        } finally {
            if (null != set)
                set.close();
            if (null != statement)
                statement.close();
        }
    }

    private static void createProc(Connection conn) throws SQLException {
        if (testProcExists(conn))
            return;
        Statement statement = null;
        try {
            statement = conn.createStatement();
            statement.execute(CREATE_TEST_PROC);
            statement.close();
        } finally {
            if (null != statement)
                statement.close();
        }
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        Connection conn = ServiceConnectionPool.instance().getConnection();
        dropProc(conn);
        conn.close();
    }

    private static void dropProc(Connection conn) throws SQLException {
        if (!testProcExists(conn))
            return;
        Statement statement = conn.createStatement();
        statement.execute(DROP_TEST_PROC);
        statement.close();
    }

    private final Logger log = LoggerFactory.getLogger(KeysProcessor.class);

    @Test(timeout = 120_000L)
    public void testNoConnection() throws Exception {
        final String processSqlStatement = "select 0 from dummy";
        DataSource dataSource = new DataSourceSAP();
        final EventLogger logger = mock(EventLogger.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Event e = (Event)invocationOnMock.getArguments()[0];
                log.info("Event logged, job uuid: " + e.getUuid() + "; event type: " + e.getEventType());
                return null;
            }
        }).when(logger).log(any(Event.class));


        KeysProcessor processor =
                new KeysProcessor(processSqlStatement, dataSource, logger, 0, "TEST", "testuuid",
                        new Object[0], System.currentTimeMillis(),
                        keyProcessingExecutionMaxDurationMS, keyProcessingTotalTimeoutMS, keyProcessingRetryPauseMS);
        KeysProcessor.KeysProcessorResult result = processor.call();
        assertTrue(result.getProcessResult() == KeysProcessor.ProcessResult.ERROR);
        assertTrue(result.getInfo().contains("SQL error code: -813"));
    }

    @Test(timeout = 120_000L)
    public void testWithConnection() throws Exception {
        final String processSqlStatement = CALL_TEST_PROC;
        DataSource dataSource = DataSourceFactory.newDataSource(1, null, JobProperties.ConnectionType.SIMPLE, 1000L, 1000L, 0);
        final EventLogger logger = mock(EventLogger.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Event e = (Event) invocationOnMock.getArguments()[0];
                log.info("Event logged, job uuid: " + e.getUuid() + "; event type: " + e.getEventType());
                return null;
            }
        }).when(logger).log(any(Event.class));


        KeysProcessor processor =
                new KeysProcessor(processSqlStatement, dataSource, logger, 0, "TEST", "testuuid",
                        new Object[0], System.currentTimeMillis(),
                        keyProcessingExecutionMaxDurationMS, keyProcessingTotalTimeoutMS, keyProcessingRetryPauseMS);
        KeysProcessor.KeysProcessorResult result = processor.call();
        assertTrue(result.getProcessResult() == KeysProcessor.ProcessResult.OK);
        assertTrue(result.getInfo().equalsIgnoreCase("succeed"));
    }
}
