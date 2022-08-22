package org.kr.intp.application.job;

import org.kr.intp.IntpServerInfo;
import org.kr.intp.application.agent.IntpServer;
import org.kr.intp.application.job.hooks.HookManager;
import org.kr.intp.application.manager.DbApplicationManager;
import org.kr.intp.application.pojo.event.Event;
import org.kr.intp.application.pojo.event.EventType;
import org.kr.intp.application.pojo.job.ProjectKeyType;
import org.kr.intp.application.pojo.job.ProjectTableKey;
import org.kr.intp.application.pojo.job.SpecialProcedureMapping;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.config.IntpFileConfig;
import org.kr.intp.util.db.pool.DataSourceFactory;
import org.kr.intp.util.db.pool.IDataSource;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 *
 */
public class InitialJobExecutorTest {

    private final Logger log = LoggerFactory.getLogger(InitialJobExecutorTest.class);
    private final String projectId = "FIRI";
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final long keyProcessingExecutionMaxDurationMS = 120_000L;
    private final long keyProcessingTotalTimeoutMS = 600_000L;
    private final long keyProcessingRetryPauseMS = 5_000L;
    private final IntpConfig config = new IntpConfig(IntpFileConfig.getResourceInstance().getPropertiesMap());

    private IntpServer server;

    @Before
    public void setup() throws Exception {
        IntpConfig config = new IntpConfig(IntpFileConfig.getResourceInstance().getPropertiesMap());
        server = new IntpServer(new IntpServerInfo("100", "test", 'D', 4455, 100000), config);
        server.start();
        Connection connection = ServiceConnectionPool.instance().getConnection();
        Statement statement = connection.createStatement();
        final String sql = "UPDATE \"INTP\".\"RT_ACTIVE_PROJECTS\" \n" +
                "\tSET SCHEDULER_TIME_BASE = NULL WHERE PROJECT_ID = 'FIRI'";
        statement.execute(sql);
        statement.close();
        connection.close();
    }

    @After
    public void tearDown() throws IOException {
        if (null == server)
            return;
        server.close();
    }

    @Test
    public void testInitialJobRunNoMocks() throws IOException, SQLException, InterruptedException {
        boolean scheduled = server.startApplication("FIRI");
        assertTrue(scheduled);

        Thread.sleep(60000L);
        log.info("done");
    }

    @Test
    public void testInitialJobRun() throws Exception {
        final int processors = 5;
        IDataSource dataSource = DataSourceFactory.newDataSource(processors);
        List<Object[]> keys = getKeys();
        int keysCount = keys.get(0).length - 1;
        KeysReader keysReader = getKeysReaderMock(keys);
        EventLogger logger = getEventLoggerMock();
        JobObserver observer = getJobObserverMock();
        HookManager hookManager = mock(HookManager.class);
        ProjectTableKey[] keysDefinition = getKeysDefinition();
        final String processSqlStatement =
                "call \"PCS\"." +
                        "\"kr.in-time.apps.pcsr.procs::INIT_CLOSED_PERIOD_MCE_IN_TIME_REF\"" +
                        "(?,?,?,?,?,?,?,?)";
        SpecialProcedureMapping spMapping = null;

        JobInitializator initializator = mock(JobInitializator.class);
        DbApplicationManager dbApplicationManager = mock(DbApplicationManager.class);

        InitialJobExecutor executor = new InitialJobExecutor(initializator, dbApplicationManager, projectId,
                keysReader, processSqlStatement, processors, keysCount, logger, observer, hookManager, dataSource,
                keysDefinition, spMapping, keyProcessingTotalTimeoutMS, keyProcessingRetryPauseMS,
                keyProcessingExecutionMaxDurationMS);
        executor.run();
        assertTrue(finished.get());
    }

    private ProjectTableKey[] getKeysDefinition() {
        ProjectTableKey[] keys = new ProjectTableKey[6];
        keys[0] = new ProjectTableKey(projectId, 1, 1, "ZZDTA", "RYEAR", ProjectKeyType.STRING, "", 10, true, false, true);
        keys[1] = new ProjectTableKey(projectId, 2, 1, "ZZDTA", "POPER", ProjectKeyType.STRING, "", 10, true, false, true);
        keys[2] = new ProjectTableKey(projectId, 3, 1, "ZZDTA", "RBUKRS", ProjectKeyType.STRING, "", 10, true, false, false);
        keys[3] = new ProjectTableKey(projectId, 4, 1, "ZZDTA", "RACCT", ProjectKeyType.STRING, "", 10, false, false, false);
        keys[4] = new ProjectTableKey(projectId, 5, 1, "ZZDTA", "RCNTR", ProjectKeyType.STRING, "", 10, false, false, false);
        keys[5] = new ProjectTableKey(projectId, 6, 1, "ZZDTA", "RZZPERNR", ProjectKeyType.STRING, "", 10, false, true, false);
        return keys;
    }

    private JobObserver getJobObserverMock() {
        JobObserver jobObserver = mock(JobObserver.class);
        when(jobObserver.tryStartInitial()).thenReturn(true);
        return jobObserver;
    }

    private KeysReader getKeysReaderMock(List<Object[]> keys) throws SQLException {
        KeysReader keysReader = mock(KeysReader.class);
        when(keysReader.getKeys("null")).thenReturn(keys);
        return keysReader;
    }

    private List<Object[]> getKeys() {
        List<Object[]> keys = new ArrayList<>();
        keys.add(new Object[] { "2015", "001", "ATR", "_NO_LIM_", "_NO_LIM_", "_NO_LIM_", 18306 });
        keys.add(new Object[] { "2015", "001", "ATR", "_NO_LIM_", "_NO_LIM_", "_NO_LIM_", 18306 });
        keys.add(new Object[] { "2015", "001", "DTBA", "_NO_LIM_", "_NO_LIM_", "_NO_LIM_", 5240 });
        keys.add(new Object[] { "2015", "001", "TXIN", "_NO_LIM_", "_NO_LIM_", "_NO_LIM_", 4559 });
        keys.add(new Object[] { "2015", "001", "SVCS", "_NO_LIM_", "_NO_LIM_", "_NO_LIM_", 4388 });
        keys.add(new Object[] { "2015", "001", "FAS", "_NO_LIM_", "_NO_LIM_", "_NO_LIM_", 3399 });
        return keys;
    }

    private EventLogger getEventLoggerMock() {
        EventLogger logger = mock(EventLogger.class);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                log.info(invocationOnMock.toString());
                Object[] args = invocationOnMock.getArguments();
                if (args.length == 0)
                    return null;
                Object arg0 = args[0];
                if (arg0 instanceof Event) {
                    Event e = (Event)arg0;
                    log.info("Event happened: " + e.getEventType() + "; id: " + e.getUuid());
                    if (e.getEventType() == EventType.FINISHED)
                        finished.set(true);
                }
                return null;
            }
        }).when(logger).log(any(Event.class));
        return logger;
    }
}