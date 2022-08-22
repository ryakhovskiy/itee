package org.kr.itee.perfmon.ws.web;

import org.kr.itee.perfmon.ws.pojo.*;
import org.kr.itee.perfmon.ws.utils.IOUtils;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 */
public class RequestConverterTest {

    @Test
    public void convertRequest() throws Exception {
        final String json = IOUtils.getInstance().getResourceAsString("request.json");
        RequestConverter converter = new RequestConverter();
        RequestPOJO request = converter.convertRequest(json);

        assert null != request;
        assert null != request.getItAutorunSpecification();
        assert null != request.getRtAutorunSpecification();
        assert null != request.getItQueryLoadSpecification();
        assert null != request.getItUpdateLoadUpdateSpecification();
        assert null != request.getRtQueryLoadSpecification();
        assert null != request.getRtUpdateLoadSpecification();
    }


    @Test
    public void convertRequestFull() throws Exception {
        final String json = IOUtils.getInstance().getResourceAsString("requestFull.json");
        RequestConverter converter = new RequestConverter();
        RequestPOJO request = converter.convertRequest(json);

        assert null != request;
        assert null != request.getRtQueryLoadSpecification();
        assert null != request.getRtUpdateLoadSpecification();

        //test it autorun pojo
        AutorunPOJO itAutorunPojo = request.getItAutorunSpecification();
        assertNotNull(itAutorunPojo);
        assertTrue(40000 == itAutorunPojo.getQueryProcessAddPeriodMS());
        assertTrue(8 == itAutorunPojo.getQueryProcessesLimit());
        assertTrue(2 == itAutorunPojo.getQueryProcessesBatchSize());
        assertTrue(100 == itAutorunPojo.getUpdateProcessAddPeriodMS());
        assertTrue(5 == itAutorunPojo.getUpdateProcessesLimit());
        assertTrue(1 == itAutorunPojo.getUpdateProcessesBatchSize());
        assertTrue(360000 == itAutorunPojo.getTimeoutMS());

        //test rt autorun pojo
        AutorunPOJO rtAutorunPojo = request.getRtAutorunSpecification();
        assertNotNull(rtAutorunPojo);
        assertTrue(40001 == rtAutorunPojo.getQueryProcessAddPeriodMS());
        assertTrue(6 == rtAutorunPojo.getQueryProcessesLimit());
        assertTrue(3 == rtAutorunPojo.getQueryProcessesBatchSize());
        assertTrue(101 == rtAutorunPojo.getUpdateProcessAddPeriodMS());
        assertTrue(18 == rtAutorunPojo.getUpdateProcessesLimit());
        assertTrue(9 == rtAutorunPojo.getUpdateProcessesBatchSize());
        assertTrue(360001 == rtAutorunPojo.getTimeoutMS());

        /* *******************************************************************************************
        *********************************************************************************************/

        /*
         * test it query load pojo
         */
        LoadPOJO itQueryLoadPOJO = request.getItQueryLoadSpecification();
        assertNotNull(itQueryLoadPOJO);

        //test it query load jdbc
        JdbcPOJO itQueryJdbcPojo = itQueryLoadPOJO.getJdbcPOJO();
        assertNotNull(itQueryJdbcPojo);
        assertEquals("itqueryjdbcdriver", itQueryJdbcPojo.getDriver());
        assertEquals("itqueryjdbcserver", itQueryJdbcPojo.getHost());
        assertEquals(30015, itQueryJdbcPojo.getPort());
        assertEquals("itqueryjdbcuser", itQueryJdbcPojo.getUser());
        assertEquals("itqueryjdbcpassword", itQueryJdbcPojo.getPassword());

        // test it query load general parameters
        assertTrue(itQueryLoadPOJO.isRoundRobin());
        assertEquals(3333, itQueryLoadPOJO.getExecutionTimeSeconds());
        assertEquals(5555, itQueryLoadPOJO.getConcurrentExecutors());
        assertEquals("itqueryfileencoding", itQueryLoadPOJO.getFileEncoding());
        assertEquals("itqueryfilename", itQueryLoadPOJO.getFile());
        assertEquals("itQueryType", itQueryLoadPOJO.getQueryType());
        assertEquals(1111, itQueryLoadPOJO.getQueriesPerInterval());
        assertEquals(2222, itQueryLoadPOJO.getSchedulerIntervalMS());

        /*
         * test it update load pojo
         */
        LoadPOJO itUpdateLoadPOJO = request.getItUpdateLoadUpdateSpecification();
        assertNotNull(itUpdateLoadPOJO);

        //test it update load jdbc
        JdbcPOJO itUpdateJdbcPojo = itUpdateLoadPOJO.getJdbcPOJO();
        assertNotNull(itUpdateJdbcPojo);
        assertEquals("itupdatejdbcdriver", itUpdateJdbcPojo.getDriver());
        assertEquals("itupdatejdbcserver", itUpdateJdbcPojo.getHost());
        assertEquals(30115, itUpdateJdbcPojo.getPort());
        assertEquals("itupdatejdbcuser", itUpdateJdbcPojo.getUser());
        assertEquals("itupdatejdbcpassword", itUpdateJdbcPojo.getPassword());

        // test it update load general parameters
        assertFalse(itUpdateLoadPOJO.isRoundRobin());
        assertEquals(6666, itUpdateLoadPOJO.getExecutionTimeSeconds());
        assertEquals(7777, itUpdateLoadPOJO.getConcurrentExecutors());
        assertEquals("itupdatefileencoding", itUpdateLoadPOJO.getFileEncoding());
        assertEquals("itupdatefilename", itUpdateLoadPOJO.getFile());
        assertEquals("itUpdateQueryType", itUpdateLoadPOJO.getQueryType());
        assertEquals(8888, itUpdateLoadPOJO.getQueriesPerInterval());
        assertEquals(9999, itUpdateLoadPOJO.getSchedulerIntervalMS());

        /*
         * test it monitor parameters
         */
        MonitorPOJO itMonitorPojo = request.getItMonitorSpecification();
        assertNotNull(itMonitorPojo);

        //test it monitor jdbc
        JdbcPOJO itMonitorJdbc = itMonitorPojo.getJdbcPOJO();
        assertNotNull(itMonitorJdbc);
        assertEquals("itmonitordriver", itMonitorJdbc.getDriver());
        assertEquals("itmonitorhost", itMonitorJdbc.getHost());
        assertEquals(30215, itMonitorJdbc.getPort());
        assertEquals("itmonitoruser", itMonitorJdbc.getUser());
        assertEquals("itmonitorpassword", itMonitorJdbc.getPassword());

        //test it monitor general parameters
        assertEquals("itmonitorfile", itMonitorPojo.getOutfile());
        assertEquals(111, itMonitorPojo.getMonitorAgeSeconds());
        assertEquals(222, itMonitorPojo.getMonitorQueryIntervalMS());
        assertEquals(333, itMonitorPojo.getPowerConsumtionMonitorDurationMS());
        assertEquals(444, itMonitorPojo.getExpensiveStatementsDurationMS());
        assertEquals(555, itMonitorPojo.getPowerConcumtionMonitorPort());
        assertEquals(666, itMonitorPojo.getTopXValues());

        /* *******************************************************************************************
        *********************************************************************************************/
        /*
         * test it query load pojo
         */
        LoadPOJO rtQueryLoadPOJO = request.getRtQueryLoadSpecification();
        assertNotNull(rtQueryLoadPOJO);

        //test it query load jdbc
        JdbcPOJO rtQueryJdbcPojo = rtQueryLoadPOJO.getJdbcPOJO();
        assertNotNull(rtQueryJdbcPojo);
        assertEquals("rtqueryjdbcdriver", rtQueryJdbcPojo.getDriver());
        assertEquals("rtqueryjdbcserver", rtQueryJdbcPojo.getHost());
        assertEquals(40015, rtQueryJdbcPojo.getPort());
        assertEquals("rtqueryjdbcuser", rtQueryJdbcPojo.getUser());
        assertEquals("rtqueryjdbcpassword", rtQueryJdbcPojo.getPassword());

        // test it query load general parameters
        assertTrue(rtQueryLoadPOJO.isRoundRobin());
        assertEquals(33, rtQueryLoadPOJO.getExecutionTimeSeconds());
        assertEquals(55, rtQueryLoadPOJO.getConcurrentExecutors());
        assertEquals("rtqueryfileencoding", rtQueryLoadPOJO.getFileEncoding());
        assertEquals("rtqueryfilename", rtQueryLoadPOJO.getFile());
        assertEquals("rtQueryType", rtQueryLoadPOJO.getQueryType());
        assertEquals(11, rtQueryLoadPOJO.getQueriesPerInterval());
        assertEquals(22, rtQueryLoadPOJO.getSchedulerIntervalMS());

        /*
         * test it update load pojo
         */
        LoadPOJO rtUpdateLoadPOJO = request.getRtUpdateLoadSpecification();
        assertNotNull(rtUpdateLoadPOJO);

        //test it update load jdbc
        JdbcPOJO rtUpdateJdbcPojo = rtUpdateLoadPOJO.getJdbcPOJO();
        assertNotNull(rtUpdateJdbcPojo);
        assertEquals("rtupdatejdbcdriver", rtUpdateJdbcPojo.getDriver());
        assertEquals("rtupdatejdbcserver", rtUpdateJdbcPojo.getHost());
        assertEquals(40115, rtUpdateJdbcPojo.getPort());
        assertEquals("rtupdatejdbcuser", rtUpdateJdbcPojo.getUser());
        assertEquals("rtupdatejdbcpassword", rtUpdateJdbcPojo.getPassword());

        // test it update load general parameters
        assertFalse(rtUpdateLoadPOJO.isRoundRobin());
        assertEquals(66, rtUpdateLoadPOJO.getExecutionTimeSeconds());
        assertEquals(77, rtUpdateLoadPOJO.getConcurrentExecutors());
        assertEquals("rtupdatefileencoding", rtUpdateLoadPOJO.getFileEncoding());
        assertEquals("rtupdatefilename", rtUpdateLoadPOJO.getFile());
        assertEquals("rtUpdateQueryType", rtUpdateLoadPOJO.getQueryType());
        assertEquals(88, rtUpdateLoadPOJO.getQueriesPerInterval());
        assertEquals(99, rtUpdateLoadPOJO.getSchedulerIntervalMS());

        /*
         * test it monitor parameters
         */
        MonitorPOJO rtMonitorPojo = request.getRtMonitorSpecification();
        assertNotNull(rtMonitorPojo);

        //test it monitor jdbc
        JdbcPOJO rtMonitorJdbc = rtMonitorPojo.getJdbcPOJO();
        assertNotNull(rtMonitorJdbc);
        assertEquals("rtmonitordriver", rtMonitorJdbc.getDriver());
        assertEquals("rtmonitorhost", rtMonitorJdbc.getHost());
        assertEquals(40215, rtMonitorJdbc.getPort());
        assertEquals("rtmonitoruser", rtMonitorJdbc.getUser());
        assertEquals("rtmonitorpassword", rtMonitorJdbc.getPassword());

        //test it monitor general parameters
        assertEquals("rtmonitorfile", rtMonitorPojo.getOutfile());
        assertEquals(11111, rtMonitorPojo.getMonitorAgeSeconds());
        assertEquals(22222, rtMonitorPojo.getMonitorQueryIntervalMS());
        assertEquals(33333, rtMonitorPojo.getPowerConsumtionMonitorDurationMS());
        assertEquals(44444, rtMonitorPojo.getExpensiveStatementsDurationMS());
        assertEquals(55555, rtMonitorPojo.getPowerConcumtionMonitorPort());
        assertEquals(66666, rtMonitorPojo.getTopXValues());
    }

}