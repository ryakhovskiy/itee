package org.kr.intp.application.pojo.job;

import org.kr.intp.config.IntpFileConfig;
import org.kr.intp.util.db.TimeController;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class JobPropertiesTest extends TestCase {

    private static final String SCHEMA = IntpFileConfig.getResourceInstance().getIntpSchema();

    private static final String PROJECT_ID = "AUTOTST" + TimeController.getInstance().getServerUtcTimeMillis();
    private static final int VERSION = 0;

    protected static final String props_full = "{\n" +
            "  \"initial\": {\"startdate\": \"2014-07-09 12:00:00\", \"enddate\": \"2014-12-09 12:00:00\", \"period\": 1000, \"executors\": 10, \"fullload\": true, \"flrestrictions\": \"spmon = '201212'\"},\n" +
            "  \"delta\": {\"startdate\": \"2014-07-09 12:01:00\", \"enddate\": \"2014-12-09 12:30:00\", \"period\": 10, \"executors\": 10},\n" +
            "  \"sla\": 20\n" +
            "}";

    protected static final String props_wo_enddate = "{\n" +
            "  \"initial\": {\"startdate\": \"2014-07-09 12:00:00\", \"period\": 1000, \"executors\": 10, \"fullload\": true},\n" +
            "  \"delta\": {\"startdate\": \"2014-07-09 12:01:00\", \"period\": 10, \"executors\": 10},\n" +
            "  \"sla\": 20\n" +
            "}";

    protected static final String props_wo_startdata = "{\n" +
            "  \"initial\": {\"enddate\": \"2014-12-09 12:00:00\", \"period\": 1000, \"executors\": 10, \"fullload\": true},\n" +
            "  \"delta\": {\"enddate\": \"2014-12-09 12:30:00\", \"period\": 10, \"executors\": 10},\n" +
            "  \"sla\": 20\n" +
            "}";

    protected static final String props_wo_dates = "{\n" +
            "  \"initial\": {\"period\": 2000, \"executors\": 3, \"fullload\": true},\n" +
            "  \"delta\": {\"period\": 20, \"executors\": 3},\n" +
            "  \"sla\": 20\n" +
            "}";

    protected  static final String props_calendar = "{\"initial\":{\"calendar\":{\"firstdayofcalendaryear\":true,\"lastdayofcalendaryear\":true,\"firstdayoffiscalyear\":true,\"lastdayoffiscalyear\":true,\"firstdayofcalendarmonth\":true,\"lastdayofcalendarmonth\":true,\"firstdayofaccountingperiod\":true,\"lastdayofaccountingperiod\":true,\"firstdayofpayrollperiod\":true,\"lastdayofpayrollperiod\":true,\"firstdayofbiweeklyperiod\":true,\"lastdayofbiweeklyperiod\":true},\"startdate\":\"2015-04-01 10:00:00\",\"enddate\":\"2015-04-02 11:00:00\",\"executors\":19,\"period\":72000,\"fullload\":true,\"flrestrictions\":\"\",\"scheduling\":\"PERIOD\",\"periodchoice\":\"MONTH\"},\"delta\":{\"calendar\":{\"firstdayofcalendaryear\":true,\"lastdayofcalendaryear\":false,\"firstdayoffiscalyear\":true,\"lastdayoffiscalyear\":false,\"firstdayofcalendarmonth\":false,\"lastdayofcalendarmonth\":true,\"firstdayofaccountingperiod\":false,\"lastdayofaccountingperiod\":true,\"firstdayofpayrollperiod\":true,\"lastdayofpayrollperiod\":false,\"firstdayofbiweeklyperiod\":false,\"lastdayofbiweeklyperiod\":true},\"startdate\":\"2015-04-06 12:00:00\",\"enddate\":\"2015-04-10 13:00:00\",\"executors\":30,\"period\":200,\"scheduling\":\"CALENDAR\",\"periodchoice\":\"SECONDS\"},\"sla\":20}";

    @Override
    protected void setUp() throws SQLException {

    }

    @Override
    protected void tearDown() throws SQLException {
        deleteProperties();
    }

    public void testCalendars() throws SQLException, IOException {
        insertProperties(props_calendar);
        JobProperties[] props = JobProperties.loadProperties(PROJECT_ID, VERSION);

        JobProperties initial = props[0];
        JobProperties delta = props[1];

        System.out.println("Initial: " + initial);
        System.out.println("Delta:   " + delta);

        assert initial.getStartdate() > 0 : "StartDate has not been parsed";
        assert initial.getEnddate() > 0 : "EndDate has not been parsed";
        assert initial.isFullload() : "FullLoad has not been parsed";
        assert initial.getPeriod() > 0 : "Period has not been parsed";
        assert initial.getProcessExecutors() > 0 : "ProcessExecutors has not been parsed";
    }

    @Test
    public void testDbProps() {

    }

    public void testFullProperties() throws Exception {
        insertProperties(props_full);
        JobProperties[] props = JobProperties.loadProperties(PROJECT_ID, VERSION);

        JobProperties initial = props[0];
        JobProperties delta = props[1];

        System.out.println("Initial: " + initial);
        System.out.println("Delta:   " + delta);

        assert initial.getStartdate() > 0 : "StartDate has not been parsed";
        assert initial.getEnddate() > 0 : "EndDate has not been parsed";
        assert initial.isFullload() : "FullLoad has not been parsed";
        assert initial.getPeriod() > 0 : "Period has not been parsed";
        assert initial.getProcessExecutors() > 0 : "ProcessExecutors has not been parsed";
        assert initial.getFullLoadRestrictions().length() > 0 : "FullLoad Restrictions have not been parsed";
    }

    private void insertProperties(String properies) throws SQLException {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            final String query = "insert into " + SCHEMA + ".RT_PARAMETERS values (?,?,CURRENT_UTCTIMESTAMP,?)";
            statement = connection.prepareStatement(query);
            statement.setString(1, PROJECT_ID);
            statement.setInt(2, VERSION);
            statement.setString(3, properies);
            statement.executeUpdate();
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private void deleteProperties() throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            statement = connection.createStatement();
            statement.executeUpdate("delete from " + SCHEMA + ".RT_PARAMETERS where PROJECT_ID = '" + PROJECT_ID + "'");
        } finally {
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }
}