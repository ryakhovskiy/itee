package org.kr.intp.application.job.scheduler;

import org.kr.intp.application.AppContext;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.TimeController;
import org.kr.intp.util.db.pool.ServiceConnectionPool;

import java.sql.*;
import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created by kr on 4/10/2015.
 */
public class CustomCalendar implements Runnable {

    private static final CustomCalendar instance = new CustomCalendar();

    public static CustomCalendar getInstance() {
        return instance;
    }

    private final Logger log = LoggerFactory.getLogger(CustomCalendar.class);
    private final Map<String, Map<String, Boolean>> calendar = new HashMap<>();
    private final Object initializationSynchronizer = new Object();
    private final IntpConfig config = AppContext.instance().getConfiguration();

    private CustomCalendar() {
        try {
            initialize();
        } catch (SQLException e) {
            final String message = "Cannot initialize Custom Calendar based on a Calendar Table: " + e.getMessage();
            log.error(message, e);
            throw new RuntimeException(message, e);
        }
    }

    private void initialize() throws SQLException {
        synchronized (initializationSynchronizer) {
            log.debug("initializing calendar");
            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;
            try {
                connection = ServiceConnectionPool.instance().getConnection();
                String sql = "select column_name from table_columns where schema_name = ? and table_name = ?" +
                        " and column_name like 'IS%'";
                statement = connection.prepareStatement(sql);
                statement.setString(1, config.getFiscalCalendarSchema());
                statement.setString(2, config.getFiscalCalendarTable());
                resultSet = statement.executeQuery();
                List<String> flags = new ArrayList<String>();
                while (resultSet.next())
                    flags.add(resultSet.getString(1));
                StringBuilder sqlBuilder = new StringBuilder();
                sqlBuilder.append("select calendardate,");
                for (String col : flags)
                    sqlBuilder.append(col).append(',');
                sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
                sqlBuilder.append(" from ").append(config.getFiscalCalendarSchema()).append('.');
                sqlBuilder.append(config.getFiscalCalendarTable());
                sqlBuilder.append(" where calendaryear = ? and calendarmonth = ?");
                statement.close();
                statement = connection.prepareStatement(sqlBuilder.toString());
                resultSet.close();
                statement.setInt(1, Calendar.getInstance().get(Calendar.YEAR));
                statement.setInt(2, Calendar.getInstance().get(Calendar.MONTH) + 1);
                resultSet = statement.executeQuery();

                ResultSetMetaData rsmd = resultSet.getMetaData();
                final int cols = rsmd.getColumnCount();
                final List<String> colNames = new ArrayList<String>();
                for (int i = 1; i <= cols; i++)
                    colNames.add(rsmd.getColumnName(i));
                while (resultSet.next()) {
                    Date date = resultSet.getDate(1);
                    Map<String, Boolean> fs = new HashMap<String, Boolean>();
                    for (String col : colNames) {
                        if (col.equals("calendardate".toUpperCase()))
                            continue;
                        Integer flag = resultSet.getInt(col);
                        boolean enabled = (flag != null && flag > 0);
                        fs.put(col, enabled);
                    }
                    synchronized (calendar) {
                        calendar.put(date.toString(), fs);
                    }
                }
            } finally {
                if (null != resultSet)
                    resultSet.close();
                if (null != statement)
                    statement.close();
                if (null != connection)
                    connection.close();
            }
        }
    }

    public boolean isCurrentDateFlagged(Map<String, Boolean> requiredFlags) {
        final Date now = new Date(TimeController.getInstance().getServerUtcTimeMillis());
        Map<String, Boolean> flags = null;
        synchronized (calendar) {
            flags = calendar.get(now.toString());
        }
        for (Map.Entry<String, Boolean> e : requiredFlags.entrySet()) {
            String requiredFlagName = e.getKey();
            boolean requiredFlagEnabled = e.getValue() == null ? false : e.getValue();
            if (requiredFlagEnabled) {
                boolean calendarFlagEnabled = flags.get(requiredFlagName) == null ? false : flags.get(requiredFlagName);
                if (calendarFlagEnabled)
                    return true;
            }
        }
        return false;
    }

    public void run() {
        try {
            boolean initRequired = false;
            synchronized (calendar) {
                if (calendar.size() == 0)
                    initRequired = true;
            }
            if (initRequired) {
                initialize();
                return;
            }
            String sdate = null;
            synchronized (calendar) {
                sdate = calendar.keySet().iterator().next();
            }
            DateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            java.util.Date date = simpleDateFormat.parse(sdate);
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            int month = c.get(Calendar.MONTH);
            initRequired = month != Calendar.getInstance().get(Calendar.MONTH);
            if (initRequired)
                initialize();
        } catch (SQLException | ParseException e) {
            log.error(e.getMessage(), e);
        }
    }
}
