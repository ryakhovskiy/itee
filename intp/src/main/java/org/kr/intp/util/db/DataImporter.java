package org.kr.intp.util.db;

import org.kr.intp.application.AppContext;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.kr.intp.util.db.meta.DbMetadataManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class DataImporter {

    private static final DataImporter di = new DataImporter();
    private final String schema = AppContext.instance().getConfiguration().getIntpSchema();
    private final Logger log = LoggerFactory.getLogger(DataImporter.class);
    private DataImporter() {}
    public static DataImporter getDataImporter() { return di; }

    public void checkData1(DbMetadataManager metadataManager) throws SQLException, IOException {
        log.debug("checking data...");
        final String basePath = "com/kr/intp/db/data";
        BufferedReader reader = null;
        InputStream is = null;
        try {
            is = this.getClass().getClassLoader().getResourceAsStream(basePath);
            if (null == is) {
                log.error("DataImporter, cannot load base path: " + basePath);
                return;
            }
            reader = new BufferedReader(new InputStreamReader(is));
            log.trace("BufferedReader: " + reader);
            try {
                String line;
                while (null != (line = reader.readLine())) {
                    log.trace("data:" + line);
                    handleDataFile(metadataManager, basePath, line);
                }
            } catch (Exception e) {
                e.printStackTrace();
                e.getCause().printStackTrace();
            }
        } finally {
            if (null != is)
                is.close();
            if (null != reader)
                reader.close();
        }
    }

    public void checkData(DbMetadataManager metadataManager) throws SQLException, IOException {
        final String basePath = "com/kr/intp/db/data";
        handleDataFile(metadataManager, basePath, "rt_status_desc.csv");
        handleDataFile(metadataManager, basePath, "rt_categories.csv");
        handleDataFile(metadataManager, basePath, "rt_groups.csv");
        handleDataFile(metadataManager, basePath, "report_settings.csv");
    }

    private void handleDataFile(DbMetadataManager metadataManager, String basePath, String resource) throws SQLException, IOException {
        log.trace("parsing file: " + resource);
        final int extIndex = resource.indexOf(".csv");
        final String table = resource.substring(0, extIndex).toUpperCase();
        if (null == metadataManager) {
            log.error("Cannot load data: metadataManager is null");
            return;
        }
        if (!metadataManager.isTableExists(table)) {
            log.error("Table: " + table + " does not exists! Cannot insert data");
            return;
        }
        final int records = metadataManager.getRecordsCount(schema, table);
        if (records > 0) {
            log.debug(table + ": skipping data import, table is not empty; records: " + records);
            return;
        }
        final List<Object[]> lines = readCsv(basePath + '/' + resource);
        insertData(metadataManager, lines, table);
    }

    private void insertData(DbMetadataManager metadataManager, List<Object[]> lines, String table) throws SQLException {
        if (0 == lines.size())
            return;
        final int cols = lines.get(0).length;
        if (0 == cols)
            return;
        final StringBuilder sqlBldr = new StringBuilder();
        sqlBldr.append("insert into ").append(schema).append('.').append(table).append(" (");
        final Object[] headers = lines.get(0);
        for (int i = 0; i < cols; i++)
            sqlBldr.append(headers[i]).append(",");
        sqlBldr.deleteCharAt(sqlBldr.length() - 1).append(") values (");
        for (int i = 0; i < cols; i++)
            sqlBldr.append("?,");
        sqlBldr.deleteCharAt(sqlBldr.length() - 1).append(")");
        final String sql = sqlBldr.toString();
        final int id = metadataManager.prepareSql(sql);
        for (int i = 1; i < lines.size(); i++)
            metadataManager.executePrepared(id, lines.get(i));
        metadataManager.closePrepared(id);
    }

    private List<Object[]> readCsv(String resource) throws IOException {
        final List<Object[]> lines = new ArrayList<>();
        BufferedReader reader = null;
        InputStream is = null;
        try {
            is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
            if (null == is) {
                log.error("DataImporter, cannot load file: " + resource);
                return new ArrayList<>();
            }
            reader = new BufferedReader(new InputStreamReader(is));
            String line;
            while (null != (line = reader.readLine())) {
                Scanner scanner = new Scanner(line);
                scanner.useDelimiter("\t");
                List<Object> cols = new ArrayList<Object>();
                while (scanner.hasNext()) {
                    if (scanner.hasNextInt())
                        cols.add(scanner.nextInt());
                    else if (scanner.hasNextDouble())
                        cols.add(scanner.nextDouble());
                    else
                        cols.add(scanner.next());
                }
                lines.add(cols.toArray());
            }
            return lines;
        } finally {
            if (null != is)
                is.close();
            if (null != reader)
                reader.close();
        }
    }
}
