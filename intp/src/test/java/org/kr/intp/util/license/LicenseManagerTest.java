package org.kr.intp.util.license;

import org.kr.intp.IntpServerInfo;
import org.kr.intp.application.agent.IServer;
import org.kr.intp.util.crypt.LicenseCrypter;
import org.kr.intp.util.db.TimeController;
import org.kr.intp.util.db.pool.ServiceConnectionPool;
import junit.framework.TestCase;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

/**
 * Created by kr on 31.03.2014.
 */
public class LicenseManagerTest extends TestCase {

    private static final IntpServerInfo serverInfo = new IntpServerInfo("000", "", 'D', 4455, 10000);

    public void testRun() throws Exception {
        IServer server = mock(IServer.class);
        final LicenseManager licenseManager = LicenseManager.newInstance(server, serverInfo);
        licenseManager.run();
    }

    public void testLicenseGenerator() throws IOException, SQLException {
        generateLicense();
    }

    public void generateLicense() throws SQLException, IOException {
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        final HashMap<String, String> data = new HashMap<String, String>();
        try {
            connection = ServiceConnectionPool.instance().getConnection();
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("select * from ");
            stringBuilder.append(new String(new byte[]{109, 95, 108, 105, 99, 101, 110, 115, 101}));
            String query = String.format(stringBuilder.toString());
            statement = connection.prepareStatement(query);
            resultSet = statement.executeQuery();
            if (resultSet.next()) {
                data.put("01", resultSet.getString(1));
                data.put("02", resultSet.getString(2));
                data.put("03", resultSet.getString(3));
                data.put("04", resultSet.getString(4));
                data.put("05", resultSet.getString(5));
                data.put("06", resultSet.getString(6));
            }
            data.put("07", serverInfo.getInstanceId());
            data.put("08", String.valueOf(serverInfo.getIntpSize()));
            data.put("09", String.valueOf(serverInfo.getType()));
            data.put("10", serverInfo.getName());
            data.put("11", String.valueOf(TimeController.getInstance().getServerUtcTimeMillis()
                    + TimeUnit.DAYS.toMillis(3)));
            data.put("12", "5");
            System.out.println(generateLicense(data));
        } finally {
            if (null != resultSet)
                resultSet.close();
            if (null != statement)
                statement.close();
            if (null != connection)
                connection.close();
        }
    }

    private String generateLicense(Map<String, String> data) throws IOException {
        final ObjectMapper objectMapper = new ObjectMapper();
        final LicenseCrypter licenseCrypter = LicenseCrypter.newInstance();
        String info = objectMapper.writeValueAsString(data);
        return licenseCrypter.encryptData(info);
    }

}
