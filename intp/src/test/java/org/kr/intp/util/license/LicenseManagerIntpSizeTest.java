package org.kr.intp.util.license;

import org.kr.intp.IntpServerInfo;
import org.kr.intp.application.agent.IServer;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.config.IntpFileConfig;
import org.kr.intp.util.crypt.LicenseCrypter;
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

import static org.mockito.Mockito.mock;

/**
 * Created by kr on 31.03.2014.
 */
public class LicenseManagerIntpSizeTest extends TestCase {

    private final IntpConfig config = new IntpConfig(IntpFileConfig.getResourceInstance().getPropertiesMap());
    private final String instanceId = config.getIntpInstanceId();
    private final String name = config.getIntpName();
    private final char type = config.getIntpType();
    private final int port = config.getIntpPort();
    private final int intpSize = config.getIntpSize();

    private IntpServerInfo serverInfo = new IntpServerInfo(instanceId, name, type, port, intpSize);
    // IntpServerInfo("002", "D_PCS_Report_App", 'D', 4443, 100000);

    public void testLicenseGenerator() throws IOException, SQLException {
        generateLicense();
    }

    public void testRun() throws Exception {
        IServer server = mock(IServer.class);
        final LicenseManager licenseManager = LicenseManager.newInstance(server, serverInfo);
        licenseManager.testCheckLicense();
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
            }
            data.put("03", serverInfo.getInstanceId());
            data.put("04", String.valueOf(serverInfo.getIntpSize()));
            data.put("05", String.valueOf(serverInfo.getType()));
            data.put("06", serverInfo.getName());
            data.put("07", "1609282800000");
            data.put("08", "10");
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
