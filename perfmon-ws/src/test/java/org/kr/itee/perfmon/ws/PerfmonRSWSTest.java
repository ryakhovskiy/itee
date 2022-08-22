package org.kr.itee.perfmon.ws;

import org.kr.itee.perfmon.ws.utils.IOUtils;
import org.kr.itee.perfmon.ws.web.rest.PerfmonRSWS;
import org.kr.itee.perfmon.ws.web.server.Server;
import org.apache.log4j.Logger;
import org.junit.Test;
import sun.misc.Unsafe;

import javax.ws.rs.core.MediaType;
import java.io.*;
import java.lang.reflect.Field;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

/**
 *
 */
public class PerfmonRSWSTest {

    private final Logger log = Logger.getLogger(PerfmonRSWS.class);

    @Test
    public void testStart() throws Exception {
        //Server s = startWebService();
        try {
            URL url = new URL("http://localhost:4950/perfmon/autorun/start");
            URLConnection connection = url.openConnection();
            connection.setDoOutput(true);
            connection.setConnectTimeout(50000);
            connection.setReadTimeout(50000);
            connection.setRequestProperty("Content-Type", MediaType.APPLICATION_JSON);

            try (OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream())) {
                String request = IOUtils.getInstance().getResourceAsString("request.json");
                writer.write(request);
            }

            int id;
            try (InputStream inputStream = connection.getInputStream()) {
                String response = readInputStream(inputStream);
                log.info(response);
                id = Integer.parseInt(response);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw e;
            }

            Thread.sleep(90000L);

            url = new URL("http://localhost:4950/perfmon/autorun/stop/" + id);
            HttpURLConnection hconn = (HttpURLConnection)url.openConnection();
            hconn.setRequestMethod("POST");
            hconn.setConnectTimeout(50000);
            hconn.setReadTimeout(50000);
            hconn.setRequestProperty("Content-Type", MediaType.APPLICATION_JSON);

            try (InputStream inputStream = hconn.getInputStream()) {
                String response = readInputStream(inputStream);
                log.info(response);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw e;
            }
        } finally {
            //stopServer(s);
        }
    }

    private Server startWebService() {
        Server s = new Server(4950);
        s.start();
        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return s;
    }

    private void stopServer(Server s) {
        s.stop();
    }

    private String readInputStream(InputStream inputStream) {
        if (null == inputStream) {
            log.error("stream is not initialized: null");
            return "";
        }
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line = null;
            while (null != (line = reader.readLine()))
                builder.append(line);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        return builder.toString();
    }

    @SuppressWarnings("restriction")
    private static Unsafe getUnsafe() throws Exception {
        Field singleoneInstanceField = Unsafe.class.getDeclaredField("theUnsafe");
        singleoneInstanceField.setAccessible(true);
        return (Unsafe) singleoneInstanceField.get(null);
    }
}
