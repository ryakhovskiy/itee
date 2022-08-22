package org.kr.itee.perfmon.ws.web;

import org.kr.itee.perfmon.ws.pojo.RequestPOJO;
import org.kr.itee.perfmon.ws.utils.IOUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 *
 */
public class AutorunControllerTest {

    @Test
    public void testStart() throws Exception {
        final String raw = IOUtils.getInstance().getResourceAsString("request.json");
        RequestPOJO request = new RequestConverter().convertRequest(raw);
        AutorunController autorunController = new AutorunController(request);
        autorunController.start();
        Thread.sleep(120000L);
        autorunController.stop();
        Thread.sleep(30000L);
    }

    @Test
    public void testFailedRequestStart() throws Exception {
        final String raw = IOUtils.getInstance().getResourceAsString("fail.json");
        URL url = new URL("http://localhost:4950/perfmon/autorun/start");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");

        OutputStream os = conn.getOutputStream();
        os.write(raw.getBytes());
        os.flush();

        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + conn.getResponseCode());
        }

        BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

        String output;
        System.out.println("Output from Server .... \n");
        while ((output = br.readLine()) != null) {
            System.out.println(output);
        }

        conn.disconnect();
    }

    @Test
    public void testStop() throws Exception {
        final String raw = IOUtils.getInstance().getResourceAsString("fail.json");
        URL url = new URL("http://localhost:4950/perfmon/autorun/stop/1");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setDoOutput(true);
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");

        OutputStream os = conn.getOutputStream();
        os.write(raw.getBytes());
        os.flush();

        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
            throw new RuntimeException("Failed : HTTP error code : "
                    + conn.getResponseCode());
        }

        BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

        String output;
        System.out.println("Output from Server .... \n");
        while ((output = br.readLine()) != null) {
            System.out.println(output);
        }
        conn.disconnect();
    }
}