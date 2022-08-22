package org.kr.itee.perfmon.ws.monitor;

import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

/**
 * Created bykron 13.01.2015.
 */
public class PowerConsumptionMonitor implements IMonitor {

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF8");
    private static final long TIMEOUT_MS = 5000;

    private final Logger log = Logger.getLogger(PowerConsumptionMonitor.class);
    private final String server;
    private final int port;
    private final int duration_s;
    private final Object sync = new Object();
    private String results = "PWM: No Data Available";
    private boolean done = false;

    public PowerConsumptionMonitor(String server, int port, long duration_ms) {
        this.server = server;
        this.port = port;
        this.duration_s = (int)(duration_ms / 1000);
    }

    @Override
    public void run() {
        log.debug("initializing PWC Monitor...");
        String data = "PWM: No Data Available";
        try {
            data = getDataFromSocket();
        } catch (IOException e) {
            data = "PWM: " + e.getMessage();
            log.error(e);
        } finally {
            synchronized (sync) {
                results = data;
                sync.notifyAll();
                done = true;
            }
        }
    }

    private String getDataFromSocket() throws IOException {
        return getDataFromSocketN();
    }

    private String getDataFromSocketO() throws IOException {
        Socket socket = null;
        BufferedReader reader = null;
        PrintWriter writer = null;
        try {
            socket = new Socket(server, port);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), DEFAULT_CHARSET));
            writer = new PrintWriter (new OutputStreamWriter(socket.getOutputStream(), DEFAULT_CHARSET));

            writer.print("duration:" + duration_s);
            writer.flush();

            writer.print("monitor_go");
            writer.flush();

            StringBuilder response = new StringBuilder();
            String r = null;
            while (null != (r = reader.readLine())) {
                response.append(r);
                if (r.toLowerCase().contains("end of message") ||
                        r.toLowerCase().contains("end_of_message") ||
                        r.toLowerCase().contains("endofmessage"))
                    break;
            }
            return response.toString();
        } finally {
            if (null != reader)
                reader.close();
            if (null != writer)
                writer.close();
            if (null != socket)
                socket.close();
        }
    }

    private String getDataFromSocketN() throws IOException {
        Socket socket = null;
        BufferedReader reader = null;
        PrintWriter writer = null;
        try {
            socket = new Socket(server, port);
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), DEFAULT_CHARSET));
            writer = new PrintWriter (new OutputStreamWriter(socket.getOutputStream(), DEFAULT_CHARSET));

            writer.print("duration:" + duration_s);
            writer.flush();
            String r = reader.readLine(); //r.equals("<BTR>set duration ok\n") == true
            System.out.printf("PCM out:%s%n", r);

            writer.print("mon_start");
            writer.flush();
            r = reader.readLine();
            System.out.printf("PCM out:%s%n", r); //r.equals("<BI>2365\n") == true; //2365 -- msg size.
            final int size = getSize(r);
            CharBuffer charBuffer = CharBuffer.allocate(size);
            int read = 0;
            while (read < size)
                read += reader.read(charBuffer);
            return format(charBuffer);
        } finally {
            if (null != reader)
                reader.close();
            if (null != writer)
                writer.close();
            if (null != socket)
                socket.close();
        }
    }

    private String format(CharBuffer charBuffer) {
        final String prefix = "<BT>";
        String s = new String(charBuffer.array());
        final int firstBT = s.indexOf(prefix);
        if (firstBT >= 0)
            s = s.substring(firstBT + prefix.length());
        final int firstST = s.indexOf("System Time");
        if (firstST >= 0)
            s = s.substring(firstST);
        return s.replace(prefix, "").replace(";;", "");
    }

    private int getSize(String data) {
        if (null == data)
            return 0;
        final String prefix = "<BI>";
        final int indexBI = data.indexOf(prefix);
        if (indexBI >= 0)
            return Integer.parseInt(data.substring(data.indexOf(prefix) + prefix.length()));
        return Integer.parseInt(data.trim());
    }

    @Override
    public List<String> getResults() {
        synchronized (sync) {
            if (!done) {
                try {
                    sync.wait(TIMEOUT_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return Arrays.asList(results.split("\n"));
        }
    }

    @Override
    public void shutdown() {

    }
}
