package org.kr.itee.perfmon.ws.os;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

/**
 *
 */
class InputStreamMonitor implements Runnable {

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF8");
    private final Logger log = Logger.getLogger(InputStreamMonitor.class);
    private final InputStream inputStream;

    public InputStreamMonitor(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public void run() {
        try {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, DEFAULT_CHARSET));
            String line;
            try {
                while (null != (line = reader.readLine()))
                    log.trace("SUBPROCESS OUT: " + line);
            } catch (IOException e) {
                log.trace("SUBPROCESS ERR: " + e.getMessage(), e);
            }
        } finally {
            close();
        }
    }

    private void close() {
        if (null == inputStream)
            return;
        try {
            inputStream.close();
        } catch (IOException e) {
            log.error("Error while closing subprocess output");
        }
    }
}

