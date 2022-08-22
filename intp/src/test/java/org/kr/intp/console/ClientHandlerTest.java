package org.kr.intp.console;

import org.kr.intp.IntpTestBase;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 24.09.13
 * Time: 21:12
 * To change this template use File | Settings | File Templates.
 */
public class ClientHandlerTest extends IntpTestBase {

    private static final int SERVER_PORT = 4446;
    private final Logger log = LoggerFactory.getLogger(ClientHandlerTest.class);

    public void testHandleClient() throws Exception {
        /*final ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

        Runnable server = new Runnable() {
            @Override
            public void run() {
                Socket clientSocket = null;
                try {
                    clientSocket = serverSocket.accept();
                    Object returned = new ClientHandler(clientSocket).handleClient();
                    assert null != returned;
                    assert returned instanceof ShutdownMessage;
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                } catch (SAXException e) {
                    log.error(e.getMessage(), e);
                } catch (XPathExpressionException e) {
                    log.error(e.getMessage(), e);
                } finally {
                    if (null != clientSocket)
                        try {
                            clientSocket.close();
                        } catch (IOException e) {
                            log.error(e.getMessage(), e);
                        }
                }
            }
        };
        Thread serverThread = new Thread(server);
        serverThread.start();   */
        Socket socket = null;
        try {
            socket = new Socket("localhost", SERVER_PORT);
            PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));
            String message = "<intp type='shutdown' />";
            writer.print(message);
            writer.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            if (null != socket)
                try {
                    socket.close();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
        }
        //serverThread.join();
    }
}
