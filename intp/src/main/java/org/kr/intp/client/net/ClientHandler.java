package org.kr.intp.client.net;

import org.kr.intp.client.net.protocol.IIntpMessage;
import org.kr.intp.client.net.protocol.ShutdownMessage;
import org.kr.intp.client.net.protocol.StatusMessage;
import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.net.Socket;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 24.09.13
 * Time: 13:46
 * To change this template use File | Settings | File Templates.
 */
public class ClientHandler {

    private static final Logger staticLogger = LoggerFactory.getLogger(ClientHandler.class);
    private static DocumentBuilder docBilder;

    private final Logger log = LoggerFactory.getLogger(ClientHandler.class);
    private final Socket clientSocket;

    static {
        DocumentBuilderFactory docBuildFactory = DocumentBuilderFactory.newInstance();
        try {
            docBilder = docBuildFactory.newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            staticLogger.error(e.getMessage(), e);
        }
    }

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    public IIntpMessage getMessage() throws IOException, SAXException, XPathExpressionException {
        try {
            return parseMessage();
        } finally {
            close();
        }
    }

    private IIntpMessage parseMessage() throws IOException, SAXException, XPathExpressionException {
        Document document = docBilder.parse(clientSocket.getInputStream());
        //TODO: use JAXB marshaller
        XPathFactory xPathFactory = XPathFactory.newInstance();
        XPath xPath = xPathFactory.newXPath();
        Element element = (Element)xPath.evaluate("/intp", document, XPathConstants.NODE);
        if (null == element) {
            log.debug("Not INTP message");
            throw new UnsupportedOperationException();
        }
        String type = element.getAttribute("type").toLowerCase();
        if (type.equals("shutdown"))
            return new ShutdownMessage(document);
        else if (type.equals("status"))
            return new StatusMessage(document);
        else
            throw new UnsupportedOperationException("wrong message type: " + type);
    }

    public void close() {
        log.debug("closing ClientHandler");
        if (null != clientSocket) {
            try {
                clientSocket.close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

}
