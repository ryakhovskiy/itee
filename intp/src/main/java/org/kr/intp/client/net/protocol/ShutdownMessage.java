package org.kr.intp.client.net.protocol;

import org.w3c.dom.Document;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 24.09.13
 * Time: 21:01
 * To change this template use File | Settings | File Templates.
 */
public class ShutdownMessage extends IntpMessage {

    public ShutdownMessage(Document document) {
        super(document);
    }
}
