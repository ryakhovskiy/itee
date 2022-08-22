package org.kr.intp.client.net.protocol;

import org.kr.intp.logging.Logger;
import org.kr.intp.logging.LoggerFactory;
import org.w3c.dom.Document;

import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 9/25/13
 * Time: 11:18 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class IntpMessage implements IIntpMessage {

    private final Logger log = LoggerFactory.getLogger(IntpMessage.class);
    private final Document document;

    public IntpMessage(Document document) {
        this.document = document;
    }

    public String getPayload() {
        String output = "";
        try {
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(document), new StreamResult(writer));
            output = writer.getBuffer().toString().replaceAll("\n|\r", ""); //remove line breaks.
        } catch (TransformerConfigurationException e) {
            log.error(e.getMessage(), e);
        } catch (TransformerException e) {
            log.error(e.getMessage(), e);
        }
        return output;
    }

}
