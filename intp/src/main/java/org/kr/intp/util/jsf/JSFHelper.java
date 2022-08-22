package org.kr.intp.util.jsf;

import javax.ejb.Stateless;
import javax.faces.application.FacesMessage;
import javax.faces.context.FacesContext;

/**
 */
@Stateless
public class JSFHelper {

    /**
     * This method reads a message string from a resource bundle and adds a {@link javax.faces.application.FacesMessage}
     * to the {@link javax.faces.context.FacesContext}.
     * @param resourceBundle the resource bundle name as specified under <var>name</var> in the faces-config.xml.
     * @param messageKey the message key.
     * @param severity the severity level.
     * @return <code>true</code> if message added.
     */
    public boolean addFacesMessageFromResourceBundle(String resourceBundle, String messageKey, FacesMessage.Severity severity) {
        String messageContent = FacesContext.getCurrentInstance().getApplication().
                getResourceBundle(FacesContext.getCurrentInstance(), resourceBundle).getString(messageKey);
        FacesMessage message = new FacesMessage(severity,
                messageContent, messageContent);
        FacesContext.getCurrentInstance().addMessage(null, message);
        return true;
    }

}
