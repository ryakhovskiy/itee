package org.kr.intp.client.net;

import java.io.IOException;

/**
 * Created by kr on 1/17/14.
 */
public class SocketListenerException extends IOException {

    public SocketListenerException() {

    }

    public SocketListenerException(Throwable cause) {
        super(cause);
    }

    public SocketListenerException(String message) {
        super(message);
    }

    public SocketListenerException(String message, Throwable cause) {
        super(message, cause);
    }

}
