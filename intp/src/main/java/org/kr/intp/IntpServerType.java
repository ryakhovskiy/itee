package org.kr.intp;

/**
 * Created with IntelliJ IDEA.
 * User: kr
 * Date: 17.11.13
 * Time: 22:09
 * To change this template use File | Settings | File Templates.
 */
public enum IntpServerType {

    DEVELOPMENT('D'),
    QUALITY('Q'),
    TEST('T'),
    PRODUCTION('P');

    private final char type;

    IntpServerType(char type) {
        this.type = type;
    }

}
