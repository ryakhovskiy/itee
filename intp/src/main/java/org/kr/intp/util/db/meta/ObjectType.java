package org.kr.intp.util.db.meta;

/**
 * Created by kr on 7/17/2014.
 */
public enum ObjectType {

    PROCEDURE('P'),
    TABLE('T'),
    VIEW('V'),
    INDEX('I'),
    SEQUENCE('S'),
    STATEMENT('C'),
    TRIGGER('R');

    private final char type;

    ObjectType(char type) {
        this.type = type;
    }

    public char getType() { return type; }

    public String getTypeAsString() { return String.valueOf(type); }

}
