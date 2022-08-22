package org.kr.intp.model.pojo;

/**
 */
public enum ServerType {
    D('D'), Q('Q'), S('S'), P('P');

    private final char label;

    ServerType(char label) {
        this.label = label;
    }

    public char getLabel() {
        return label;
    }
}
