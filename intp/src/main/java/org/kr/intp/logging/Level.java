package org.kr.intp.logging;

/**
 * Created bykron 23.11.2016.
 */
public enum  Level {

    //custom enum is required, because slf4j supports org.slf4j.Level since 1.7.15,
    // but SAP HANA XSA uses slf4j of lower version

    ERROR("ERROR", 40),
    WARN("WARN", 30),
    INFO("INFO", 20),
    DEBUG("DEBUG", 10),
    TRACE("TRACE", 0);

    private final String label;
    private final int id;

    Level(String label, int id) {
        this.label = label;
        this.id = id;
    }

    @Override
    public String toString() {
        return label;
    }

    public int getId() { return id; }

}
