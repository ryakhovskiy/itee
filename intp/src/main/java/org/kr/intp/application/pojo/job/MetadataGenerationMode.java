package org.kr.intp.application.pojo.job;

/**
 *
 */
public enum MetadataGenerationMode {

    FULL(0),
    SKIP_TRIGGERS(1);

    private final int mode;

    MetadataGenerationMode(int mode) {
        this.mode = mode;
    }

    public int getMode() {
        return mode;
    }

    public static MetadataGenerationMode fromInt(int mode) {
        switch (mode) {
            case 0:
                return FULL;
            case 1:
            default:
                return SKIP_TRIGGERS;
        }
    }
}
