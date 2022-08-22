package org.kr.intp.model.pojo;

/**
 */
public enum ExecutorOptimizationStrategy {
    AUTO("auto"), FFDAUTO("ffd-auto");

    private final String label;

    private ExecutorOptimizationStrategy(final String label){
        this.label = label;
    }

    @Override
    public String toString() {
        return label;
    }

    public static ExecutorOptimizationStrategy fromLabel(String label) {
        switch (label) {
            case "ffd-auto":
                return FFDAUTO;
            case "auto":
            default:
                return AUTO;
        }
    }

}
