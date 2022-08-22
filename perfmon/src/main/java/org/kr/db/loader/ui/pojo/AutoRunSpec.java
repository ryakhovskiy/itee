package org.kr.db.loader.ui.pojo;

import java.io.Serializable;

/**
 * Created bykron 30.07.2014.
 */
public class AutoRunSpec implements Serializable {

    private final String specName;
    private final Scenario itScenario;
    private final Scenario rtScenario;
    private final boolean isItEnabled;
    private final boolean isRtEnabled;

    public AutoRunSpec(String name, Scenario itScenario, Scenario rtScenario, boolean isItEnabled, boolean isRtEnabled) {
        this.specName = name;
        this.itScenario = itScenario;
        this.rtScenario = rtScenario;
        this.isItEnabled = isItEnabled;
        this.isRtEnabled = isRtEnabled;
    }

    public String getSpecName() {
        return specName;
    }

    public Scenario getItScenario() {
        return itScenario;
    }

    public Scenario getRtScenario() {
        return rtScenario;
    }

    public boolean isItEnabled() {
        return isItEnabled;
    }

    public boolean isRtEnabled() {
        return isRtEnabled;
    }
}
