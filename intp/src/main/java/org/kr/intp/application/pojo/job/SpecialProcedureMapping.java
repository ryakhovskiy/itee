package org.kr.intp.application.pojo.job;

import java.util.HashMap;
import java.util.Map;

/**
 * Created bykron 20.05.2015.
 */
public class SpecialProcedureMapping {

    private final int keyId;
    private final Map<String, String> mapping = new HashMap<String, String>();

    public SpecialProcedureMapping(int keyId, Map<String, String> mapping) {
        this.keyId = keyId;
        synchronized (mapping) {
            this.mapping.putAll(mapping);
        }
    }

    public int getKeyId() {
        return keyId;
    }

    public Map<String, String> getMapping() {
        return new HashMap<String, String>(mapping);
    }

    public String toString() {
        return String.format("keyId: %s; mapping: %s", keyId, mapping.toString());
    }
}
