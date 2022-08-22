package org.kr.intp.application.job;

import org.kr.intp.application.pojo.job.ProjectTableKey;

import java.util.*;

public class KeysSorter {

    private final ProjectTableKey[] keysDefinitions;

    public KeysSorter(ProjectTableKey[] keysDefinitions) {
        this.keysDefinitions = keysDefinitions;
    }

    public Map<String, List<Object[]>> sort(List<Object[]> keys) {
        TreeMap<String, List<Object[]>> sortedKeys = new TreeMap<String, List<Object[]>>();

        //prepare keys
        for (Object[] k : keys) {
            StringBuilder kb = new StringBuilder();
            for (int i = 0; i < keysDefinitions.length; i++) {
                if (keysDefinitions[i].isSequential())
                    kb.append(k[i]);
            }
            final String skey = kb.toString();
            if (sortedKeys.containsKey(skey))
                sortedKeys.get(skey).add(k);
            else
                sortedKeys.put(skey, new ArrayList<Object[]>(Collections.singletonList(k)));
        }
        return sortedKeys;
    }
}
