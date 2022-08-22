package org.kr.intp.util.db.load;

/**
 * Created by kr on 31.03.2014.
 */
public class DBMSLoadInfo {

    private int cpu;
    private int memory;

    public DBMSLoadInfo(int cpu, int memory) {
        this.cpu = cpu;
        this.memory = memory;
    }

    public int getCpu() {
        return cpu;
    }

    public int getMemory() {
        return memory;
    }
}
