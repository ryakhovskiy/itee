package org.kr.intp.util.license;

/**
 * Created by kr on 16.12.13.
 */
public final class LicenseData {

    private final boolean acceptable;
    private final int tables;
    private final long expiration;
    private final int intpSize;

    LicenseData(boolean acceptable, int tables, long expiration, int intpsize) {
        this.acceptable = acceptable;
        this.tables = tables;
        this.expiration = expiration;
        this.intpSize = intpsize;
    }

    public boolean isAcceptable() {
        return acceptable;
    }

    public int getMaxTablesCount() {
        return tables;
    }

    public long getExpirationTime() {
        return expiration;
    }

    public int getIntpSize() {
        return intpSize;
    }

}
