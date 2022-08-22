package org.kr.intp.util.db;

/**
 * https://help.sap.com/hana_one/html/_jsql_error_codes.html
 */
public enum SAPSQLErrorCodes {

    TRANSACTION_ROLLED_BACK_INTERNAL_ERROR(129),
    COMMUNICATION_ERROR(1025),
    CONNECTION_FAILURE(1029),
    SEND_ERROR(1030),
    RECEIVE_ERROR(1031);

    private final int code;

    SAPSQLErrorCodes(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
