package org.kr.intp.application.job.retry;

import org.kr.intp.util.db.SAPSQLErrorCodes;
import org.kr.intp.util.retry.RetryingInvokerBase;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class RetryingSapCallableStatementInvoker extends RetryingInvokerBase {

    private static final int[] ERRORS_TO_BE_RETRIED;

    static {
        SAPSQLErrorCodes[] codes = SAPSQLErrorCodes.values();
        ERRORS_TO_BE_RETRIED = new int[codes.length];
        for (int i = 0; i < codes.length; i++) {
            ERRORS_TO_BE_RETRIED[i] = codes[i].getCode();
        }
        Arrays.sort(ERRORS_TO_BE_RETRIED);
    }

    public RetryingSapCallableStatementInvoker(long pauseTimeMS, long callTimeoutMS) {
        super(pauseTimeMS, callTimeoutMS);
    }

    public RetryingSapCallableStatementInvoker(long pauseTimeMS, long callTimeoutMS, String name) {
        super(pauseTimeMS, callTimeoutMS, name);
    }

    @Override
    public boolean isErrorRecoverable(Throwable e) {
        if (e instanceof TimeoutException || e.getCause() instanceof TimeoutException)
            return true;

        SQLException sqlException;
        if (e instanceof SQLException) {
            sqlException = (SQLException)e;
        } else if (e.getCause() instanceof SQLException) {
            sqlException = (SQLException)e.getCause();
        } else {
            return false;
        }
        int code = sqlException.getErrorCode();
        int idx = Arrays.binarySearch(ERRORS_TO_BE_RETRIED, code);
        return idx >= 0;
    }
}
