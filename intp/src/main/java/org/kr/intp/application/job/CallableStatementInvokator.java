package org.kr.intp.application.job;

import org.kr.intp.util.retry.RetryingInvokable;

import javax.sql.DataSource;
import java.sql.CallableStatement;
import java.sql.Connection;

public class CallableStatementInvokator implements RetryingInvokable<Boolean> {

    private final DataSource dataSource;
    private final String sql;
    private final Object[] args;

    public CallableStatementInvokator(DataSource dataSource, String sql, Object... args) {
        this.dataSource = dataSource;
        this.sql = sql;
        this.args = args;
    }

    @Override
    public Boolean invoke() throws Exception {
        Connection connection = dataSource.getConnection();
        CallableStatement statement = connection.prepareCall(sql);

        for (int i = 0; i < args.length; i++) {
            statement.setObject(i + 1, args[i]);
        }

        return statement.execute();
    }
}
