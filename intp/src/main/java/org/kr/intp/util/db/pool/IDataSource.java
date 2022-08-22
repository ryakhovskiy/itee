package org.kr.intp.util.db.pool;

import javax.sql.DataSource;

public interface IDataSource extends DataSource {

    void close();

}
