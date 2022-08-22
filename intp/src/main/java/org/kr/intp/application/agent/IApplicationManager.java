package org.kr.intp.application.agent;

import java.io.IOException;
import java.sql.SQLException;

/**
 *
 */
public interface IApplicationManager {

    boolean startApplication(String id) throws SQLException, IOException;
    boolean stopApplication(String id) throws SQLException, IOException;

}
