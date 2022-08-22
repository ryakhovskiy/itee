package org.kr.intp.application.manager;

import org.kr.intp.application.AppContext;
import org.kr.intp.config.IntpConfig;
import org.kr.intp.config.IntpFileConfig;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class DbApplicationManagerSmokeTest {

    @BeforeClass
    public static void setupClass() {
        IntpConfig config = new IntpConfig(IntpFileConfig.getResourceInstance().getPropertiesMap());
        AppContext.instance().setConfiguration(config);
    }

    @Test
    public void stopTest() {

    }

}