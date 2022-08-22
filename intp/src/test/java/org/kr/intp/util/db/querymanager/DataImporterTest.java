package org.kr.intp.util.db.querymanager;

import org.kr.intp.util.db.DataImporter;
import junit.framework.TestCase;

/**
 * Created bykron 10.06.2015.
 */
public class DataImporterTest extends TestCase {

    public void testCheckData() throws Exception {
        DataImporter.getDataImporter().checkData(null);
    }
}