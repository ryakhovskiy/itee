package org.kr.db.loader.ui.utils;

import junit.framework.TestCase;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class IOUtilsTest extends TestCase {

    private static final String FILE = "test_file";

    public void testSaveBinaryLines() throws Exception {
        final File file = new File(FILE);
        file.deleteOnExit();
        final List<String> data = new ArrayList<String>();
        data.add("Line1");
        data.add("Line2");
        data.add("Line3");
        IOUtils.getInstance().saveBinaryLines(FILE, data);
    }
}