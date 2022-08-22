package org.kr.db.loader.ui;

import org.kr.db.loader.ui.utils.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by kr on 5/17/2014.
 */
public class IOUtilsTest extends TestCaseBase {
    public void testSaveLines() throws Exception {
        File f = new File("test.txt");
        f.deleteOnExit();

        List<String> lines = new ArrayList<String>();
        lines.add("Hello");
        lines.add("World");
        lines.add("!");

        IOUtils.getInstance().saveLines("test.txt", lines);

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(f));
            String line = null;
            while (null != (line = reader.readLine()))
                assert lines.contains(line);
        } finally {
            if (null != reader)
                reader.close();
        }

        f.deleteOnExit();
    }
}
