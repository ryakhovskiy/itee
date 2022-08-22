package org.kr.intp.util.io;

import org.kr.intp.IntpTestBase;

import java.io.IOException;

public class QueryReaderTest extends IntpTestBase {

    public void testQueryReader() throws IOException {
        for (QueryReader.Query q : new QueryReader("procedures.sql")) {
            System.out.println(q);
        }
    }

}