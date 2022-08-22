package org.kr.intp.util.io;

import org.kr.intp.application.AppContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

/**
 * Created by kr on 7/17/2014.
 */
public class QueryReader implements Iterable<QueryReader.Query> {

    private static final String SCHEMA = AppContext.instance().getConfiguration().getIntpSchema();
    private static final String GEN_SCHEMA = AppContext.instance().getConfiguration().getIntpGenObjectsSchema();
    private static final String LINE_SEP = System.getProperty("line.separator");
    private final String text;

    public QueryReader(String resource) throws IOException {
        BufferedReader reader = null;
        InputStream is = null;
        try {
            is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
            if (null == is)
                throw new IOException("Resource not found: " + resource);
            reader = new BufferedReader(new InputStreamReader(is));
            StringBuilder textBuilder = new StringBuilder();
            String nextLine;
            while (null != (nextLine = reader.readLine()))
                textBuilder.append(nextLine).append(LINE_SEP);
            text = textBuilder.toString();
        } finally {
            if (null != is)
                is.close();
            if (null != reader)
                reader.close();
        }
    }

    @Override
    public Iterator<Query> iterator() {
        return new QueryFileIterator();
    }

    class QueryFileIterator implements Iterator<Query> {

        private int currentPosition;

        private QueryFileIterator() {
            currentPosition = text.indexOf("--");
        }

        @Override
        public boolean hasNext() {
            final int nextpos = getNextPosition();
            return nextpos > 0 && nextpos > currentPosition;
        }

        @Override
        public Query next() {
            final int nextpos = getNextPosition();
            final String nexttext = text.substring(currentPosition, nextpos);
            final int queryIndex = nexttext.indexOf(LINE_SEP);
            final String queryMetadata = nexttext.substring(2, queryIndex);
            String[] queryMetadataArray = queryMetadata.split("#");
            if(queryMetadataArray.length<3)
                throw new IllegalArgumentException("The defined query file is not valid.");

            String schema = queryMetadataArray[0];
            String name = queryMetadataArray[1];
            String type = queryMetadataArray[2];
            int version = Integer.parseInt(queryMetadataArray[3]);

            schema = schema.replace("$$SCHEMA$$", SCHEMA).replace("$$GEN_SCHEMA$$", GEN_SCHEMA)
                    .replace("$$VERSION$$", String.valueOf(version));

            String query = nexttext.substring(queryIndex, nexttext.length()).replace("$$SCHEMA$$", SCHEMA).replace("$$GEN_SCHEMA$$", GEN_SCHEMA)
                    .replace("$$VERSION$$", String.valueOf(version)).replace("$$NAME$$", name);
            currentPosition = nextpos;
            return new Query(query, schema, name, type, version);
        }

        private int getNextPosition() {
            final int nextpos = text.indexOf("--", currentPosition + 1);
            if (nextpos > 0)
                return nextpos;
            if (currentPosition < text.length())
                return text.length();
            return currentPosition;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Removing queries is not supported.");
        }
    }

    public class Query {

        private final String query;
        private final String name;
        private final String schema;
        private final String type;
        private final int version;

        public Query(String query, String schema, String name, String type, int version) {
            this.query = query;
            this.schema = schema;
            this.name = name;
            this.type = type;
            this.version = version;
        }

        public int getVersion() { return version; }

        public String getQuery() {
            return query;
        }

        public String getSchema() { return schema; }
        public String getName() {
            return name;
        }

        public String getType() {return type; }

        @Override
        public String toString() {
            return String.format("S: %s; N: %s; T: %s; V: %d; Q: %s", schema, name, type, version, query);
        }
    }
}
