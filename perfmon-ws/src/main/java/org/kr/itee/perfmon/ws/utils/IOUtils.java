package org.kr.itee.perfmon.ws.utils;

import org.kr.itee.perfmon.ws.Bootstrap;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by kr on 5/15/2016.
 */
public class IOUtils {

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF8");
    private static final String HOME = Bootstrap.getHome();
    private final Logger log = Logger.getLogger(IOUtils.class);

    public String readString(InputStream stream) throws IOException {
        final StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, DEFAULT_CHARSET))) {
            String line;
            while (null != (line = reader.readLine()))
                builder.append(line);
        }
        return builder.toString();
    }

    private static final String FILE_SEPARATOR = System.getProperty("file.separator");
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private static final IOUtils ioutils = new IOUtils();
    private IOUtils() {}
    public static IOUtils getInstance() {
        return ioutils;
    }

    public void copyFiles(String sourceDirName, String destDirName) throws IOException {
        final File sourceDir = new File(sourceDirName);
        final File[] sources = sourceDir.listFiles();
        if (null == sources)
            throw new IOException("Source files error; find files in the source directory: " + sourceDirName);
        for (final File source : sources) {
            final File dest = new File(destDirName + FILE_SEPARATOR + source.getName());
            if (source.isDirectory())
                copyDir(source, dest);
            else
                copyFile(source, dest);
        }
    }

    public void copyDir(File source, File dest) throws IOException {
        final boolean res = dest.mkdir();
        if (!res)
            throw new IOException("Error while copying directory: " + dest.getName());
        copyFiles(source.getAbsolutePath(), dest.getAbsolutePath());
    }

    public void copyFile(File source, File dest) throws IOException {
        try (InputStream is = new FileInputStream(source);
            OutputStream os = new FileOutputStream(dest)) {
            final byte[] buffer = new byte[1024 * 1024];
            int length;
            while ((length = is.read(buffer)) > 0)
                os.write(buffer, 0, length);
        }
    }

    public boolean deleteDir(File dir) throws IOException {
        final File[] files = dir.listFiles();
        if (null != files) { //some JVMs return null for empty dirs
            for (final File f: files) {
                if (f.isDirectory()) {
                    deleteDir(f);
                } else {
                    boolean res = f.delete();
                    if (!res)
                        log.warn("Cannot remove file: " + f.getAbsolutePath());
                }
            }
        }
        return dir.delete();
    }

    public void deleteProcessDir(int id) throws IOException {
        final File dir = Paths.get(HOME, "adbl-" + id).toAbsolutePath().toFile();
        deleteDir(dir);
    }

    public void cleanUpWorkingDir() throws IOException {
        final File workingDir = new File(HOME);
        final File[] files = workingDir.listFiles();
        if (null == files)
            return;
        for (final File dir : files) {
            if (!dir.isDirectory())
                continue;
            if (dir.getName().startsWith("adbl-")) {
                boolean deleted = deleteDir(dir);
                if (!deleted)
                    log.warn("Cannot delete directory: " + dir.getName());
            }
        }
    }

    public void saveLines(String path, List<String> lines) throws FileNotFoundException {
        final File file = new File(path);
        try (PrintWriter writer = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file), DEFAULT_CHARSET))) {
            for (String line : lines)
                writer.println(line);
            writer.flush();
        }
    }

    public void saveBinaryLines(String path, List<String> data) throws Exception {
        final byte[] bytes = getDataBytes(data);
        try (FileOutputStream fos = new FileOutputStream(path)) {
            fos.write(bytes, 0, bytes.length);
            fos.flush();
        }
    }

    private byte[] getDataBytes(List<String> data) throws Exception {
        final StringBuilder dataBuilder = new StringBuilder();
        for (String line : data)
            dataBuilder.append(line).append(LINE_SEPARATOR);
        final byte[] bytes = dataBuilder.toString().getBytes(Charset.forName("UTF8"));
        return Crypter.getCrypter().encrypt(bytes).getBytes("UTF8");
    }

    public String getResourceAsString(String resource) throws IOException {
        InputStream is = null;
        BufferedReader reader = null;
        try {
            is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
            if (null == is)
                throw new IOException("Resource not found: " + resource);
            reader = new BufferedReader(new InputStreamReader(is, DEFAULT_CHARSET));
            String line;
            final StringBuilder data = new StringBuilder();
            while (null != (line = reader.readLine()))
                data.append(line).append(LINE_SEPARATOR);
            return data.toString();
        } finally {
            if (null != is)
                is.close();
            if (null != reader)
                reader.close();
        }
    }

    public static String getDbLoaderJarName(String dir) {
        final String defaultName = "dbloader.jar";
        if (null == dir)
            dir = ".";
        final String[] files = new File(dir).list();
        if (null == files)
            return defaultName;
        for (String file : files) {
            if (file.startsWith("dbloader") && file.endsWith(".jar"))
                return file;
        }
        return defaultName;
    }

}
