package org.kr.db.loader.ui.utils;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * Created by kr on 5/5/2014.
 */
public class IOUtils {

    private static final String FILE_SEPARATOR = System.getProperty("file.separator");
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private static final IOUtils ioutils = new IOUtils();
    private IOUtils() {}
    public static IOUtils getInstance() {
        return ioutils;
    }

    public void copyFiles(String sourceDirName, String destDirName) throws IOException {
        File sourceDir = new File(sourceDirName);
        File[] sources = sourceDir.listFiles();
        if (null == sources)
            throw new IOException("Source files error; find files in the source directory: " + sourceDirName);
        for (File source : sources) {
            File dest = new File(destDirName + FILE_SEPARATOR + source.getName());
            if (source.isDirectory())
                copyDir(source, dest);
            else
                copyFile(source, dest);
        }
    }

    public void copyDir(File source, File dest) throws IOException {
        boolean res = dest.mkdir();
        if (!res)
            throw new IOException("Error while copying directory: " + dest.getName());
        copyFiles(source.getAbsolutePath(), dest.getAbsolutePath());
    }

    public void copyFile(File source, File dest) throws IOException {
        InputStream is = null;
        OutputStream os = null;
        try {
            is = new FileInputStream(source);
            os = new FileOutputStream(dest);
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) > 0)
                os.write(buffer, 0, length);
        } finally {
            if (null != is)
                is.close();
            if (null != os)
                os.close();
        }
    }

    public void deleteDir(File dir) throws IOException {
        File[] files = dir.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteDir(f);
                } else {
                    boolean res = f.delete();
                    if (!res)
                        throw new IOException("Cannot remove file: " + f.getAbsolutePath());
                }
            }
        }
        dir.delete();
    }

    public void cleanUpWorkingDir() throws IOException {
        File currentDir = new File(".");
        final File[] files = currentDir.listFiles();
        if (null == files)
            return;
        for (File dir : files) {
            if (!dir.isDirectory())
                continue;
            if (dir.getName().startsWith("adbl"))
                deleteDir(dir);
        }
    }

    public void saveLines(String path, List<String> lines) throws FileNotFoundException {
        File file = new File(path);
        PrintWriter writer = null;
        try {
            writer = new PrintWriter(file);
            for (String line : lines)
                writer.println(line);
            writer.flush();
        } finally {
            if (null != writer)
                writer.close();
        }
    }

    public void saveBinaryLines(String path, List<String> data) throws Exception {
        Files.write(Paths.get(path), data, Charset.forName("UTF8"), StandardOpenOption.CREATE_NEW);
    }

    public String getResourceAsString(String resource) throws IOException {
        InputStream is = null;
        BufferedReader reader = null;
        try {
            is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
            if (null == is)
                throw new IOException("Resource not found: " + resource);
            reader = new BufferedReader(new InputStreamReader(is));
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
