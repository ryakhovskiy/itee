package org.kr.itee.perfmon.ws.os;

import org.kr.itee.perfmon.ws.Bootstrap;
import org.kr.itee.perfmon.ws.conf.ConfigurationManager;
import org.kr.itee.perfmon.ws.utils.IOUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class ProcessController {

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF8");
    private static final String OS = System.getProperty("os.name").toLowerCase();
    private static final boolean IS_WIN = OS.contains("win");
    private static final String CLASS_PATH_SEPARATOR = IS_WIN ? ";" : ":";
    private static final String FILE_SEPARATOR = System.getProperty("file.separator");

    private static final String JAVA_HOME = System.getenv().get("JAVA_HOME");
    private static final String JAVA_EXECUTABLE = Paths.get(JAVA_HOME, "bin", IS_WIN ? "java.exe" : "java").toString();
    private static final String WORK_DIR = Bootstrap.getHome();


    private static final String JAR_NAME = IOUtils.getDbLoaderJarName(WORK_DIR + FILE_SEPARATOR + "adbl");
    private static final String MAIN_CLASS = ConfigurationManager.instance().getSubprocessMainClassName();
    private static final String CLASS_PATH = "\"" + WORK_DIR + FILE_SEPARATOR + "lib" + FILE_SEPARATOR + "*" +
            CLASS_PATH_SEPARATOR + JAR_NAME + "\"";

    private static final String PROPERTIES_FILENAME = "dbloader.properties";
    private static final AtomicInteger counter = new AtomicInteger(1);

    private final Logger log = Logger.getLogger(ProcessController.class);
    private final ReentrantLock processLock = new ReentrantLock();
    private final Map<Integer, Process> processes = new HashMap<>();
    private final boolean isJmsEnabled;
    private final String brokerUrl;

    public ProcessController() throws IOException {
        File java_exe = new File(JAVA_EXECUTABLE);
        if (!java_exe.exists())
            throw new IOException("JAVA_HOME does not point to valid Java installation.");
        IOUtils.getInstance().cleanUpWorkingDir();
        this.isJmsEnabled = ConfigurationManager.instance().isJmsEnabled();
        this.brokerUrl = ConfigurationManager.instance().getBrokerURL();
    }

    public boolean startProcess(Properties properties) throws IOException {
        log.debug("starting new process...");
        boolean result = false;
        String name;
        int c;
        do {
            c = counter.getAndIncrement();
            String dirname = "adbl-" + c;
            name = Paths.get(WORK_DIR, dirname).toString();
            log.info("creating process directory: " + name);
            File f = new File(name);
            result = f.mkdir();
            if (!result)
                log.warn("Cannot create subdirectory for new process: " + name);
            else
                IOUtils.getInstance().copyFiles(Paths.get(WORK_DIR, "adbl").toString(), name);
        } while (!result);
        String subdir = name;
        createPropertiesFile(properties, subdir);
        ProcessBuilder processBuilder = isJmsEnabled ?
                new ProcessBuilder(JAVA_EXECUTABLE, "-cp", CLASS_PATH, MAIN_CLASS, brokerUrl).directory(new File(subdir)) :
                new ProcessBuilder(JAVA_EXECUTABLE, "-cp", CLASS_PATH, MAIN_CLASS).directory(new File(subdir));
        final Process process = processBuilder.start();
        attach(c, process);
        return true;
    }

    private void attach(final int id, final Process process) {
        try {
            processLock.lock();
            processes.put(id, process);
        } finally {
            processLock.unlock();
        }
        Thread t = new Thread(new ProcessMonitor(id, process));
        t.setDaemon(true);
        t.start();
    }

    private void detach(final int id, final Process process) {
        try {
            processLock.lock();
            if (!processes.containsKey(id))
                return;
            processes.remove(id);
            process.destroy();
        } finally {
            processLock.unlock();
        }
    }

    private void destroyAny() {
        try {
            processLock.lock();
            if (0 == processes.size())
                return;
            Map.Entry<Integer, Process> e = processes.entrySet().iterator().next();
            final int id = e.getKey();
            final Process process = processes.remove(id);
            process.destroy();
        } finally {
            processLock.unlock();
        }
    }

    void destroyAll() {
        try {
            processLock.lock();
            for (Map.Entry<Integer, Process> e : processes.entrySet())
                e.getValue().destroy();
            processes.clear();
        } finally {
            processLock.unlock();
        }
    }

    public boolean stopAllProcesses() {
        destroyAll();
        return true;
    }

    public boolean stopProcess() {
        try {
            processLock.lock();
            if (processes.isEmpty())
                return false;
            destroyAny();
        } finally {
            processLock.unlock();
        }
        return true;
    }

    private void createPropertiesFile(Properties properties, String dir) throws IOException {
        final String path = Paths.get(dir, PROPERTIES_FILENAME).toString();
        try (OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(path), DEFAULT_CHARSET)) {
            properties.store(writer, "AUTOMATICALLY CREATED PROPERTIES");
        }
    }

    private class ProcessMonitor implements Runnable {

        private final int id;
        private final Process process;

        public ProcessMonitor(int id, Process process) {
            this.id = id;
            this.process = process;
        }

        public void run() {
            try {
                final Thread is = new Thread(new InputStreamMonitor(process.getInputStream()));
                is.setDaemon(true);
                is.start();
                Thread es = new Thread(new InputStreamMonitor(process.getErrorStream()));
                es.setDaemon(true);
                es.start();
                final int exitcode = process.waitFor();
                log.debug("ExitCode: " + exitcode);
                IOUtils.getInstance().deleteProcessDir(id);
            } catch (InterruptedException e) {
                log.debug("thread has been interrupted");
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                log.error("Cannot cleanup working directory: " + e.getMessage(), e);
            } finally {
                ProcessController.this.detach(id, process);
            }
        }
    }
}
