package org.kr.db.loader.ui.panels;

import org.kr.db.loader.ui.utils.IOUtils;

import java.io.*;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by kr on 28.04.2014.
 */
public class ProcessController {

    private static final AtomicInteger totalProcessesRunning = new AtomicInteger(0);

    public static int getCurrentProcessesCount() {
        return totalProcessesRunning.get();
    }

    private static final String JAVA_HOME = System.getenv().get("JAVA_HOME");
    private static final String FILE_SEPARATOR = System.getProperty("file.separator");
    private static final String JAVA_EXECUTABLE = JAVA_HOME + FILE_SEPARATOR + "bin" + FILE_SEPARATOR + "java.exe";
    private static final String MAIN_CLASS = "AppMain";
    private static final String WORK_DIR = System.getProperty("user.dir");
    private static final String JAR_NAME = IOUtils.getDbLoaderJarName(WORK_DIR + FILE_SEPARATOR + "bin");
    private static final String CLASS_PATH = "\"" + WORK_DIR + "\\lib" + FILE_SEPARATOR + "*;" + JAR_NAME + "\" ";
    private static final String PROPERTIES_FILENAME = "dbloader.properties";
    private static final AtomicInteger counter = new AtomicInteger(1);

    private final DbLoaderPanel.ProcessSpinnerController processSpinnerController;
    private final ReentrantLock processLock = new ReentrantLock();
    private final Set<Process> processes = new HashSet<Process>();

    public ProcessController(DbLoaderPanel.ProcessSpinnerController processSpinnerController) throws IOException {
        this.processSpinnerController = processSpinnerController;
        File java_exe = new File(JAVA_EXECUTABLE);
        if (!java_exe.exists())
            throw new IOException("JAVA_HOME does not point to valid Java installation.");
        IOUtils.getInstance().cleanUpWorkingDir();
    }

    public synchronized boolean startProcess(Properties properties, String brokerUrl) throws IOException {
        final String subdir = createProcessDir();
        createPropertiesFile(properties, subdir);
        System.out.printf("classpath: %s%nJAVA_EXECUTABLE: %s%n", CLASS_PATH, JAVA_EXECUTABLE);
        final ProcessBuilder processBuilder = new ProcessBuilder(JAVA_EXECUTABLE, "-cp", CLASS_PATH, MAIN_CLASS, brokerUrl).directory(new File(subdir));
        final Process process = processBuilder.start();
        attach(process);
        return true;
    }

    public synchronized boolean startProcess(Properties properties) throws IOException {
        final String subdir = createProcessDir();
        createPropertiesFile(properties, subdir);
        final ProcessBuilder processBuilder = new ProcessBuilder(JAVA_EXECUTABLE, "-cp", CLASS_PATH, MAIN_CLASS).directory(new File(subdir));
        final Process process = processBuilder.start();
        attach(process);
        return true;
    }

    private void attach(final Process process) {
        try {
            processLock.lock();
            processes.add(process);
        } finally {
            processLock.unlock();
        }
        Thread t = new Thread(new ProcessMonitor(process));
        t.setDaemon(true);
        t.start();
    }

    private void detach(Process process) {
        try {
            processLock.lock();
            if (!processes.contains(process))
                return;
            processes.remove(process);
            process.destroy();
        } finally {
            processLock.unlock();
        }
    }

    private void destroyAny() {
        try {
            processLock.lock();
            if (processes.isEmpty())
                return;
            final Process process = processes.iterator().next();
            processes.remove(process);
            process.destroy();
        } finally {
            processLock.unlock();
        }
    }

    void destroyAll() {
        try {
            processLock.lock();
            for (Process p : processes)
                p.destroy();
            processes.clear();
        } finally {
            processLock.unlock();
        }
    }

    public synchronized boolean stopAllProcesses() {
        destroyAll();
        return true;
    }

    public synchronized boolean stopProcess() {
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

    private String createProcessDir() throws IOException {
        int c = counter.getAndIncrement();
        String name = "adbl-" + c;
        File f = new File(name);
        boolean res = f.mkdir();
        if (!res)
            throw new IOException("Cannot create subdirectory for new process");
        IOUtils.getInstance().copyFiles("bin", name);
        return name;
    }

    private void createPropertiesFile(Properties properties, String dir) throws IOException {
        OutputStreamWriter writer = null;
        try {
            writer = new OutputStreamWriter(new FileOutputStream(dir + FILE_SEPARATOR + PROPERTIES_FILENAME));
            properties.store(writer, "AUTOMATICAL CREATED PROPERTIES");
        } finally {
            if (null != writer)
                writer.close();
        }
    }

    private class ProcessMonitor implements Runnable {

        private final Process process;

        public ProcessMonitor(Process process) {
            this.process = process;
        }

        public void run() {
            try {
                processSpinnerController.increase();
                final Thread is = new Thread(new InputStreamMonitor(process.getInputStream()));
                is.setDaemon(true);
                is.start();
                Thread es = new Thread(new InputStreamMonitor(process.getErrorStream()));
                es.setDaemon(true);
                es.start();
                final int exitcode = process.waitFor();
                System.out.println("ExitCode: " + exitcode);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                ProcessController.this.detach(process);
                processSpinnerController.decrease();
            }
        }
    }

    private class InputStreamMonitor implements Runnable {
        private final InputStream inputStream;
        public InputStreamMonitor(InputStream inputStream) {
            this.inputStream = inputStream;
        }
        public void run() {
            //JFrame frame = new JFrame();
            try {
                final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String line;
                try {
                    while (null != (line = reader.readLine())) {
                        System.out.printf("SUBPROCESS OUT: %s%n", line);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } finally {
                if (null != inputStream)
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
            }
        }
    }

}
