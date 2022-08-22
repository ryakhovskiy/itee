package org.kr.db.loader.ui.automon;

import org.kr.db.loader.ui.AppMain;
import org.kr.db.loader.ui.panels.AutoRunPanel;
import org.kr.db.loader.ui.panels.DbLoaderPanel;
import org.kr.db.loader.ui.panels.MonitorPanel;
import org.kr.db.loader.ui.pojo.AutoRunSpec;
import org.kr.db.loader.ui.pojo.Scenario;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created bykron 31.07.2014.
 */
public class AutoRunner {

    private final MonitorPanel rtMonitorPanel;
    private final DbLoaderPanel rtupDbLoaderPanel;
    private final DbLoaderPanel rtqpDbLoaderPanel;
    private final MonitorPanel itMonitorPanel;
    private final DbLoaderPanel itupDbLoaderPanel;
    private final DbLoaderPanel itqpDbLoaderPanel;
    private final List<AutoScenarioRunner> runners = new ArrayList<AutoScenarioRunner>();
    private volatile boolean running = false;

    public AutoRunner(MonitorPanel rtMonitorPanel, DbLoaderPanel rtupDbLoaderPanel, DbLoaderPanel rtqpDbLoaderPanel,
                      MonitorPanel itMonitorPanel, DbLoaderPanel itupDbLoaderPanel, DbLoaderPanel itqpDbLoaderPanel) {
        this.rtMonitorPanel = rtMonitorPanel;
        this.rtupDbLoaderPanel = rtupDbLoaderPanel;
        this.rtqpDbLoaderPanel = rtqpDbLoaderPanel;
        this.itMonitorPanel = itMonitorPanel;
        this.itupDbLoaderPanel = itupDbLoaderPanel;
        this.itqpDbLoaderPanel = itqpDbLoaderPanel;
    }

    public void runAutoSpec(final AutoRunSpec autoRunSpec, final AutoRunPanel.LaunchButtonEnabler enabler,
                            final AutoRunPanel.ProgressUpdater rtUpdater,
                            final AutoRunPanel.ProgressUpdater itUpdater) {
        final AutoScenarioRunner itasr = new AutoScenarioRunner(
                autoRunSpec.getItScenario(), itMonitorPanel, itupDbLoaderPanel, itqpDbLoaderPanel, itUpdater);
        final AutoScenarioRunner rtasr = new AutoScenarioRunner(
                autoRunSpec.getRtScenario(), rtMonitorPanel, rtupDbLoaderPanel, rtqpDbLoaderPanel, rtUpdater);
        runners.add(itasr);
        runners.add(rtasr);
        this.running = true;

        final boolean rtEnabled = autoRunSpec.isRtEnabled();
        final boolean itEnabled = autoRunSpec.isItEnabled();
        if (rtEnabled)
            rtasr.start();
        if (itEnabled)
            itasr.start();

        new Thread() {
          public void run() {
              try {
                  while (rtasr.isRunning())
                      Thread.sleep(1000);
                  while (itasr.isRunning())
                      Thread.sleep(1000);
              } catch (InterruptedException e) {
                  System.err.println("-----INTERRUPTED");
                  e.printStackTrace();
              } finally {
                  running = false;
                  enabler.enableLaunchButton();
              }
          }
        }.start();
    }

    public void stopAutoSpec() {
        if (!running)
            return;
        for (AutoScenarioRunner asr : runners)
            asr.stopAutoSpec();
    }

    private class AutoScenarioRunner {

        private final List<Future> futures = new ArrayList<Future>();
        private final Scenario scenario;
        private final MonitorPanel monitorPanel;
        private final DbLoaderPanel updatePanel;
        private final DbLoaderPanel queryPanel;
        private final AutoRunPanel.ProgressUpdater progressUpdater;
        private final ExecutorService executor = Executors.newFixedThreadPool(4);

        private volatile boolean running;

        private AutoScenarioRunner(Scenario scenario, MonitorPanel monitorPanel, DbLoaderPanel updatePanel,
                                   DbLoaderPanel queryPanel, AutoRunPanel.ProgressUpdater progressUpdater) {
            this.scenario = scenario;
            this.monitorPanel = monitorPanel;
            this.updatePanel = updatePanel;
            this.queryPanel = queryPanel;
            this.progressUpdater = progressUpdater;
        }

        public void start() {
            running = true;
            final Future f = executor.submit(new Runnable() {
                @Override
                public void run() {
                    runAutoScenario();
                }
            });
            futures.add(f);
        }

        private void runAutoScenario() {
            if (scenario.getTimeout() > 0)
                scheduleStopThread(scenario.getTimeout());
            final long start = AppMain.currentTimeMillis();
            try {
                monitorPanel.startMonitor();
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        final long procPeriod = scenario.getUpdateProcPeriod();
                        final int procLimit = scenario.getUpdateMaxProcesses();
                        final int procBatchSize = scenario.getUpdateProcessesCount();
                        runAutoLoader(updatePanel, procPeriod, procLimit, procBatchSize);
                    }
                });
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        final long procPeriod = scenario.getQueryProcPeriod();
                        final int procLimit = scenario.getQueryMaxProcesses();
                        final int procBatchSize = scenario.getQueryProcessesCount();
                        runAutoLoader(queryPanel, procPeriod, procLimit, procBatchSize);
                    }
                });
                while (!executor.isTerminated()) {
                    final long current = AppMain.currentTimeMillis() - start;
                    progressUpdater.setProgress((int)current);
                    executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                System.out.println("AUTORUN INTERRUPTED after (ms): " + (AppMain.currentTimeMillis() - start));
            } finally {
                executor.shutdownNow();
                queryPanel.stopProcesses();
                updatePanel.stopProcesses();
            }
        }

        private void runAutoLoader(DbLoaderPanel panel, long procPeriod, int procLimit, int batchSize) {
            int currentProcesses = 0;
            do {
                try {
                    Thread.sleep(procPeriod);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                panel.addProcesses(batchSize);
                currentProcesses += batchSize;
            } while (procLimit == 0 || currentProcesses < procLimit);
        }

        private void scheduleStopThread(final long timeout) {
            final Thread autoRunThread = Thread.currentThread();
            final Future f = executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(timeout);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        stopByTimer(autoRunThread);
                    }
                }
            });
            futures.add(f);
            System.out.println(futures.size());
        }

        private void stopAutoSpec() {
            if (!running) {
                System.out.println("AutoSpec is not running!");
                return;
            }
            stopImmediately();
        }

        private void stopByTimer(Thread autoRunThread) {
            stopImmediately();
            autoRunThread.interrupt();
        }

        private void stopImmediately() {
            synchronized (futures) {
                try {
                    monitorPanel.stopMonitor();
                    for (Future f : futures)
                        f.cancel(true);
                    futures.clear();
                    Thread.currentThread().interrupt();
                } finally {
                    running = false;
                }
            }
        }

        private boolean isRunning() {
            return running;
        }
    }
}
