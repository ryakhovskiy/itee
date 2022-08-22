package org.kr.intp.logging;

import org.kr.intp.application.AppContext;
import org.kr.intp.application.agent.IServer;

/**
 * keep messages while DbLogger is not created
 */
class TempLogger extends BaseLogger {

    private static final long WAIT_TIMEOUT_MS = 500L;

    protected TempLogger() {
        super(true, "LOG_STORAGE");
    }

    public void message(Level level, String name, String message) {
        super.message(new LogMessage(level, name, message, Thread.currentThread().getName(), null));
    }

    public void message(Level level, String name, String format, Object arg) {
        super.message(new LogMessage(level, name, String.format(format, arg), Thread.currentThread().getName(), null));
    }

    public void message(Level level, String name, String format, Object arg1, Object arg2) {
        super.message(new LogMessage(level, name, String.format(format, arg1, arg2), Thread.currentThread().getName(), null));
    }

    public void message(Level level, String name, String format, Object... arguments) {
        super.message(new LogMessage(level, name, String.format(format, arguments), Thread.currentThread().getName(), null));
    }

    public void message(Level level, String name, String message, Throwable t) {
        super.message(new LogMessage(level, name, message, Thread.currentThread().getName(), t));
    }

    public void run() {
        while (true) {
            try {
                dumpLogs();
                Thread.sleep(WAIT_TIMEOUT_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void dumpLogs() throws InterruptedException {
        if (0 == queue.size())
            return;
        final IServer server = AppContext.instance().getServer();
        if (null == server)
            return;
        final BaseLogger logger = server.getDbLogger();
        if (null == logger)
            return;
        if (!(logger instanceof DbLogger))
            return;
        final DbLogger dbLogger = (DbLogger)logger;
        while (!queue.isEmpty()) {
            final LogMessage msg = super.queue.take();
            dbLogger.message(msg);
        }
    }
}
