package org.kr.intp.application.agent;

import org.kr.intp.config.IntpConfig;
import org.kr.intp.logging.BaseLogger;

import java.io.Closeable;

/**
 *
 */
public interface IServer extends IResourceMonitor, IApplicationManager, Closeable {

    IntpConfig getConfiguration();

    BaseLogger getDbLogger();
}
