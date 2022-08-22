package org.kr.db.loader.ui.conf;

/**
 *
 */
public class GlobalConfiguration {

    private static final ConfigurationManager config = new ConfigurationManager();

    public static final boolean isGlobalConfigActivated = config.useCustomProperties();
    public static final boolean keepStatsInMemory = !isGlobalConfigActivated ||
            (isGlobalConfigActivated && config.keepStatsInmemory());
    private static final ProcedureConfiguration[] proceduresConfiguration = config.getProceduresConfigurations();

    public static ProcedureConfiguration[] getProceduresConfiguration() {
        return proceduresConfiguration;
    }

    public static ProcedureConfiguration[] getCustomProceduresConfiguration() {
        final ProcedureConfiguration[] copy = new ProcedureConfiguration[proceduresConfiguration.length];
        System.arraycopy(proceduresConfiguration, 0, copy, 0, proceduresConfiguration.length);
        return copy;
    }

    private GlobalConfiguration() { }
}
