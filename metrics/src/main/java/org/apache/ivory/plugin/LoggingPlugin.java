package org.apache.ivory.plugin;

import org.apache.ivory.aspect.ResourceMessage;
import org.apache.log4j.Logger;

public class LoggingPlugin implements MonitoringPlugin {
    private static final Logger METRIC = Logger.getLogger("METRIC");

    @Override
    public void monitor(ResourceMessage message) {
        METRIC.info(message);
    }
}
