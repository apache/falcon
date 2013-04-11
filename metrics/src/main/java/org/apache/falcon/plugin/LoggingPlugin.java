package org.apache.falcon.plugin;

import org.apache.falcon.aspect.ResourceMessage;
import org.apache.log4j.Logger;

public class LoggingPlugin implements MonitoringPlugin {
    private static final Logger METRIC = Logger.getLogger("METRIC");

    @Override
    public void monitor(ResourceMessage message) {
        METRIC.info(message);
    }
}
