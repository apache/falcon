package org.apache.falcon.plugin;

import org.apache.falcon.FalconException;
import org.apache.falcon.aspect.AbstractFalconAspect;
import org.apache.falcon.aspect.ResourceMessage;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.util.StartupProperties;
import org.apache.log4j.Logger;
import org.aspectj.lang.annotation.Aspect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Aspect
public class ChainableMonitoringPlugin extends AbstractFalconAspect implements MonitoringPlugin {
    private static final Logger LOG = Logger.getLogger(ChainableMonitoringPlugin.class);

    private List<MonitoringPlugin> plugins = new ArrayList<MonitoringPlugin>();

    public ChainableMonitoringPlugin() {
        String pluginClasses = StartupProperties.get().
                getProperty("monitoring.plugins", LoggingPlugin.class.getName());
        try {
            for (String pluginClass : pluginClasses.split(",")) {
                MonitoringPlugin plugin = ReflectionUtils.getInstanceByClassName(pluginClass.trim());
                plugins.add(plugin);
                LOG.info("Registered Monitoring Plugin " + pluginClass);
            }
        } catch (FalconException e) {
            plugins = Arrays.asList((MonitoringPlugin)new LoggingPlugin());
            LOG.error("Unable to initialize monitoring plugins: " + pluginClasses, e);
        }
    }

    @Override
    public void monitor(ResourceMessage message) {
        for (MonitoringPlugin plugin : plugins) {
            try {
                plugin.monitor(message);
            } catch (Exception e) {
                LOG.debug("Unable to publish message to " + plugin.getClass(), e);
            }
        }
    }

    @Override
    public void publishMessage(ResourceMessage message) {
        monitor(message);
    }
}
