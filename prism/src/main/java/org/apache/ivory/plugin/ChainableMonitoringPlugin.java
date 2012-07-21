package org.apache.ivory.plugin;

import org.apache.ivory.IvoryException;
import org.apache.ivory.aspect.AbstractIvoryAspect;
import org.apache.ivory.aspect.ResourceMessage;
import org.apache.ivory.util.ReflectionUtils;
import org.apache.ivory.util.StartupProperties;
import org.apache.log4j.Logger;
import org.aspectj.lang.annotation.Aspect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Aspect
public class ChainableMonitoringPlugin extends AbstractIvoryAspect implements MonitoringPlugin {
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
        } catch (IvoryException e) {
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
