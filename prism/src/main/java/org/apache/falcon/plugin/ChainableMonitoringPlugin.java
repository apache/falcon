/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.falcon.plugin;

import org.apache.falcon.FalconException;
import org.apache.falcon.aspect.AbstractFalconAspect;
import org.apache.falcon.aspect.AlertMessage;
import org.apache.falcon.aspect.ResourceMessage;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.util.StartupProperties;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class implements the chain of responsibility for configured implementations
 * of {@link MonitoringPlugin}. {@link LoggingPlugin} is the default.
 */
@Aspect
public class ChainableMonitoringPlugin extends AbstractFalconAspect
        implements MonitoringPlugin, AlertingPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(ChainableMonitoringPlugin.class);

    private List<MonitoringPlugin> monitoringPlugins = new ArrayList<MonitoringPlugin>();
    private List<AlertingPlugin> alertingPlugins = new ArrayList<AlertingPlugin>();

    public ChainableMonitoringPlugin() {
        initializeMonitoringPlugins();
        initializeAlertingPlugins();
    }

    private void initializeMonitoringPlugins() {
        String pluginClasses = StartupProperties.get().
                getProperty("monitoring.plugins", LoggingPlugin.class.getName());
        try {
            for (String pluginClass : pluginClasses.split(",")) {
                MonitoringPlugin plugin = ReflectionUtils.getInstanceByClassName(pluginClass.trim());
                monitoringPlugins.add(plugin);
                LOG.info("Registered Monitoring Plugin {}", pluginClass);
            }
        } catch (FalconException e) {
            monitoringPlugins = Arrays.asList((MonitoringPlugin) new LoggingPlugin());
            LOG.error("Unable to initialize monitoring.plugins: {}", pluginClasses, e);
        }
    }

    private void initializeAlertingPlugins() {
        String pluginClasses = StartupProperties.get().
                getProperty("alerting.plugins", LoggingPlugin.class.getName());
        try {
            for (String pluginClass : pluginClasses.split(",")) {
                AlertingPlugin plugin = ReflectionUtils.getInstanceByClassName(pluginClass.trim());
                alertingPlugins.add(plugin);
                LOG.info("Registered Alerting Plugin {}", pluginClass);
            }
        } catch (FalconException e) {
            alertingPlugins = Arrays.asList((AlertingPlugin) new LoggingPlugin());
            LOG.error("Unable to initialize alerting.plugins: {}", pluginClasses, e);
        }
    }

    @Override
    public void monitor(ResourceMessage message) {
        for (MonitoringPlugin plugin : monitoringPlugins) {
            try {
                plugin.monitor(message);
            } catch (Exception e) {
                LOG.debug("Unable to publish message to {}", plugin.getClass(), e);
            }
        }
    }

    @Override
    public void publishMessage(ResourceMessage message) {
        monitor(message);
    }

    @Override
    public void publishAlert(AlertMessage alertMessage) {
        alert(alertMessage);
    }

    @Override
    public void alert(AlertMessage alertMessage) {
        for (AlertingPlugin plugin : alertingPlugins) {
            try {
                plugin.alert(alertMessage);
            } catch (Exception e) {
                LOG.debug("Unable to publish message to {}", plugin.getClass(), e);
            }
        }
    }
}
