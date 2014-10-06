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
import org.apache.falcon.aspect.AuditMessage;
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
 * of {@link MonitoringPlugin}. {@link DefaultMonitoringPlugin} is the default.
 */
@Aspect
public class ChainableMonitoringPlugin extends AbstractFalconAspect
        implements MonitoringPlugin, AlertingPlugin, AuditingPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(ChainableMonitoringPlugin.class);

    private List<MonitoringPlugin> monitoringPlugins = new ArrayList<MonitoringPlugin>();
    private List<AlertingPlugin> alertingPlugins = new ArrayList<AlertingPlugin>();
    private List<AuditingPlugin> auditingPlugins = new ArrayList<AuditingPlugin>();

    public ChainableMonitoringPlugin() {
        initializeMonitoringPlugins();
        initializeAlertingPlugins();
        initializeAuditingPlugins();
    }

    private void initializeMonitoringPlugins() {
        String pluginClasses = StartupProperties.get().
                getProperty("monitoring.plugins", DefaultMonitoringPlugin.class.getName());
        try {
            for (String pluginClass : pluginClasses.split(",")) {
                MonitoringPlugin plugin = ReflectionUtils.getInstanceByClassName(pluginClass.trim());
                monitoringPlugins.add(plugin);
                LOG.info("Registered Monitoring Plugin {}", pluginClass);
            }
        } catch (FalconException e) {
            monitoringPlugins = Arrays.asList((MonitoringPlugin) new DefaultMonitoringPlugin());
            LOG.error("Unable to initialize monitoring.plugins: {}", pluginClasses, e);
        }
    }

    private void initializeAlertingPlugins() {
        String pluginClasses = StartupProperties.get().
                getProperty("alerting.plugins", DefaultMonitoringPlugin.class.getName());
        try {
            for (String pluginClass : pluginClasses.split(",")) {
                AlertingPlugin plugin = ReflectionUtils.getInstanceByClassName(pluginClass.trim());
                alertingPlugins.add(plugin);
                LOG.info("Registered Alerting Plugin {}", pluginClass);
            }
        } catch (FalconException e) {
            alertingPlugins = Arrays.asList((AlertingPlugin) new DefaultMonitoringPlugin());
            LOG.error("Unable to initialize alerting.plugins: {}", pluginClasses, e);
        }
    }

    private void initializeAuditingPlugins() {
        String pluginClasses = StartupProperties.get().
                getProperty("auditing.plugins", DefaultMonitoringPlugin.class.getName());
        try {
            for (String pluginClass : pluginClasses.split(",")) {
                AuditingPlugin plugin = ReflectionUtils.getInstanceByClassName(pluginClass.trim());
                auditingPlugins.add(plugin);
                LOG.info("Registered Auditing Plugin {}", pluginClass);
            }
        } catch (FalconException e) {
            alertingPlugins = Arrays.asList((AlertingPlugin) new DefaultMonitoringPlugin());
            LOG.error("Unable to initialize auditing.plugins: {}", pluginClasses, e);
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
                LOG.debug("Unable to publish alert to {}", plugin.getClass(), e);
            }
        }
    }

    @Override
    public void publishAudit(AuditMessage auditMessage) {
        audit(auditMessage);
    }

    @Override
    public void audit(AuditMessage auditMessage) {
        for (AuditingPlugin plugin : auditingPlugins) {
            try {
                plugin.audit(auditMessage);
            } catch (Exception e) {
                LOG.debug("Unable to publish auditMessage to {}", plugin.getClass(), e);
            }
        }
    }
}
