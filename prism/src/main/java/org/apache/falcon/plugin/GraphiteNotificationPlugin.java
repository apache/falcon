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

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.aspect.ResourceMessage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.metrics.MetricNotificationService;
import org.apache.falcon.service.Services;
import org.apache.falcon.util.StartupProperties;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Graphite Notification Plugin.
 */
public class GraphiteNotificationPlugin implements MonitoringPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(GraphiteNotificationPlugin.class);

    @Override
    public void monitor(ResourceMessage message) {
        MetricNotificationService metricNotificationService =
                Services.get().getService(MetricNotificationService.SERVICE_NAME);
        try {
            String entityType = message.getDimensions().get("entity-type");
            String entityName = message.getDimensions().get("entity-name");
            String prefix = StartupProperties.get().getProperty("falcon.graphite.prefix");
            if (entityType.equals(EntityType.PROCESS.name())) {
                Entity entity = ConfigurationStore.get().get(EntityType.PROCESS, entityName);
                Process process = (Process) entity;
                String pipeline =  StringUtils.isNotBlank(process.getPipelines()) ? process.getPipelines() : "default";


                if ((message.getAction().equals("wf-instance-succeeded"))) {
                    Long timeTaken =  message.getExecutionTime() / 1000000000;
                    String metricsName = prefix + message.getDimensions().get("cluster") + pipeline
                            + ".GENERATE." + entityName + ".processing_time";
                    metricNotificationService.publish(metricsName, timeTaken);

                    DateTime nominalTime = new DateTime(message.getDimensions().get("nominal-time"));
                    DateTime startTime = new DateTime(message.getDimensions().get("start-time"));
                    metricsName = prefix + message.getDimensions().get("cluster") + pipeline
                            + ".GENERATE." + entityName + ".start_delay";
                    metricNotificationService.publish(metricsName,
                        (long)Seconds.secondsBetween(nominalTime, startTime).getSeconds());
                }

                if (message.getAction().equals("wf-instance-failed")){
                    String metricName =  prefix + message.getDimensions().get("cluster") + pipeline
                            + ".GENERATE." +  entityName + ".failure"
                        + message.getDimensions().get("error-message");
                    metricNotificationService.publish(metricName, (long) 1);
                }
            }
        } catch (Exception e) {
            LOG.error("Exception in sending metrics to Graphite:", e);
        }
    }
}
