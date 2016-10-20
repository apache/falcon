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
            String entityType = StringUtils.isNotBlank(message.getDimensions().get("entityType"))
                    ? message.getDimensions().get("entityType") :message.getDimensions().get("entity-type");
            String entityName = StringUtils.isNotBlank(message.getDimensions().get("entityName"))
                    ? message.getDimensions().get("entityName") :message.getDimensions().get("entity-name");
            String prefix = StartupProperties.get().getProperty("falcon.graphite.prefix");
            LOG.debug("message:" + message.getAction());
            if (entityType.equalsIgnoreCase(EntityType.PROCESS.name())
                    && ConfigurationStore.get().get(EntityType.PROCESS, entityName) != null) {
                Entity entity = ConfigurationStore.get().get(EntityType.PROCESS, entityName);
                Process process = (Process) entity;
                String pipeline =  StringUtils.isNotBlank(process.getPipelines()) ? process.getPipelines() : "default";

                if ((message.getAction().equals("wf-instance-succeeded"))) {
                    Long timeTaken =  message.getExecutionTime() / 1000000000;
                    StringBuilder processingMetric = new StringBuilder(prefix).append(".").append(message.
                            getDimensions().get("cluster")).append(".").append(pipeline).append(".GENERATE.")
                            .append(entityName).append(".processing_time");
                    metricNotificationService.publish(processingMetric.toString(), timeTaken);

                    DateTime nominalTime = new DateTime(message.getDimensions().get("nominal-time"));
                    DateTime startTime = new DateTime(message.getDimensions().get("start-time"));
                    StringBuilder startTimeMetric = new StringBuilder(prefix).append(".").append(message.
                            getDimensions().get("cluster")).append(".").append(pipeline).append(".GENERATE.").
                            append(entityName).append(".start_delay");
                    metricNotificationService.publish(startTimeMetric.toString(),
                            (long)Seconds.secondsBetween(nominalTime, startTime).getSeconds());
                }

                if (message.getAction().equals("wf-instance-failed")){
                    StringBuilder metricName = new StringBuilder(prefix).append(".").append(message.
                            getDimensions().get("cluster")).append(".").append(pipeline).append(".GENERATE.").
                            append(entityName).append(".failure").append(message.getDimensions().get("error-message"));
                    metricNotificationService.publish(metricName.toString(), (long) 1);
                }
            }
        } catch (Exception e) {
            LOG.error("Exception in sending metrics to Graphite:", e);
        }
    }
}
