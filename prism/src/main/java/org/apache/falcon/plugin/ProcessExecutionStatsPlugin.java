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
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.jdbc.MonitoringJdbcStateStore;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This plugin writes process completion time ,number of failures and wait time to DB.
 */
public class ProcessExecutionStatsPlugin implements MonitoringPlugin {
    private static final Logger LOG = LoggerFactory.getLogger(ProcessExecutionStatsPlugin.class);

    private static final MonitoringJdbcStateStore MONITORING_JDBC_STATE_STORE = new MonitoringJdbcStateStore();

    @Override
    public void monitor(ResourceMessage message) {
        try {
            String entityType = StringUtils.isNotBlank(message.getDimensions().get("entityType"))
                    ? message.getDimensions().get("entityType") :message.getDimensions().get("entity-type");
            String entityName = StringUtils.isNotBlank(message.getDimensions().get("entityName"))
                    ? message.getDimensions().get("entityName") :message.getDimensions().get("entity-name");
            LOG.debug("message:" + message.getAction());
            if (entityType.equalsIgnoreCase(EntityType.PROCESS.name())
                    && ConfigurationStore.get().get(EntityType.PROCESS, entityName) != null) {
                Process process = ConfigurationStore.get().get(EntityType.PROCESS, entityName);
                String pipelines =  StringUtils.isNotBlank(process.getPipelines()) ? process.getPipelines()
                        : "__untagged";
                String cluster =  message.getDimensions().get("cluster");
                DateTime nominalTime = new DateTime(message.getDimensions().get("nominal-time"));
                DateTime startTime = new DateTime(message.getDimensions().get("start-time"));
                Long startDelay = (long) Seconds.secondsBetween(nominalTime, startTime).getSeconds();
                Long timeTaken =  message.getExecutionTime() / 1000000000;

                String [] pipelineNames = pipelines.split(",");

                for(String name : pipelineNames){

                    if ((message.getAction().equals("wf-instance-succeeded"))) {
                        MONITORING_JDBC_STATE_STORE.putProcessInstance(entityName, cluster, nominalTime.getMillis(),
                                startDelay, timeTaken, name, "succeeded");
                    }
                    if (message.getAction().equals("wf-instance-failed")){
                        MONITORING_JDBC_STATE_STORE.putProcessInstance(entityName, cluster, nominalTime.getMillis(),
                                startDelay, timeTaken, name, "failed");
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Exception in sending metrics to FalconDB:", e);
        }
    }
}
