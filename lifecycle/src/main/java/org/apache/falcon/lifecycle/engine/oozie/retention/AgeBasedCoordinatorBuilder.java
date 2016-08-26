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

package org.apache.falcon.lifecycle.engine.oozie.retention;

import org.apache.commons.lang3.time.DateUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.ExecutionType;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.lifecycle.engine.oozie.utils.OozieBuilderUtils;
import org.apache.falcon.oozie.coordinator.ACTION;
import org.apache.falcon.oozie.coordinator.CONTROLS;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.WORKFLOW;
import org.apache.falcon.util.RuntimeProperties;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Utility class to build coordinators for AgeBasedDelete Policy.
 */
public final class AgeBasedCoordinatorBuilder {

    private AgeBasedCoordinatorBuilder() {

    }

    private static final Logger LOG = LoggerFactory.getLogger(AgeBasedCoordinatorBuilder.class);

    /**
     * Builds the coordinator app.
     * @param cluster - cluster to schedule retention on.
     * @param basePath - Base path to marshal coordinator app.
     * @param feed - feed for which retention is to be scheduled.
     * @param wfProp - properties passed from workflow to coordinator e.g. ENTITY_PATH
     * @return - Properties from creating the coordinator application to be used by Bundle.
     * @throws FalconException
     */
    public static Properties build(Cluster cluster, Path basePath, Feed feed, Properties wfProp)
        throws FalconException {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster.getName());
        if (feedCluster.getValidity().getEnd().before(new Date())) {
            LOG.warn("Feed Retention is not applicable as Feed's end time for cluster {} is not in the future",
                    cluster.getName());
            return null;
        }

        COORDINATORAPP coord = new COORDINATORAPP();
        String coordName = EntityUtil.getWorkflowName(LifeCycle.EVICTION.getTag(), feed).toString();
        coord.setName(coordName);
        Date endDate = feedCluster.getValidity().getEnd();
        if (RuntimeProperties.get().getProperty(
                "falcon.retention.keep.instances.beyond.validity", "true").equalsIgnoreCase("false")) {
            int retentionLimitinSecs = FeedHelper.getRetentionLimitInSeconds(feed, cluster.getName());
            endDate = DateUtils.addSeconds(endDate, retentionLimitinSecs);
        }
        coord.setEnd(SchemaHelper.formatDateUTC(endDate));
        coord.setStart(SchemaHelper.formatDateUTC(new Date()));
        coord.setTimezone(feed.getTimezone().getID());

        Frequency retentionFrequency = FeedHelper.getLifecycleRetentionFrequency(feed, cluster.getName());
        // set controls
        long frequencyInMillis = ExpressionHelper.get().evaluate(retentionFrequency.toString(), Long.class);
        CONTROLS controls = new CONTROLS();
        controls.setExecution(ExecutionType.LAST_ONLY.value());
        controls.setTimeout(String.valueOf(frequencyInMillis / (1000 * 60)));
        controls.setConcurrency("1");
        controls.setThrottle("1");
        coord.setControls(controls);

        coord.setFrequency("${coord:" + retentionFrequency.toString() + "}");

        Path buildPath = OozieBuilderUtils.getBuildPath(basePath, LifeCycle.EVICTION.getTag());
        Properties props = OozieBuilderUtils.createCoordDefaultConfiguration(coordName, feed);
        props.putAll(OozieBuilderUtils.getProperties(buildPath, coordName));
        props.putAll(EntityUtil.getEntityProperties(feed));
        props.put("queueName", FeedHelper.getLifecycleRetentionQueue(feed, cluster.getName()));
        List<org.apache.falcon.entity.v0.feed.Property> retentionProperties =
                FeedHelper.getLifecycle(feed, cluster.getName()).getRetentionStage().getProperties().getProperties();
        for (org.apache.falcon.entity.v0.feed.Property retentionProperty : retentionProperties) {
            props.put(retentionProperty.getName(), retentionProperty.getValue());
        }

        WORKFLOW workflow = new WORKFLOW();
        String entityPath = wfProp.getProperty(OozieBuilderUtils.ENTITY_PATH);
        String storagePath = OozieBuilderUtils.getStoragePath(entityPath);
        workflow.setAppPath(storagePath);
        workflow.setConfiguration(OozieBuilderUtils.getCoordinatorConfig(props));
        ACTION action = new ACTION();
        action.setWorkflow(workflow);

        coord.setAction(action);

        Path marshalPath = OozieBuilderUtils.marshalCoordinator(cluster, coord, buildPath);
        return OozieBuilderUtils.getProperties(marshalPath, coordName);
    }


    protected static WorkflowExecutionContext.EntityOperations getOperation() {
        return WorkflowExecutionContext.EntityOperations.DELETE;
    }
}
