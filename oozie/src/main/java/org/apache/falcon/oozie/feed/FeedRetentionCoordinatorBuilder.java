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

package org.apache.falcon.oozie.feed;

import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.v0.Frequency.TimeUnit;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.oozie.OozieCoordinatorBuilder;
import org.apache.falcon.oozie.OozieEntityBuilder;
import org.apache.falcon.oozie.OozieOrchestrationWorkflowBuilder;
import org.apache.falcon.oozie.coordinator.ACTION;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.WORKFLOW;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Builds feed retention coordinator.
 */
public class FeedRetentionCoordinatorBuilder extends OozieCoordinatorBuilder<Feed> {
    public FeedRetentionCoordinatorBuilder(Feed entity) {
        super(entity, LifeCycle.EVICTION);
    }

    @Override public List<Properties> buildCoords(Cluster cluster, Path buildPath) throws FalconException {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(entity, cluster.getName());

        if (feedCluster.getValidity().getEnd().before(new Date())) {
            LOG.warn("Feed Retention is not applicable as Feed's end time for cluster {} is not in the future",
                cluster.getName());
            return null;
        }

        COORDINATORAPP coord = new COORDINATORAPP();
        String coordName = getEntityName();
        coord.setName(coordName);
        coord.setEnd(SchemaHelper.formatDateUTC(feedCluster.getValidity().getEnd()));
        coord.setStart(SchemaHelper.formatDateUTC(new Date()));
        coord.setTimezone(entity.getTimezone().getID());
        TimeUnit timeUnit = entity.getFrequency().getTimeUnit();
        if (timeUnit == TimeUnit.hours || timeUnit == TimeUnit.minutes) {
            coord.setFrequency("${coord:hours(6)}");
        } else {
            coord.setFrequency("${coord:days(1)}");
        }

        Path coordPath = getBuildPath(buildPath);
        Properties props = createCoordDefaultConfiguration(cluster, coordName);
        props.put("timeZone", entity.getTimezone().getID());
        props.put("frequency", entity.getFrequency().getTimeUnit().name());

        final Storage storage = FeedHelper.createStorage(cluster, entity);
        props.put("falconFeedStorageType", storage.getType().name());

        String feedDataPath = storage.getUriTemplate();
        props.put("feedDataPath",
            feedDataPath.replaceAll(Storage.DOLLAR_EXPR_START_REGEX, Storage.QUESTION_EXPR_START_REGEX));

        props.put("limit", feedCluster.getRetention().getLimit().toString());

        props.put(WorkflowExecutionArgs.FEED_NAMES.getName(), entity.getName());
        props.put(WorkflowExecutionArgs.FEED_INSTANCE_PATHS.getName(), IGNORE);

        props.put("falconInputFeeds", entity.getName());
        props.put("falconInPaths", IGNORE);

        props.putAll(FeedHelper.getUserWorkflowProperties(getLifecycle()));

        WORKFLOW workflow = new WORKFLOW();
        Properties wfProp = OozieOrchestrationWorkflowBuilder.get(entity, cluster, Tag.RETENTION).build(cluster,
            coordPath);
        workflow.setAppPath(getStoragePath(wfProp.getProperty(OozieEntityBuilder.ENTITY_PATH)));
        props.putAll(wfProp);
        workflow.setConfiguration(getConfig(props));
        ACTION action = new ACTION();
        action.setWorkflow(workflow);

        coord.setAction(action);

        Path marshalPath = marshal(cluster, coord, coordPath);
        return Arrays.asList(getProperties(marshalPath, coordName));
    }

    @Override
    protected WorkflowExecutionContext.EntityOperations getOperation() {
        return WorkflowExecutionContext.EntityOperations.DELETE;
    }
}
