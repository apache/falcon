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

import org.apache.commons.lang3.time.DateUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.oozie.OozieCoordinatorBuilder;
import org.apache.falcon.oozie.OozieEntityBuilder;
import org.apache.falcon.oozie.OozieOrchestrationWorkflowBuilder;
import org.apache.falcon.oozie.coordinator.ACTION;
import org.apache.falcon.oozie.coordinator.COORDINATORAPP;
import org.apache.falcon.oozie.coordinator.WORKFLOW;
import org.apache.falcon.util.DateUtil;
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
        if (feedCluster == null) {
            return null;
        }

        COORDINATORAPP coord = new COORDINATORAPP();
        String coordName = getEntityName();
        coord.setName(coordName);
        Date endDate = feedCluster.getValidity().getEnd();
        coord.setEnd(SchemaHelper.formatDateUTC(endDate));
        if (feedCluster.getValidity().getEnd().before(new Date())) {
            Date startDate = DateUtils.addMinutes(endDate, -1);
            coord.setStart(SchemaHelper.formatDateUTC(startDate));
        } else {
            coord.setStart(SchemaHelper.formatDateUTC(new Date()));
        }
        coord.setTimezone(entity.getTimezone().getID());
        Frequency entityFrequency = entity.getFrequency();
        Frequency defaultFrequency = new Frequency("hours(24)");
        if (DateUtil.getFrequencyInMillis(entityFrequency) < DateUtil.getFrequencyInMillis(defaultFrequency)) {
            coord.setFrequency("${coord:hours(6)}");
        } else {
            coord.setFrequency("${coord:days(1)}");
        }

        Path coordPath = getBuildPath(buildPath);
        Properties props = createCoordDefaultConfiguration(coordName);

        WORKFLOW workflow = new WORKFLOW();
        Properties wfProps = OozieOrchestrationWorkflowBuilder.get(entity, cluster, Tag.RETENTION).build(cluster,
            coordPath);
        workflow.setAppPath(getStoragePath(wfProps.getProperty(OozieEntityBuilder.ENTITY_PATH)));
        props.putAll(getProperties(coordPath, coordName));
        // Add the custom properties set in feed. Else, dryrun won't catch any missing props.
        props.putAll(EntityUtil.getEntityProperties(entity));
        workflow.setConfiguration(getConfig(props));
        ACTION action = new ACTION();
        action.setWorkflow(workflow);

        coord.setAction(action);

        Path marshalPath = marshal(cluster, coord, coordPath);
        return Arrays.asList(getProperties(marshalPath, coordName));
    }
}
