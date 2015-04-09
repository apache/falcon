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

package org.apache.falcon.update;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.ProcessHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Helper methods to facilitate entity updates.
 */
public final class UpdateHelper {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateHelper.class);

    private static final String[] FEED_FIELDS = new String[]{"partitions", "groups", "lateArrival.cutOff",
                                                             "schema.location", "schema.provider", "tags",
                                                             "group", "owner", "permission", };
    private static final String[] PROCESS_FIELDS = new String[]{"retry.policy", "retry.delay", "retry.attempts",
                                                                "lateProcess.policy", "lateProcess.delay",
                                                                "lateProcess.lateInputs[\\d+].input",
                                                                "lateProcess.lateInputs[\\d+].workflowPath",
                                                                "owner", "group", "permission", "tags",
                                                                "pipelines", };

    private UpdateHelper() {}

    public static boolean isEntityUpdated(Entity oldEntity, Entity newEntity, String cluster,
        Path oldStagingPath) throws FalconException {
        Entity oldView = EntityUtil.getClusterView(oldEntity, cluster);
        Entity newView = EntityUtil.getClusterView(newEntity, cluster);

        //staging path contains md5 of the cluster view of entity
        String[] parts = oldStagingPath.getName().split("_");
        if (parts[0].equals(EntityUtil.md5(newView))) {
            return false;
        }

        switch (oldEntity.getEntityType()) {
        case FEED:
            return !EntityUtil.equals(oldView, newView, FEED_FIELDS);

        case PROCESS:
            return !EntityUtil.equals(oldView, newView, PROCESS_FIELDS);

        default:
        }
        throw new IllegalArgumentException("Unhandled entity type " + oldEntity.getEntityType());
    }

    public static boolean shouldUpdate(Entity oldEntity, Entity newEntity, Entity affectedEntity, String cluster)
        throws FalconException {
        if (oldEntity.getEntityType() == EntityType.FEED && affectedEntity.getEntityType() == EntityType.PROCESS) {

            Feed oldFeed = (Feed) oldEntity;
            Feed newFeed = (Feed) newEntity;
            Process affectedProcess = (Process) affectedEntity;

            //check if affectedProcess is defined for this cluster
            Cluster processCluster = ProcessHelper.getCluster(affectedProcess, cluster);
            if (processCluster == null) {
                LOG.debug("Process {} is not defined for cluster {}. Skipping", affectedProcess.getName(), cluster);
                return false;
            }

            if (processCluster.getValidity().getEnd().before(new Date())) {
                LOG.debug("Process {} validity {} is in the past. Skipping...", affectedProcess.getName(),
                    processCluster.getValidity().getEnd());
                return false;
            }

            if (!oldFeed.getFrequency().equals(newFeed.getFrequency())) {
                LOG.debug("{}: Frequency has changed. Updating...", oldFeed.toShortString());
                return true;
            }

            if (!StringUtils.equals(oldFeed.getAvailabilityFlag(), newFeed.getAvailabilityFlag())) {
                LOG.debug("{}: Availability flag has changed. Updating...", oldFeed.toShortString());
                return true;
            }

            org.apache.falcon.entity.v0.feed.Cluster oldFeedCluster = FeedHelper.getCluster(oldFeed, cluster);
            org.apache.falcon.entity.v0.feed.Cluster newFeedCluster = FeedHelper.getCluster(newFeed, cluster);
            if (!oldFeedCluster.getValidity().getStart().equals(newFeedCluster.getValidity().getStart())) {
                LOG.debug("{}: Start time for cluster {} has changed. Updating...", oldFeed.toShortString(), cluster);
                return true;
            }

            Storage oldFeedStorage = FeedHelper.createStorage(cluster, oldFeed);
            Storage newFeedStorage = FeedHelper.createStorage(cluster, newFeed);

            if (!oldFeedStorage.isIdentical(newFeedStorage)) {
                LOG.debug("{}: Storage has changed. Updating...", oldFeed.toShortString());
                return true;
            }
            return false;

        } else {
            LOG.debug(newEntity.toShortString());
            LOG.debug(affectedEntity.toShortString());
            throw new FalconException("Don't know what to do. Unexpected scenario");
        }
    }
}
