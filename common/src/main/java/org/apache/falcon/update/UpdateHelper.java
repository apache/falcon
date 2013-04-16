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

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Partition;
import org.apache.falcon.entity.v0.feed.Partitions;
import org.apache.falcon.entity.v0.process.Cluster;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public final class UpdateHelper {
    private static final Logger LOG = Logger.getLogger(UpdateHelper.class);
    private static final String[] FEED_FIELDS = new String[]{"partitions", "groups", "lateArrival.cutOff",
                                                             "schema.location", "schema.provider",
                                                             "ACL.group", "ACL.owner", "ACL.permission"};
    private static final String[] PROCESS_FIELDS = new String[]{"retry.policy", "retry.delay", "retry.attempts",
                                                                "lateProcess.policy", "lateProcess.delay",
                                                                "lateProcess.lateInputs[\\d+].input",
                                                                "lateProcess.lateInputs[\\d+].workflowPath"};

    public static boolean shouldUpdate(Entity oldEntity, Entity newEntity, String cluster) throws FalconException {
        Entity oldView = EntityUtil.getClusterView(oldEntity, cluster);
        Entity newView = EntityUtil.getClusterView(newEntity, cluster);
        switch (oldEntity.getEntityType()) {
            case FEED:
                if (EntityUtil.equals(oldView, newView, FEED_FIELDS)) {
                    return false;
                }
                return true;

            case PROCESS:
                if (EntityUtil.equals(oldView, newView, PROCESS_FIELDS)) {
                    return false;
                }
                return true;
        }
        throw new IllegalArgumentException("Unhandled entity type " + oldEntity.getEntityType());
    }

    public static boolean shouldUpdate(Entity oldEntity, Entity newEntity, Entity affectedEntity)
            throws FalconException {
        if (oldEntity.getEntityType() == EntityType.FEED && affectedEntity.getEntityType() == EntityType.PROCESS) {
            return shouldUpdate((Feed) oldEntity, (Feed) newEntity, (Process) affectedEntity);
        } else {
            LOG.debug(newEntity.toShortString());
            LOG.debug(affectedEntity.toShortString());
            throw new FalconException("Don't know what to do. Unexpected scenario");
        }
    }

    public static boolean shouldUpdate(Feed oldFeed, Feed newFeed, Process affectedProcess) {
        if (!FeedHelper
                .getLocation(oldFeed.getLocations(), LocationType.DATA)
                .getPath()
                .equals(FeedHelper.getLocation(newFeed.getLocations(),
                        LocationType.DATA).getPath())
                || !FeedHelper
                .getLocation(oldFeed.getLocations(), LocationType.META)
                .getPath()
                .equals(FeedHelper.getLocation(newFeed.getLocations(),
                        LocationType.META).getPath())
                || !FeedHelper
                .getLocation(oldFeed.getLocations(), LocationType.STATS)
                .getPath()
                .equals(FeedHelper.getLocation(newFeed.getLocations(),
                        LocationType.STATS).getPath())
                || !FeedHelper
                .getLocation(oldFeed.getLocations(), LocationType.TMP)
                .getPath()
                .equals(FeedHelper.getLocation(newFeed.getLocations(),
                        LocationType.TMP).getPath())) {
            return true;
        }
        LOG.debug(oldFeed.toShortString() + ": Location identical. Ignoring...");

        if (!oldFeed.getFrequency().equals(newFeed.getFrequency())) {
            return true;
        }
        LOG.debug(oldFeed.toShortString() + ": Frequency identical. Ignoring...");

        // it is not possible to have oldFeed partitions as non empty and
        // new being empty. validator should have gated this.
        // Also if new partitions are added and old is empty, then there is
        // nothing
        // to update in process
        boolean partitionApplicable = false;
        Inputs affectedInputs = affectedProcess.getInputs();
        if (affectedInputs != null && affectedInputs.getInputs() != null) {
            for (Input input : affectedInputs.getInputs()) {
                if (input.getFeed().equals(oldFeed.getName())) {
                    if (input.getPartition() != null && !input.getPartition().isEmpty()) {
                        partitionApplicable = true;
                    }
                }
            }
            if (partitionApplicable) {
                LOG.debug("Partitions are applicable. Checking ...");
                if (newFeed.getPartitions() != null && oldFeed.getPartitions() != null) {
                    List<String> newParts = getPartitions(newFeed.getPartitions());
                    List<String> oldParts = getPartitions(oldFeed.getPartitions());
                    if (newParts.size() != oldParts.size()) {
                        return true;
                    }
                    if (!newParts.containsAll(oldParts)) {
                        return true;
                    }
                }
                LOG.debug(oldFeed.toShortString() + ": Partitions identical. Ignoring...");
            }
        }

        for (Cluster cluster : affectedProcess.getClusters().getClusters()) {
            if (!FeedHelper
                    .getCluster(oldFeed, cluster.getName())
                    .getValidity()
                    .getStart()
                    .equals(FeedHelper.getCluster(newFeed, cluster.getName())
                            .getValidity().getStart())
                    || !FeedHelper.getLocation(oldFeed, LocationType.DATA,
                    cluster.getName()).getPath().equals(
                    FeedHelper.getLocation(newFeed, LocationType.DATA,
                            cluster.getName()).getPath())
                    || !FeedHelper.getLocation(oldFeed, LocationType.META,
                    cluster.getName()).getPath().equals(
                    FeedHelper.getLocation(newFeed, LocationType.META,
                            cluster.getName()).getPath())
                    || !FeedHelper.getLocation(oldFeed, LocationType.STATS,
                    cluster.getName()).getPath().equals(
                    FeedHelper.getLocation(newFeed, LocationType.STATS,
                            cluster.getName()).getPath())
                    || !FeedHelper.getLocation(oldFeed, LocationType.TMP,
                    cluster.getName()).getPath().equals(
                    FeedHelper.getLocation(newFeed, LocationType.TMP,
                            cluster.getName()).getPath())) {
                return true;
            }
            LOG.debug(oldFeed.toShortString() + ": Feed on cluster" + cluster.getName() + " identical. Ignoring...");
        }

        return false;
    }

    private static List<String> getPartitions(Partitions partitions) {
        List<String> parts = new ArrayList<String>();
        for (Partition partition : partitions.getPartitions()) {
            parts.add(partition.getName());
        }
        return parts;
    }
}
