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

package org.apache.falcon.workflow;

import org.apache.falcon.FalconException;
import org.apache.falcon.Tag;
import org.apache.falcon.converter.AbstractOozieEntityMapper;
import org.apache.falcon.converter.OozieFeedMapper;
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.security.CurrentUser;
import org.apache.hadoop.fs.Path;

import java.util.*;

/**
 * Workflow definition builder for feed replication & retention.
 */
public class OozieFeedWorkflowBuilder extends OozieWorkflowBuilder<Feed> {

    @Override
    public Map<String, Properties> newWorkflowSchedule(Feed feed, List<String> clusters) throws FalconException {
        Map<String, Properties> propertiesMap = new HashMap<String, Properties>();

        for (String clusterName : clusters) {
            org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, clusterName);
            Properties properties = newWorkflowSchedule(feed, feedCluster.getValidity().getStart(), clusterName,
                    CurrentUser.getUser());
            if (properties == null) {
                continue;
            }
            propertiesMap.put(clusterName, properties);
        }
        return propertiesMap;
    }

    @Override
    public Properties newWorkflowSchedule(Feed feed, Date startDate, String clusterName, String user)
        throws FalconException {

        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, clusterName);
        if (!startDate.before(feedCluster.getValidity().getEnd())) {
            return null;
        }

        Cluster cluster = configStore.get(EntityType.CLUSTER, feedCluster.getName());
        Path bundlePath = new Path(ClusterHelper.getLocation(cluster, "staging"), EntityUtil.getStagingPath(feed));
        Feed feedClone = (Feed) feed.copy();
        EntityUtil.setStartDate(feedClone, clusterName, startDate);

        AbstractOozieEntityMapper<Feed> mapper = new OozieFeedMapper(feedClone);
        if (!mapper.map(cluster, bundlePath)) {
            return null;
        }
        return createAppProperties(clusterName, bundlePath, user);
    }

    @Override
    public Date getNextStartTime(Feed feed, String cluster, Date now) throws FalconException {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, cluster);
        return EntityUtil.getNextStartTime(feedCluster.getValidity().getStart(),
                feed.getFrequency(), feed.getTimezone(), now);
    }

    @Override
    public String[] getWorkflowNames(Feed entity) {
        return new String[]{
                EntityUtil.getWorkflowName(Tag.RETENTION, entity).toString(),
                EntityUtil.getWorkflowName(Tag.REPLICATION, entity).toString(), };
    }
}
