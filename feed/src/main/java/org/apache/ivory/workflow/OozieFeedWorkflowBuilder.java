/*
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

package org.apache.ivory.workflow;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.ivory.IvoryException;
import org.apache.ivory.Tag;
import org.apache.ivory.converter.AbstractOozieEntityMapper;
import org.apache.ivory.converter.OozieFeedMapper;
import org.apache.ivory.entity.ClusterHelper;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.parser.Frequency;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.cluster.Cluster;
import org.apache.ivory.entity.v0.feed.Feed;

public class OozieFeedWorkflowBuilder extends OozieWorkflowBuilder<Feed> {

    @Override
    public Map<String, Properties> newWorkflowSchedule(Feed feed, List<String> clusters) throws IvoryException {
        Map<String, Path> pathsMap = new HashMap<String, Path>();
        
        for (String clusterName: clusters) {
            org.apache.ivory.entity.v0.feed.Cluster feedCluster = feed.getCluster(clusterName);
            if (!EntityUtil.parseDateUTC(feedCluster.getValidity().getStart()).before(
                    EntityUtil.parseDateUTC(feedCluster.getValidity().getEnd())))
                // start time >= end time
                continue;

            Cluster cluster = configStore.get(EntityType.CLUSTER, feedCluster.getName());
            Path bundlePath = new Path(ClusterHelper.getLocation(cluster, "staging"), EntityUtil.getStagingPath(feed));

            AbstractOozieEntityMapper<Feed> mapper = new OozieFeedMapper(feed);
            if(mapper.map(cluster, bundlePath)==false){
            	continue;
            }
            pathsMap.put(clusterName, bundlePath);
        }
        return createAppProperties(pathsMap);
    }

    @Override
    public Date getNextStartTime(Feed feed, String cluster, Date now) throws IvoryException {
        org.apache.ivory.entity.v0.feed.Cluster feedCluster = feed.getCluster(cluster);
        return EntityUtil.getNextStartTime(EntityUtil.parseDateUTC(feedCluster.getValidity().getStart()),
                Frequency.valueOf(feed.getFrequency()), feed.getPeriodicity(), feedCluster.getValidity().getTimezone(), now);
    }

	@Override
	public String[] getWorkflowNames(Feed entity) {
		return new String[] {
				EntityUtil.getWorkflowName(Tag.RETENTION, entity).toString(),
				EntityUtil.getWorkflowName(Tag.REPLICATION, entity).toString() };
	}
}