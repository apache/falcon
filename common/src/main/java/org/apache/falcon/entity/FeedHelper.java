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

package org.apache.falcon.entity;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.expression.ExpressionHelper;

public class FeedHelper {
    public static Cluster getCluster(Feed feed, String clusterName) {
        for(Cluster cluster:feed.getClusters().getClusters())
            if(cluster.getName().equals(clusterName))
                return cluster;
        return null;
    }
    
	public static Location getLocation(Feed feed, LocationType type,
			String clusterName) {
		Cluster cluster = getCluster(feed, clusterName);
		if (cluster!=null &&cluster.getLocations() != null 
				&& cluster.getLocations() .getLocations().size() != 0) {
			return getLocation(cluster.getLocations() , type);
		}
		else{
			return getLocation(feed.getLocations(), type);
		}

	}
	
	public static Location getLocation(Feed feed, LocationType type) {
		return getLocation(feed.getLocations(), type);
	}

	public static Location getLocation(Locations locations, LocationType type) {
		for (Location loc : locations.getLocations()) {
			if (loc.getType() == type) {
				return loc;
			}
		}
		Location loc = new Location();
		loc.setPath("/tmp");
		loc.setType(type);
		return loc;
	}
    
    public static String normalizePartitionExpression(String part1, String part2) {
        String partExp = StringUtils.stripToEmpty(part1) + "/" + StringUtils.stripToEmpty(part2);
        partExp = partExp.replaceAll("//+", "/");
        partExp = StringUtils.stripStart(partExp, "/");
        partExp = StringUtils.stripEnd(partExp, "/");
        return partExp;
    }

    public static String normalizePartitionExpression(String partition) {
        return normalizePartitionExpression(partition, null);
    }    
    
    private static Properties loadClusterProperties(org.apache.falcon.entity.v0.cluster.Cluster cluster) {
        Properties properties = new Properties();
        Map<String, String> clusterVars = new HashMap<String, String>();
        clusterVars.put("colo", cluster.getColo());
        clusterVars.put("name", cluster.getName());
        if (cluster.getProperties() != null) {
            for (Property property : cluster.getProperties().getProperties())
                clusterVars.put(property.getName(), property.getValue());
        }
        properties.put("cluster", clusterVars);
        return properties;
    }

    public static String evaluateClusterExp(org.apache.falcon.entity.v0.cluster.Cluster clusterEntity, String exp)
            throws FalconException {
        Properties properties = loadClusterProperties(clusterEntity);
        ExpressionHelper expHelp = ExpressionHelper.get();
        expHelp.setPropertiesForVariable(properties);
        return expHelp.evaluateFullExpression(exp, String.class);
    }
}
