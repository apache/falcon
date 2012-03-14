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

package org.apache.ivory.entity.parser;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.ivory.IvoryException;
import org.apache.ivory.Pair;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Cluster;
import org.apache.ivory.entity.v0.feed.ClusterType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.log4j.Logger;
import org.apache.oozie.util.DateUtils;

public class FeedEntityParser extends EntityParser<Feed> {

    private static final Logger LOG = Logger.getLogger(ProcessEntityParser.class);

    public FeedEntityParser() {
        super(EntityType.FEED);
    }
    
    @Override
    public void validate(Feed feed) throws IvoryException {
    	
    		validateXMLelements(feed);
    	
        if(feed.getClusters() == null || feed.getClusters().getCluster() == null)
            throw new ValidationException("Feed should have atleast one cluster");
        
        //validate on dependent clusters  
        List<Pair<EntityType, String>> entities = new ArrayList<Pair<EntityType,String>>();
		for (Cluster cluster : feed.getClusters().getCluster()) {
			 validateClusterValidity(cluster.getValidity().getStart(),cluster.getValidity().getEnd(),cluster.getName());
			entities.add(Pair.of(EntityType.CLUSTER, cluster.getName()));
		}
        validateEntitiesExist(entities);   
        validateFeedSourceCluster(feed);
       
    }

	private void validateXMLelements(Feed feed) throws ValidationException {

		for (Cluster cluster : feed.getClusters().getCluster()) {
			if(!EntityUtil.isValidUTCDate(cluster.getValidity().getStart())){
				 throw new ValidationException("Invalid start date: "+ cluster.getValidity().getStart()+" for cluster: "+cluster.getName());
			}
			if(!EntityUtil.isValidUTCDate(cluster.getValidity().getEnd())){
				 throw new ValidationException("Invalid end date: "+ cluster.getValidity().getEnd()+" for cluster: "+cluster.getName());
			}
		}		
	}
	
	private void validateFeedSourceCluster(Feed feed)
			throws ValidationException {
		int i = 0;
		for (Cluster cluster : feed.getClusters().getCluster()) {
			if (cluster.getType().equals(ClusterType.SOURCE)) {
				i++;
			}
		}
		if (i == 0)
			throw new ValidationException(
					"Feed should have atleast one source cluster");
		if (i > 1)
			throw new ValidationException(
					"Feed should not have more than one source cluster");
	}
	
	private void validateClusterValidity(String start, String end, String clusterName)
			throws IvoryException {
		try {
			Date processStart = DateUtils.parseDateUTC(start);
			Date processEnd = DateUtils.parseDateUTC(end);
			if (processStart.after(processEnd)) {
				throw new ValidationException("Feed start time: " + start
						+ " cannot be after feed end time: " + end + " for cluster: "+clusterName);
			}
		} catch (ValidationException e) {
			throw new ValidationException(e);
		} catch (Exception e) {
			throw new IvoryException(e);
		}
	}
}
