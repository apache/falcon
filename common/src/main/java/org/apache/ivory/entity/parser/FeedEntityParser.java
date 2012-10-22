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

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.ivory.IvoryException;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.FeedHelper;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityGraph;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Cluster;
import org.apache.ivory.entity.v0.feed.ClusterType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.feed.LocationType;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.expression.ExpressionHelper;
import org.apache.ivory.group.FeedGroup;
import org.apache.ivory.group.FeedGroupMap;
import org.apache.log4j.Logger;

public class FeedEntityParser extends EntityParser<Feed> {

    private static final Logger LOG = Logger.getLogger(FeedEntityParser.class);

    public FeedEntityParser() {
        super(EntityType.FEED);
    }

    @Override
    public void validate(Feed feed) throws IvoryException {
        if(feed.getTimezone() == null)
            feed.setTimezone(TimeZone.getTimeZone("UTC"));
        
        if (feed.getClusters() == null)
            throw new ValidationException("Feed should have atleast one cluster");

        for (Cluster cluster : feed.getClusters().getClusters()) {
            validateEntityExists(EntityType.CLUSTER, cluster.getName());
            validateClusterValidity(cluster.getValidity().getStart(), cluster.getValidity().getEnd(), cluster.getName());
            validateFeedCutOffPeriod(feed, cluster);
        }

        validateFeedPartitionExpression(feed);
        validateFeedClustersSize(feed);
        validateFeedGroups(feed);

        // Seems like a good enough entity object for a new one
        // But is this an update ?

        Feed oldFeed = ConfigurationStore.get().get(EntityType.FEED, feed.getName());
        if (oldFeed == null)
            return; // Not an update case

        // Is actually an update. Need to iterate over all the processes
        // depending on this feed and see if they are valid with the new
        // feed reference
        EntityGraph graph = EntityGraph.get();
        Set<Entity> referenced = graph.getDependents(oldFeed);
        Set<Process> processes = findProcesses(referenced);
        if (processes.isEmpty())
            return;

        ensureValidityFor(feed, processes);
    }

    private Set<Process> findProcesses(Set<Entity> referenced) {
        Set<Process> processes = new HashSet<Process>();
        for (Entity entity : referenced) {
            if (entity.getEntityType() == EntityType.PROCESS) {
                processes.add((Process) entity);
            }
        }
        return processes;
    }

    private void validateFeedGroups(Feed feed) throws ValidationException {
        String[] groupNames = feed.getGroups() != null ? feed.getGroups().split(",") : new String[] {};
        String defaultPath = FeedHelper.getLocation(feed, LocationType.DATA)
		.getPath();
		for (Cluster cluster : feed.getClusters().getClusters()) {
			if (!FeedGroup.getDatePattern(
					FeedHelper.getLocation(feed, LocationType.DATA,
							cluster.getName()).getPath()).equals(
					FeedGroup.getDatePattern(defaultPath))) {
				throw new ValidationException("Feeds default path pattern: "
						+ FeedHelper.getLocation(feed, LocationType.DATA)
								.getPath()
						+ ", does not match with cluster: "
						+ cluster.getName()
						+ " path pattern: "
						+ FeedHelper.getLocation(feed, LocationType.DATA,
								cluster.getName()).getPath());
			}
		}
        for (String groupName : groupNames) {
            FeedGroup group = FeedGroupMap.get().getGroupsMapping().get(groupName);
            if (group == null || group.canContainFeed(feed)) {
                continue;
            } else {
                throw new ValidationException("Feed " + feed.getName() + "'s frequency: " + feed.getFrequency().toString()
                        + ", path pattern: " + FeedHelper.getLocation(feed, LocationType.DATA).getPath()
                        + " does not match with group: " + group.getName() + "'s frequency: " + group.getFrequency()
                        + ", date pattern: " + group.getDatePattern());
            }
        }
    }

    private void ensureValidityFor(Feed newFeed, Set<Process> processes) throws IvoryException {
        for (Process process : processes) {
            try {
                ensureValidityFor(newFeed, process);
            } catch (IvoryException e) {
                throw new ValidationException("Process " + process.getName() + " is not compatible " + "with changes to feed "
                        + newFeed.getName(), e);
            }
        }
    }

    private void ensureValidityFor(Feed newFeed, Process process) throws IvoryException {
        for (org.apache.ivory.entity.v0.process.Cluster cluster : process.getClusters().getClusters()) {
            String clusterName = cluster.getName();
            if (process.getInputs() != null) {
                for (Input input : process.getInputs().getInputs()) {
                    if (!input.getFeed().equals(newFeed.getName()))
                        continue;
                    CrossEntityValidations.validateFeedDefinedForCluster(newFeed, clusterName);
                    CrossEntityValidations.validateFeedRetentionPeriod(input.getStart(), newFeed, clusterName);
                    CrossEntityValidations.validateInstanceRange(process, input, newFeed);

                    if (input.getPartition() != null) {
                        CrossEntityValidations.validateInputPartition(input, newFeed);
                    }
                }
            }

            if (process.getOutputs() != null) {
                for (Output output : process.getOutputs().getOutputs()) {
                    if (!output.getFeed().equals(newFeed.getName()))
                        continue;
                    CrossEntityValidations.validateFeedDefinedForCluster(newFeed, clusterName);
                    CrossEntityValidations.validateInstance(process, output, newFeed);
                }
            }
            LOG.debug("Verified and found " + process.getName() + " to be valid for new definition of " + newFeed.getName());
        }
    }

	private void validateFeedClustersSize(Feed feed)
			throws ValidationException {
		if (feed.getClusters().getClusters().size() == 0) {
			throw new ValidationException(
					"Feed should have atleast one cluster");
		}
	}

    private void validateClusterValidity(Date start, Date end, String clusterName) throws IvoryException {
        try {
            if (start.after(end)) {
                throw new ValidationException("Feed start time: " + start + " cannot be after feed end time: " + end
                        + " for cluster: " + clusterName);
            }
        } catch (ValidationException e) {
            throw new ValidationException(e);
        } catch (Exception e) {
            throw new IvoryException(e);
        }
    }

    private void validateFeedCutOffPeriod(Feed feed, Cluster cluster) throws IvoryException {
        ExpressionHelper evaluator = ExpressionHelper.get();

        String feedRetention = cluster.getRetention().getLimit().toString();
        long retentionPeriod = evaluator.evaluate(feedRetention, Long.class);

        if(feed.getLateArrival()==null){
        	LOG.debug("Feed's late arrival cut-off not set");
        	return;
        }
        String feedCutoff = feed.getLateArrival().getCutOff().toString();
        long feedCutOffPeriod = evaluator.evaluate(feedCutoff, Long.class);

        if (retentionPeriod < feedCutOffPeriod) {
            throw new ValidationException("Feed's retention limit: " + feedRetention + " of referenced cluster " + cluster.getName()
                    + " should be more than feed's late arrival cut-off period: " + feedCutoff + " for feed: " + feed.getName());
        }
    }
    
    private void validateFeedPartitionExpression(Feed feed) throws IvoryException {
        int numSourceClusters = 0, numTrgClusters = 0;
        Set<String> clusters = new HashSet<String>();
        for (Cluster cl : feed.getClusters().getClusters()) {
			if (!clusters.add(cl.getName())) {
				throw new ValidationException("Cluster: " + cl.getName()
						+ " is defined more than once for feed: "+feed.getName());
			}
            if (cl.getType() == ClusterType.SOURCE){
                numSourceClusters++;
            } else if(cl.getType() == ClusterType.TARGET) {
                numTrgClusters++;
            }
        }
        
		if (numTrgClusters >= 1 && numSourceClusters == 0) {
			throw new ValidationException("Feed: " + feed.getName()
					+ " should have atleast one source cluster defined");
		}
        
        int feedParts = feed.getPartitions() != null ? feed.getPartitions().getPartitions().size() : 0;
        
        for(Cluster cluster:feed.getClusters().getClusters()) {

            if(cluster.getType() == ClusterType.SOURCE && numSourceClusters > 1 && numTrgClusters >= 1) {
                String part = FeedHelper.normalizePartitionExpression(cluster.getPartition());
                if(StringUtils.split(part, '/').length == 0)
                    throw new ValidationException("Partition expression has to be specified for cluster " + cluster.getName() +
                            " as there are more than one source clusters");
                validateClusterExpDefined(cluster);

            } else if(cluster.getType() == ClusterType.TARGET) {

                for(Cluster src:feed.getClusters().getClusters()) {
                    if(src.getType() == ClusterType.SOURCE) {
                        String part = FeedHelper.normalizePartitionExpression(src.getPartition(), cluster.getPartition());
                        int numParts = StringUtils.split(part, '/').length;
                        if(numParts > feedParts)
                            throw new ValidationException("Partition for " + src.getName() + " and " + cluster.getName() + 
                                    "clusters is more than the number of partitions defined in feed");
                    }
                }
                
                if(numTrgClusters > 1 && numSourceClusters >= 1) {
                    String part = FeedHelper.normalizePartitionExpression(cluster.getPartition());
                    if(StringUtils.split(part, '/').length == 0)
                        throw new ValidationException("Partition expression has to be specified for cluster " + cluster.getName() +
                                " as there are more than one target clusters");
                    validateClusterExpDefined(cluster);                    
                }
                
            }
        }
    }

    private void validateClusterExpDefined(Cluster cl) throws IvoryException {
        if(cl.getPartition() == null)
            return;
        
        org.apache.ivory.entity.v0.cluster.Cluster cluster = (org.apache.ivory.entity.v0.cluster.Cluster) EntityUtil.getEntity(EntityType.CLUSTER, cl.getName());
        String part = FeedHelper.normalizePartitionExpression(cl.getPartition());
        if(FeedHelper.evaluateClusterExp(cluster, part).equals(part))
            throw new ValidationException("Alteast one of the partition tags has to be a cluster expression for cluster " + cl.getName()); 
    }
}
