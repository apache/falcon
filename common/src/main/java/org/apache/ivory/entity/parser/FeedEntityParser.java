
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

import org.apache.ivory.IvoryException;
import org.apache.ivory.Pair;
import org.apache.ivory.entity.EntityUtil;
import org.apache.ivory.entity.store.ConfigurationStore;
import org.apache.ivory.entity.v0.Entity;
import org.apache.ivory.entity.v0.EntityGraph;
import org.apache.ivory.entity.v0.EntityType;
import org.apache.ivory.entity.v0.feed.Cluster;
import org.apache.ivory.entity.v0.feed.ClusterType;
import org.apache.ivory.entity.v0.feed.Feed;
import org.apache.ivory.entity.v0.process.Input;
import org.apache.ivory.entity.v0.process.Output;
import org.apache.ivory.entity.v0.process.Process;
import org.apache.ivory.expression.ExpressionHelper;
import org.apache.ivory.group.FeedGroupMap;
import org.apache.ivory.group.FeedGroup;
import org.apache.log4j.Logger;

import java.util.*;

public class FeedEntityParser extends EntityParser<Feed> {

	private static final Logger LOG = Logger.getLogger(FeedEntityParser.class);

	public FeedEntityParser() {
		super(EntityType.FEED);
	}

	@Override
	public void validate(Feed feed) throws IvoryException {

		validateXMLelements(feed);

		if (feed.getClusters() == null
				|| feed.getClusters().getCluster() == null)
			throw new ValidationException(
					"Feed should have atleast one cluster");

		// validate on dependent clusters
		List<Pair<EntityType, String>> entities = new ArrayList<Pair<EntityType, String>>();
		for (Cluster cluster : feed.getClusters().getCluster()) {
			validateClusterValidity(cluster.getValidity().getStart(), cluster
					.getValidity().getEnd(), cluster.getName());
			validateFeedCutOffPeriod(feed, cluster);
			validateFeedPartitionExpression(feed, cluster);
			entities.add(Pair.of(EntityType.CLUSTER, cluster.getName()));
		}

		validateEntitiesExist(entities);
		validateFeedSourceCluster(feed);
		validateFeedGroups(feed);

        //Seems like a good enough entity object for a new one
        //But is this an update ?

        Feed oldFeed = ConfigurationStore.get().get(EntityType.FEED, feed.getName());
        if (oldFeed == null) return; //Not an update case

        //Is actually an update. Need to iterate over all the processes
        //depending on this feed and see if they are valid with the new
        //feed reference
        EntityGraph graph = EntityGraph.get();
        Set<Entity> referenced = graph.getDependents(oldFeed);
        Set<Process> processes = findProcesses(referenced);
        if (processes.isEmpty()) return;

        ensureValidityFor(feed, processes);
    }

    private Set<Process> findProcesses(Set<Entity> referenced) {
        Set<Process> processes = new HashSet<Process>();
        for (Entity entity : referenced) {
            if (entity.getEntityType() == EntityType.PROCESS) {
                processes.add((Process)entity);
            }
        }
        return processes;
    }

	private void validateFeedGroups(Feed feed) throws ValidationException {
		String[] groupNames = feed.getGroups() != null ? feed.getGroups()
				.split(",") : new String[] {};
		for (String groupName : groupNames) {
			FeedGroup group = FeedGroupMap.get().getGroupsMapping()
					.get(groupName);
			if (group == null || group.canContainFeed(feed)) {
				continue;
			} else {
				throw new ValidationException("Feed " + feed.getName()
						+ "'s frequency: " + feed.getFrequency()
						+ ", periodicity: " + feed.getPeriodicity()
						+ ", path pattern: " + feed.getDataPath()
						+ " does not match with group: " + group.getName()
						+ "'s frequency: " + group.getFrequency()
						+ ", periodicity: " + group.getPeriodicity()
						+ ", date pattern: " + group.getDatePattern());
			}
		}
	}

    private void ensureValidityFor(Feed newFeed, Set<Process> processes) throws IvoryException {
        for (Process process : processes) {
            try {
                ensureValidityFor(newFeed, process);
            } catch (IvoryException e) {
                throw new ValidationException("Process " + process.getName() + " is not compatible " +
                        "with changes to feed " + newFeed.getName(), e);
            }

            Date newEndDate = EntityUtil.parseDateUTC(process.getValidity().getEnd());
            if (newEndDate.before(new Date())) {
                throw new ValidationException("End time for " + process.getName() +
                        " is in the past. Entity can't be updated. Use remove and add," +
                        " before feed " + newFeed.getName() + " is updated");
            }
        }
    }

    private void ensureValidityFor(Feed newFeed, Process process) throws IvoryException {
        String clusterName = process.getCluster().getName();
        if (process.getInputs() != null && process.getInputs().getInput() != null) {
            for (Input input : process.getInputs().getInput()) {
                if (!input.getFeed().equals(newFeed.getName())) continue;
                CrossEntityValidations.validateFeedDefinedForCluster(newFeed, clusterName);
                CrossEntityValidations.validateFeedRetentionPeriod(input.getStartInstance(),
                        newFeed, clusterName);
                CrossEntityValidations.validateInstanceRange(process, input, newFeed);

                if (input.getPartition() != null) {
                    CrossEntityValidations.validateInputPartition(input, newFeed);
                }
            }
        }

        if (process.getOutputs() != null && process.getOutputs().getOutput() != null) {
            for (Output output : process.getOutputs().getOutput()) {
                if (!output.getFeed().equals(newFeed.getName())) continue;
                CrossEntityValidations.validateFeedDefinedForCluster(newFeed, clusterName);
                CrossEntityValidations.validateInstance(process, output, newFeed);
            }
        }
        LOG.debug("Verified and found " + process.getName() + " to be valid for new definition of " +
                newFeed.getName());
    }

  	private void validateXMLelements(Feed feed) throws ValidationException {

		for (Cluster cluster : feed.getClusters().getCluster()) {
			if (!EntityUtil.isValidUTCDate(cluster.getValidity().getStart())) {
				throw new ValidationException("Invalid start date: "
						+ cluster.getValidity().getStart() + " for cluster: "
						+ cluster.getName());
			}
			if (!EntityUtil.isValidUTCDate(cluster.getValidity().getEnd())) {
				throw new ValidationException("Invalid end date: "
						+ cluster.getValidity().getEnd() + " for cluster: "
						+ cluster.getName());
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

	private void validateClusterValidity(String start, String end,
			String clusterName) throws IvoryException {
		try {
			Date processStart = EntityUtil.parseDateUTC(start);
			Date processEnd = EntityUtil.parseDateUTC(end);
			if (processStart.after(processEnd)) {
				throw new ValidationException("Feed start time: " + start
						+ " cannot be after feed end time: " + end
						+ " for cluster: " + clusterName);
			}
		} catch (ValidationException e) {
			throw new ValidationException(e);
		} catch (Exception e) {
			throw new IvoryException(e);
		}
	}

	private void validateFeedCutOffPeriod(Feed feed, Cluster cluster)
			throws IvoryException {
		ExpressionHelper evaluator = ExpressionHelper.get();

		String feedRetention = cluster.getRetention().getLimit();
		long retentionPeriod = evaluator.evaluate(feedRetention, Long.class);

		String feedCutoff = feed.getLateArrival().getCutOff();
		long feedCutOffPeriod = evaluator.evaluate(feedCutoff, Long.class);

		if (retentionPeriod < feedCutOffPeriod) {
			throw new ValidationException(
					"Feed's retention limit: "
							+ feedRetention
							+ " of referenced cluster "
							+ cluster.getName()
							+ " should be more than feed's late arrival cut-off period: "
							+ feedCutoff + " for feed: " + feed.getName());
		}
	}
	
	private void loadClusterProperties(Properties prop, org.apache.ivory.entity.v0.cluster.Cluster cluster)
	{
		prop.put("${cluster.colo}", cluster.getColo());
		prop.put("${cluster.name}", cluster.getName());
		Set<String> keyset = cluster.getProperties().keySet();
		for(String propName : keyset)
		{
			prop.put("${cluster." + propName + "}", cluster.getProperties().get(propName).getValue());
		}
	}
	
	private void validateFeedPartitionExpression(Feed feed, Cluster cluster)
			throws IvoryException {
		int expressions = 0 , numSourceClusters = 0;
		for(Cluster cl : feed.getClusters().getCluster()){
			if(cl.getType().value().equals("source"))
				numSourceClusters++;
		}
		if (cluster.getType().value() == "source"
				&& feed.getPartitions() != null
				&& feed.getPartitions().getPartition().size() != 0
				&& cluster.getPartitionExp() != null) {
			String[] tokens = cluster.getPartitionExp().split("/");
			if (tokens.length != feed.getPartitions().getPartition().size()) {
				throw new ValidationException(
						"Number of expressions in Partition Expression are not equal to number of partions");
			} else {
				org.apache.ivory.entity.v0.cluster.Cluster clusterEntity = ConfigurationStore
						.get().get(EntityType.CLUSTER, cluster.getName());
				Properties properties = new Properties();
				loadClusterProperties(properties, clusterEntity);
				ExpressionHelper expHelp = ExpressionHelper.get();
				expHelp.setPropertiesForVariable(properties);
				for (String token : tokens) {
					String val = expHelp.evaluateFullExpression(token,
							String.class);
					if (!val.equals(token)) {
						expressions++;
						break;
					}
				}
				if (expressions == 0)
					throw new ValidationException(
							"Alteast one of the partition tags has to be an expression");
			}
		} else {
			if (cluster.getPartitionExp() != null
					&& cluster.getType().value() == "target")
				throw new ValidationException(
						"Target Cluster do not have Partition Expression");
			else if (feed.getPartitions() != null
					&& feed.getPartitions().getPartition().size() != 0
					&& cluster.getPartitionExp() == null && cluster.getType().value() == "source" && numSourceClusters > 1)
				throw new ValidationException("Partition Expression is missing for the cluster: " + cluster.getName());
			else if (feed.getPartitions() == null
					&& cluster.getPartitionExp() != null)
				throw new ValidationException(
						"Partition Expression not expected");
		}

	}
}
