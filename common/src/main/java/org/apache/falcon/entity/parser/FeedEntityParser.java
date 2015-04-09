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

package org.apache.falcon.entity.parser;

import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.catalog.CatalogServiceFactory;
import org.apache.falcon.entity.*;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityGraph;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.ACL;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.Sla;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.group.FeedGroup;
import org.apache.falcon.group.FeedGroupMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.List;

/**
 * Parser that parses feed entity definition.
 */
public class FeedEntityParser extends EntityParser<Feed> {

    private static final Logger LOG = LoggerFactory.getLogger(FeedEntityParser.class);

    public FeedEntityParser() {
        super(EntityType.FEED);
    }

    @Override
    public void validate(Feed feed) throws FalconException {
        if (feed.getTimezone() == null) {
            feed.setTimezone(TimeZone.getTimeZone("UTC"));
        }

        if (feed.getClusters() == null) {
            throw new ValidationException("Feed should have at least one cluster");
        }

        validateACL(feed);
        for (Cluster cluster : feed.getClusters().getClusters()) {
            validateEntityExists(EntityType.CLUSTER, cluster.getName());
            validateClusterValidity(cluster.getValidity().getStart(), cluster.getValidity().getEnd(),
                    cluster.getName());
            validateClusterHasRegistry(feed, cluster);
            validateFeedCutOffPeriod(feed, cluster);
        }

        validateFeedStorage(feed);
        validateFeedPath(feed);
        validateFeedPartitionExpression(feed);
        validateFeedGroups(feed);
        validateFeedSLA(feed);
        validateACL(feed);

        // Seems like a good enough entity object for a new one
        // But is this an update ?

        Feed oldFeed = ConfigurationStore.get().get(EntityType.FEED, feed.getName());
        if (oldFeed == null) {
            return; // Not an update case
        }

        // Is actually an update. Need to iterate over all the processes
        // depending on this feed and see if they are valid with the new
        // feed reference
        EntityGraph graph = EntityGraph.get();
        Set<Entity> referenced = graph.getDependents(oldFeed);
        Set<Process> processes = findProcesses(referenced);
        if (processes.isEmpty()) {
            return;
        }

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

    private void validateFeedSLA(Feed feed) throws FalconException {
        for (Cluster cluster : feed.getClusters().getClusters()) {
            Sla clusterSla = FeedHelper.getSLAs(cluster, feed);
            if (clusterSla != null) {
                Frequency slaLowExpression = clusterSla.getSlaLow();
                ExpressionHelper evaluator = ExpressionHelper.get();
                ExpressionHelper.setReferenceDate(new Date());
                Date slaLow = new Date(evaluator.evaluate(slaLowExpression.toString(), Long.class));

                Frequency slaHighExpression = clusterSla.getSlaHigh();
                Date slaHigh = new Date(evaluator.evaluate(slaHighExpression.toString(), Long.class));

                if (slaLow.after(slaHigh)) {
                    throw new ValidationException("slaLow of Feed: " + slaLowExpression
                            + "is greater than slaHigh: " + slaHighExpression
                            + " for cluster: " + cluster.getName()
                    );
                }

                // test that slaHigh is less than retention
                Frequency retentionExpression = cluster.getRetention().getLimit();
                Date retention = new Date(evaluator.evaluate(retentionExpression.toString(), Long.class));
                if (slaHigh.after(retention)) {
                    throw new ValidationException("slaHigh of Feed: " + slaHighExpression
                            + " is greater than retention of the feed: " + retentionExpression
                            + " for cluster: " + cluster.getName()
                    );
                }


            }
        }
    }

    private void validateFeedGroups(Feed feed) throws FalconException {
        String[] groupNames = feed.getGroups() != null ? feed.getGroups().split(",") : new String[]{};
        final Storage storage = FeedHelper.createStorage(feed);
        String defaultPath = storage.getUriTemplate(LocationType.DATA);
        for (Cluster cluster : feed.getClusters().getClusters()) {
            final String uriTemplate = FeedHelper.createStorage(cluster, feed).getUriTemplate(LocationType.DATA);
            if (!FeedGroup.getDatePattern(uriTemplate).equals(
                    FeedGroup.getDatePattern(defaultPath))) {
                throw new ValidationException("Feeds default path pattern: "
                        + storage.getUriTemplate(LocationType.DATA)
                        + ", does not match with cluster: "
                        + cluster.getName()
                        + " path pattern: "
                        + uriTemplate);
            }
        }
        for (String groupName : groupNames) {
            FeedGroup group = FeedGroupMap.get().getGroupsMapping().get(groupName);
            if (group != null && !group.canContainFeed(feed)) {
                throw new ValidationException(
                        "Feed " + feed.getName() + "'s frequency: " + feed.getFrequency().toString()
                                + ", path pattern: " + storage
                                + " does not match with group: " + group.getName() + "'s frequency: "
                                + group.getFrequency()
                                + ", date pattern: " + group.getDatePattern());
            }
        }
    }

    private void ensureValidityFor(Feed newFeed, Set<Process> processes) throws FalconException {
        for (Process process : processes) {
            try {
                ensureValidityFor(newFeed, process);
            } catch (FalconException e) {
                throw new ValidationException(
                        "Process " + process.getName() + " is not compatible " + "with changes to feed "
                                + newFeed.getName(), e);
            }
        }
    }

    private void ensureValidityFor(Feed newFeed, Process process) throws FalconException {
        for (org.apache.falcon.entity.v0.process.Cluster cluster : process.getClusters().getClusters()) {
            String clusterName = cluster.getName();
            if (process.getInputs() != null) {
                for (Input input : process.getInputs().getInputs()) {
                    if (!input.getFeed().equals(newFeed.getName())) {
                        continue;
                    }
                    CrossEntityValidations.validateFeedDefinedForCluster(newFeed, clusterName);
                    CrossEntityValidations.validateFeedRetentionPeriod(input.getStart(), newFeed, clusterName);
                    CrossEntityValidations.validateInstanceRange(process, input, newFeed);

                    validateInputPartition(newFeed, input);
                }
            }

            if (process.getOutputs() != null) {
                for (Output output : process.getOutputs().getOutputs()) {
                    if (!output.getFeed().equals(newFeed.getName())) {
                        continue;
                    }
                    CrossEntityValidations.validateFeedDefinedForCluster(newFeed, clusterName);
                    CrossEntityValidations.validateInstance(process, output, newFeed);
                }
            }
            LOG.debug("Verified and found {} to be valid for new definition of {}",
                    process.getName(), newFeed.getName());
        }
    }

    private void validateInputPartition(Feed newFeed, Input input) throws FalconException {
        if (input.getPartition() == null) {
            return;
        }

        final Storage.TYPE baseFeedStorageType = FeedHelper.getStorageType(newFeed);
        if (baseFeedStorageType == Storage.TYPE.FILESYSTEM) {
            CrossEntityValidations.validateInputPartition(input, newFeed);
        } else if (baseFeedStorageType == Storage.TYPE.TABLE) {
            throw new ValidationException("Input partitions are not supported for table storage: " + input.getName());
        }
    }

    private void validateClusterValidity(Date start, Date end, String clusterName) throws FalconException {
        try {
            if (start.after(end)) {
                throw new ValidationException("Feed start time: " + start + " cannot be after feed end time: " + end
                        + " for cluster: " + clusterName);
            }
        } catch (ValidationException e) {
            throw new ValidationException(e);
        } catch (Exception e) {
            throw new FalconException(e);
        }
    }

    private void validateFeedCutOffPeriod(Feed feed, Cluster cluster) throws FalconException {
        ExpressionHelper evaluator = ExpressionHelper.get();

        String feedRetention = cluster.getRetention().getLimit().toString();
        long retentionPeriod = evaluator.evaluate(feedRetention, Long.class);

        if (feed.getLateArrival() == null) {
            LOG.debug("Feed's late arrival cut-off not set");
            return;
        }
        String feedCutoff = feed.getLateArrival().getCutOff().toString();
        long feedCutOffPeriod = evaluator.evaluate(feedCutoff, Long.class);

        if (retentionPeriod < feedCutOffPeriod) {
            throw new ValidationException(
                    "Feed's retention limit: " + feedRetention + " of referenced cluster " + cluster.getName()
                            + " should be more than feed's late arrival cut-off period: " + feedCutoff + " for feed: "
                            + feed.getName());
        }
    }

    private void validateFeedPartitionExpression(Feed feed) throws FalconException {
        int numSourceClusters = 0, numTrgClusters = 0;
        Set<String> clusters = new HashSet<String>();
        for (Cluster cl : feed.getClusters().getClusters()) {
            if (!clusters.add(cl.getName())) {
                throw new ValidationException("Cluster: " + cl.getName()
                        + " is defined more than once for feed: " + feed.getName());
            }
            if (cl.getType() == ClusterType.SOURCE) {
                numSourceClusters++;
            } else if (cl.getType() == ClusterType.TARGET) {
                numTrgClusters++;
            }
        }

        if (numTrgClusters >= 1 && numSourceClusters == 0) {
            throw new ValidationException("Feed: " + feed.getName()
                    + " should have atleast one source cluster defined");
        }

        int feedParts = feed.getPartitions() != null ? feed.getPartitions().getPartitions().size() : 0;

        for (Cluster cluster : feed.getClusters().getClusters()) {

            if (cluster.getType() == ClusterType.SOURCE && numSourceClusters > 1 && numTrgClusters >= 1) {
                String part = FeedHelper.normalizePartitionExpression(cluster.getPartition());
                if (StringUtils.split(part, '/').length == 0) {
                    throw new ValidationException(
                            "Partition expression has to be specified for cluster " + cluster.getName()
                                    + " as there are more than one source clusters");
                }
                validateClusterExpDefined(cluster);

            } else if (cluster.getType() == ClusterType.TARGET) {

                for (Cluster src : feed.getClusters().getClusters()) {
                    if (src.getType() == ClusterType.SOURCE) {
                        String part = FeedHelper.normalizePartitionExpression(src.getPartition(),
                                cluster.getPartition());
                        int numParts = StringUtils.split(part, '/').length;
                        if (numParts > feedParts) {
                            throw new ValidationException(
                                    "Partition for " + src.getName() + " and " + cluster.getName()
                                            + "clusters is more than the number of partitions defined in feed");
                        }
                    }
                }

                if (numTrgClusters > 1 && numSourceClusters >= 1) {
                    validateClusterExpDefined(cluster);
                }
            }
        }
    }

    private void validateClusterExpDefined(Cluster cl) throws FalconException {
        if (cl.getPartition() == null) {
            return;
        }

        org.apache.falcon.entity.v0.cluster.Cluster cluster = EntityUtil.getEntity(EntityType.CLUSTER, cl.getName());
        String part = FeedHelper.normalizePartitionExpression(cl.getPartition());
        if (FeedHelper.evaluateClusterExp(cluster, part).equals(part)) {
            throw new ValidationException(
                    "Alteast one of the partition tags has to be a cluster expression for cluster " + cl.getName());
        }
    }

    /**
     * Ensure table is already defined in the catalog registry.
     * Does not matter for FileSystem storage.
     */
    private void validateFeedStorage(Feed feed) throws FalconException {
        final Storage.TYPE baseFeedStorageType = FeedHelper.getStorageType(feed);
        validateMultipleSourcesExist(feed, baseFeedStorageType);
        validateUniformStorageType(feed, baseFeedStorageType);
        validatePartitions(feed, baseFeedStorageType);
        validateStorageExists(feed);
    }

    private void validateMultipleSourcesExist(Feed feed, Storage.TYPE baseFeedStorageType) throws FalconException {
        if (baseFeedStorageType == Storage.TYPE.FILESYSTEM) {
            return;
        }

        // validate that there is only one source cluster
        int numberOfSourceClusters = 0;
        for (Cluster cluster : feed.getClusters().getClusters()) {
            if (cluster.getType() == ClusterType.SOURCE) {
                numberOfSourceClusters++;
            }
        }

        if (numberOfSourceClusters > 1) {
            throw new ValidationException("Multiple sources are not supported for feed with table storage: "
                    + feed.getName());
        }
    }

    private void validateUniformStorageType(Feed feed, Storage.TYPE feedStorageType) throws FalconException {
        for (Cluster cluster : feed.getClusters().getClusters()) {
            Storage.TYPE feedClusterStorageType = FeedHelper.getStorageType(feed, cluster);

            if (feedStorageType != feedClusterStorageType) {
                throw new ValidationException("The storage type is not uniform for cluster: " + cluster.getName());
            }
        }
    }

    private void validateClusterHasRegistry(Feed feed, Cluster cluster) throws FalconException {
        Storage.TYPE feedClusterStorageType = FeedHelper.getStorageType(feed, cluster);

        if (feedClusterStorageType != Storage.TYPE.TABLE) {
            return;
        }

        org.apache.falcon.entity.v0.cluster.Cluster clusterEntity = EntityUtil.getEntity(EntityType.CLUSTER,
                cluster.getName());
        if (ClusterHelper.getRegistryEndPoint(clusterEntity) == null) {
            throw new ValidationException("Cluster should have registry interface defined: " + clusterEntity.getName());
        }
    }

    private void validatePartitions(Feed feed, Storage.TYPE storageType) throws  FalconException {
        if (storageType == Storage.TYPE.TABLE && feed.getPartitions() != null) {
            throw new ValidationException("Partitions are not supported for feeds with table storage. "
                    + "It should be defined as part of the table URI. "
                    + feed.getName());
        }
    }

    private void validateStorageExists(Feed feed) throws FalconException {
        StringBuilder buffer = new StringBuilder();
        for (Cluster cluster : feed.getClusters().getClusters()) {
            org.apache.falcon.entity.v0.cluster.Cluster clusterEntity =
                    EntityUtil.getEntity(EntityType.CLUSTER, cluster.getName());
            if (!EntityUtil.responsibleFor(clusterEntity.getColo())) {
                continue;
            }

            final Storage storage = FeedHelper.createStorage(cluster, feed);
            // this is only true for table, filesystem always returns true
            if (storage.getType() == Storage.TYPE.FILESYSTEM) {
                continue;
            }

            CatalogStorage catalogStorage = (CatalogStorage) storage;
            Configuration clusterConf = ClusterHelper.getConfiguration(clusterEntity);
            if (!CatalogServiceFactory.getCatalogService().tableExists(
                    clusterConf, catalogStorage.getCatalogUrl(),
                    catalogStorage.getDatabase(), catalogStorage.getTable())) {
                buffer.append("Table [")
                        .append(catalogStorage.getTable())
                        .append("] does not exist for feed: ")
                        .append(feed.getName())
                        .append(" in cluster: ")
                        .append(cluster.getName());
            }
        }

        if (buffer.length() > 0) {
            throw new ValidationException(buffer.toString());
        }
    }

    /**
     * Validate ACL if authorization is enabled.
     *
     * @param feed Feed entity
     * @throws ValidationException
     */
    private void validateACL(Feed feed) throws FalconException {
        if (isAuthorizationDisabled) {
            return;
        }

        final ACL feedACL = feed.getACL();
        validateACLOwnerAndGroup(feedACL);
        try {
            authorize(feed.getName(), feedACL);
        } catch (AuthorizationException e) {
            throw new ValidationException(e);
        }

        for (Cluster cluster : feed.getClusters().getClusters()) {
            org.apache.falcon.entity.v0.cluster.Cluster clusterEntity =
                    EntityUtil.getEntity(EntityType.CLUSTER, cluster.getName());
            if (!EntityUtil.responsibleFor(clusterEntity.getColo())) {
                continue;
            }

            final Storage storage = FeedHelper.createStorage(cluster, feed);
            try {
                storage.validateACL(feedACL);
            } catch(FalconException e) {
                throw new ValidationException(e);
            }
        }
    }

    /**
     * Validate if FileSystem based feed contains location type data.
     *
     * @param feed Feed entity
     * @throws FalconException
     */
    private void validateFeedPath(Feed feed) throws FalconException {
        if (FeedHelper.getStorageType(feed) == Storage.TYPE.TABLE) {
            return;
        }

        for (Cluster cluster : feed.getClusters().getClusters()) {
            List<Location> locations = FeedHelper.getLocations(cluster, feed);
            Location dataLocation = FileSystemStorage.getLocation(locations, LocationType.DATA);

            if (dataLocation == null) {
                throw new ValidationException(feed.getName() + " is a FileSystem based feed "
                    + "but it doesn't contain location type - data in cluster " + cluster.getName());
            }

        }
    }
}
