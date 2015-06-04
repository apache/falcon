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

import org.apache.commons.lang3.StringUtils;

import org.apache.falcon.FalconException;
import org.apache.falcon.LifeCycle;
import org.apache.falcon.Tag;
import org.apache.falcon.entity.common.FeedDataPath;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Property;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.feed.Sla;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.falcon.resource.SchedulableEntityInstance;
import org.apache.falcon.util.BuildProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;

/**
 * Feed entity helper methods.
 */
public final class FeedHelper {

    private static final Logger LOG = LoggerFactory.getLogger(FeedHelper.class);
    private static final int ONE_MS = 1;

    public static final String FORMAT = "yyyyMMddHHmm";

    private FeedHelper() {}

    public static Cluster getCluster(Feed feed, String clusterName) {
        for (Cluster cluster : feed.getClusters().getClusters()) {
            if (cluster.getName().equals(clusterName)) {
                return cluster;
            }
        }
        return null;
    }

    public static Storage createStorage(Feed feed) throws FalconException {

        final Locations feedLocations = feed.getLocations();
        if (feedLocations != null
                && feedLocations.getLocations().size() != 0) {
            return new FileSystemStorage(feed);
        }

        try {
            final CatalogTable table = feed.getTable();
            if (table != null) {
                return new CatalogStorage(feed);
            }
        } catch (URISyntaxException e) {
            throw new FalconException(e);
        }

        throw new FalconException("Both catalog and locations are not defined.");
    }

    public static Storage createStorage(org.apache.falcon.entity.v0.cluster.Cluster clusterEntity,
                                        Feed feed) throws FalconException {
        return createStorage(getCluster(feed, clusterEntity.getName()), feed, clusterEntity);
    }

    public static Storage createStorage(String clusterName, Feed feed)
        throws FalconException {

        return createStorage(getCluster(feed, clusterName), feed);
    }

    public static Storage createStorage(Cluster cluster, Feed feed)
        throws FalconException {

        final org.apache.falcon.entity.v0.cluster.Cluster clusterEntity =
                EntityUtil.getEntity(EntityType.CLUSTER, cluster.getName());

        return createStorage(cluster, feed, clusterEntity);
    }

    public static Storage createStorage(Cluster cluster, Feed feed,
                                        org.apache.falcon.entity.v0.cluster.Cluster clusterEntity)
        throws FalconException {

        final List<Location> locations = getLocations(cluster, feed);
        if (locations != null) {
            return new FileSystemStorage(ClusterHelper.getStorageUrl(clusterEntity), locations);
        }

        try {
            final CatalogTable table = getTable(cluster, feed);
            if (table != null) {
                return new CatalogStorage(clusterEntity, table);
            }
        } catch (URISyntaxException e) {
            throw new FalconException(e);
        }

        throw new FalconException("Both catalog and locations are not defined.");
    }

    /**
     * Factory method to dole out a storage instance used for replication source.
     *
     * @param clusterEntity cluster entity
     * @param feed feed entity
     * @return an implementation of Storage
     * @throws FalconException
     */
    public static Storage createReadOnlyStorage(org.apache.falcon.entity.v0.cluster.Cluster clusterEntity,
                                                Feed feed) throws FalconException {
        Cluster feedCluster = getCluster(feed, clusterEntity.getName());
        final List<Location> locations = getLocations(feedCluster, feed);
        if (locations != null) {
            return new FileSystemStorage(ClusterHelper.getReadOnlyStorageUrl(clusterEntity), locations);
        }

        try {
            final CatalogTable table = getTable(feedCluster, feed);
            if (table != null) {
                return new CatalogStorage(clusterEntity, table);
            }
        } catch (URISyntaxException e) {
            throw new FalconException(e);
        }

        throw new FalconException("Both catalog and locations are not defined.");
    }

    public static Storage createStorage(String type, String storageUriTemplate)
        throws URISyntaxException {

        Storage.TYPE storageType = Storage.TYPE.valueOf(type);
        if (storageType == Storage.TYPE.FILESYSTEM) {
            return new FileSystemStorage(storageUriTemplate);
        } else if (storageType == Storage.TYPE.TABLE) {
            return new CatalogStorage(storageUriTemplate);
        }

        throw new IllegalArgumentException("Bad type: " + type);
    }

    public static Storage createStorage(String type, String storageUriTemplate,
                                        Configuration conf) throws URISyntaxException {
        Storage.TYPE storageType = Storage.TYPE.valueOf(type);
        if (storageType == Storage.TYPE.FILESYSTEM) {
            return new FileSystemStorage(storageUriTemplate);
        } else if (storageType == Storage.TYPE.TABLE) {
            return new CatalogStorage(storageUriTemplate, conf);
        }

        throw new IllegalArgumentException("Bad type: " + type);
    }

    public static Storage.TYPE getStorageType(Feed feed) throws FalconException {
        final Locations feedLocations = feed.getLocations();
        if (feedLocations != null
                && feedLocations.getLocations().size() != 0) {
            return Storage.TYPE.FILESYSTEM;
        }

        final CatalogTable table = feed.getTable();
        if (table != null) {
            return Storage.TYPE.TABLE;
        }

        throw new FalconException("Both catalog and locations are not defined.");
    }

    public static Storage.TYPE getStorageType(Feed feed,
                                              Cluster cluster) throws FalconException {
        final List<Location> locations = getLocations(cluster, feed);
        if (locations != null) {
            return Storage.TYPE.FILESYSTEM;
        }

        final CatalogTable table = getTable(cluster, feed);
        if (table != null) {
            return Storage.TYPE.TABLE;
        }

        throw new FalconException("Both catalog and locations are not defined.");
    }

    public static Storage.TYPE getStorageType(Feed feed,
                                              org.apache.falcon.entity.v0.cluster.Cluster clusterEntity)
        throws FalconException {
        Cluster feedCluster = getCluster(feed, clusterEntity.getName());
        return getStorageType(feed, feedCluster);
    }

    public static List<Location> getLocations(Cluster cluster, Feed feed) {
        // check if locations are overridden in cluster
        final Locations clusterLocations = cluster.getLocations();
        if (clusterLocations != null
                && clusterLocations.getLocations().size() != 0) {
            return clusterLocations.getLocations();
        }

        Locations feedLocations = feed.getLocations();
        return feedLocations == null ? null : feedLocations.getLocations();
    }

    public static Location getLocation(Feed feed, org.apache.falcon.entity.v0.cluster.Cluster cluster,
                                       LocationType type) {
        List<Location> locations = getLocations(getCluster(feed, cluster.getName()), feed);
        if (locations != null) {
            for (Location location : locations) {
                if (location.getType() == type) {
                    return location;
                }
            }
        }

        return null;
    }

    public static Sla getSLAs(Cluster cluster, Feed feed) {
        final Sla clusterSla = cluster.getSla();
        if (clusterSla != null) {
            return clusterSla;
        }

        final Sla feedSla = feed.getSla();
        return feedSla == null ? null : feedSla;
    }

    protected static CatalogTable getTable(Cluster cluster, Feed feed) {
        // check if table is overridden in cluster
        if (cluster.getTable() != null) {
            return cluster.getTable();
        }

        return feed.getTable();
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

    public static Properties getClusterProperties(org.apache.falcon.entity.v0.cluster.Cluster cluster) {
        Properties properties = new Properties();
        Map<String, String> clusterVars = new HashMap<>();
        clusterVars.put("colo", cluster.getColo());
        clusterVars.put("name", cluster.getName());
        if (cluster.getProperties() != null) {
            for (Property property : cluster.getProperties().getProperties()) {
                clusterVars.put(property.getName(), property.getValue());
            }
        }
        properties.put("cluster", clusterVars);
        return properties;
    }

    public static String evaluateClusterExp(org.apache.falcon.entity.v0.cluster.Cluster clusterEntity, String exp)
        throws FalconException {

        Properties properties = getClusterProperties(clusterEntity);
        ExpressionHelper expHelp = ExpressionHelper.get();
        expHelp.setPropertiesForVariable(properties);
        return expHelp.evaluateFullExpression(exp, String.class);
    }

    public static String getStagingPath(boolean isSource,
                                        org.apache.falcon.entity.v0.cluster.Cluster clusterEntity,
                                        Feed feed, CatalogStorage storage, Tag tag, String suffix) {
        String stagingDirPath = getStagingDir(isSource, clusterEntity, feed, storage, tag);

        String datedPartitionKey = storage.getDatedPartitionKeys().get(0);
        String datedPartitionKeySuffix = datedPartitionKey + "=${coord:dataOutPartitionValue('output',"
                + "'" + datedPartitionKey + "')}";
        return stagingDirPath + "/"
                + datedPartitionKeySuffix + "/"
                + suffix + "/"
                + "data";
    }

    public static String getStagingDir(boolean isSource,
                                       org.apache.falcon.entity.v0.cluster.Cluster clusterEntity,
                                       Feed feed, CatalogStorage storage, Tag tag) {
        String workflowName = EntityUtil.getWorkflowName(
                tag, Arrays.asList(clusterEntity.getName()), feed).toString();

        // log path is created at scheduling wf
        final String storageUri = isSource
                ? ClusterHelper.getReadOnlyStorageUrl(clusterEntity) // read interface
                : ClusterHelper.getStorageUrl(clusterEntity);        // write interface
        return storageUri
                + EntityUtil.getLogPath(clusterEntity, feed) + "/"
                + workflowName + "/"
                + storage.getDatabase() + "/"
                + storage.getTable();
    }

    public static Properties getUserWorkflowProperties(LifeCycle lifeCycle) {
        Properties props = new Properties();
        props.put("userWorkflowName", lifeCycle.name().toLowerCase() + "-policy");
        props.put("userWorkflowEngine", "falcon");

        String version;
        try {
            version = BuildProperties.get().getProperty("build.version");
        } catch (Exception e) {  // unfortunate that this is only available in prism/webapp
            version = "0.6";
        }
        props.put("userWorkflowVersion", version);
        return props;
    }

    public static Properties getFeedProperties(Feed feed) {
        Properties feedProperties = new Properties();
        if (feed.getProperties() != null) {
            for (org.apache.falcon.entity.v0.feed.Property property : feed.getProperties().getProperties()) {
                feedProperties.put(property.getName(), property.getValue());
            }
        }
        return feedProperties;
    }

    /**
     *  Extracts date from the actual data path e.g., /path/2014/05/06 maps to 2014-05-06T00:00Z.
     * @param instancePath - actual data path
     * @param templatePath - template path from feed definition
     * @param timeZone timeZone
     * @return date corresponding to the path
     */
    //consider just the first occurrence of the pattern
    public static Date getDate(String templatePath, Path instancePath, TimeZone timeZone) {
        String path = instancePath.toString();
        Matcher matcher = FeedDataPath.PATTERN.matcher(templatePath);
        Calendar cal = Calendar.getInstance(timeZone);
        int lastEnd = 0;

        Set<FeedDataPath.VARS> matchedVars = new HashSet<>();
        while (matcher.find()) {
            FeedDataPath.VARS pathVar = FeedDataPath.VARS.from(matcher.group());
            String pad = templatePath.substring(lastEnd, matcher.start());
            if (!path.startsWith(pad)) {
                //Template and path do not match
                return null;
            }

            int value;
            try {
                value = Integer.valueOf(path.substring(pad.length(), pad.length() + pathVar.getValueSize()));
            } catch (NumberFormatException e) {
                //Not a valid number for variable
                return null;
            }

            pathVar.setCalendar(cal, value);
            lastEnd = matcher.end();
            path = path.substring(pad.length() + pathVar.getValueSize());
            matchedVars.add(pathVar);
        }

        String remTemplatePath = templatePath.substring(lastEnd);
        //Match the remaining constant at the end
        //Handling case where feed instancePath has partitions
        if (StringUtils.isNotEmpty(path) && StringUtils.isNotEmpty(remTemplatePath)
                && !path.contains(remTemplatePath)) {
            return null;
        }


        //Reset other fields
        for (FeedDataPath.VARS var : FeedDataPath.VARS.values()) {
            if (!matchedVars.contains(var)) {
                cal.set(var.getCalendarField(), 0);
            }
        }
        return cal.getTime();
    }

    public static Path getFeedBasePath(String feedPath) throws IOException {
        Matcher matcher = FeedDataPath.PATTERN.matcher(feedPath);
        if (matcher.find()) {
            return new Path(feedPath.substring(0, matcher.start()));
        } else {
            throw new IOException("Unable to resolve pattern for feedPath: " + feedPath);
        }

    }

    private static void validateFeedInstance(Feed feed, Date instanceTime,
                                             org.apache.falcon.entity.v0.cluster.Cluster cluster) {

        // validate the cluster
        Cluster feedCluster = getCluster(feed, cluster.getName());
        if (feedCluster == null) {
            throw new IllegalArgumentException("Cluster :" + cluster.getName() + " is not a valid cluster for feed:"
                    + feed.getName());
        }

        // validate that instanceTime is in validity range
        if (feedCluster.getValidity().getStart().after(instanceTime)
                || feedCluster.getValidity().getEnd().before(instanceTime)) {
            throw new IllegalArgumentException("instanceTime: " + instanceTime + " is not in validity range for"
                    + " Feed: " + feed.getName() + " on cluster:" + cluster.getName());
        }

        // validate instanceTime on basis of startTime and frequency
        Date nextInstance = EntityUtil.getNextStartTime(feedCluster.getValidity().getStart(), feed.getFrequency(),
                feed.getTimezone(), instanceTime);
        if (!nextInstance.equals(instanceTime)) {
            throw new IllegalArgumentException("instanceTime: " + instanceTime + " is not a valid instance for the "
                    + " feed: " + feed.getName() + " on cluster: " + cluster.getName()
                    + " on the basis of startDate and frequency");
        }
    }

    /**
    * Given a feed Instance finds the generating process instance.
    *
    * [process, cluster, instanceTime]
    *
    * If the feed is replicated, then it returns null.
    *
    * @param feed output feed
    * @param feedInstanceTime instance time of the feed
    * @return returns the instance of the process which produces the given feed
            */
    public static SchedulableEntityInstance getProducerInstance(Feed feed, Date feedInstanceTime,
                                        org.apache.falcon.entity.v0.cluster.Cluster cluster) throws FalconException {

        //validate the inputs
        validateFeedInstance(feed, feedInstanceTime, cluster);

        Process process = getProducerProcess(feed);
        if (process != null) {
            try {
                Date processInstanceTime = getProducerInstanceTime(feed, feedInstanceTime, process, cluster);
                SchedulableEntityInstance producer = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                        processInstanceTime, EntityType.PROCESS);
                producer.setTag(SchedulableEntityInstance.OUTPUT);
                return producer;
            } catch (FalconException e) {
                LOG.error("Error in trying to get producer process: {}'s instance time for feed: {}'s instance: } "
                        + " on cluster:{}", process.getName(), feed.getName(), feedInstanceTime, cluster.getName());
            }
        }
        return null;
    }

    /**
     * Given a feed find it's generating process.
     *
     * If no generating process is found it returns null.
     * @param feed output feed
     * @return Process which produces the given feed.
     */
    public static Process getProducerProcess(Feed feed) throws FalconException {

        EntityList dependencies = EntityUtil.getEntityDependencies(feed);

        for (EntityList.EntityElement e : dependencies.getElements()) {
            if (e.tag.contains(EntityList.OUTPUT_TAG)) {
                return EntityUtil.getEntity(EntityType.PROCESS, e.name);
            }
        }
        return null;
    }

    /**
     * Find the producerInstance which will generate the given feedInstance.
     *
     * @param feed output feed
     * @param feedInstanceTime instance time of the output feed
     * @param producer producer process
     * @return time of the producer instance which will produce the given feed instance.
     */
    private static Date getProducerInstanceTime(Feed feed, Date feedInstanceTime, Process producer,
                                       org.apache.falcon.entity.v0.cluster.Cluster cluster) throws FalconException {

        String clusterName = cluster.getName();
        Cluster feedCluster = getCluster(feed, clusterName);
        org.apache.falcon.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(producer, clusterName);
        Date producerStartDate = processCluster.getValidity().getStart();

        // read the process definition and find the relative time difference between process and output feed
        // if output process instance time is now then output FeedInstance time is x
        String outputInstance = null;
        for (Output op : producer.getOutputs().getOutputs()) {
            if (StringUtils.equals(feed.getName(), op.getFeed())) {
                outputInstance = op.getInstance();
            }
        }

        ExpressionHelper.setReferenceDate(producerStartDate);
        ExpressionHelper evaluator = ExpressionHelper.get();
        // producerInstance = feedInstanceTime + (difference between producer process and feed)
        // the feedInstance before or equal to this time is the required one
        Date relativeFeedInstance = evaluator.evaluate(outputInstance, Date.class);
        Date feedInstanceActual = EntityUtil.getPreviousInstanceTime(feedCluster.getValidity().getStart(),
                feed.getFrequency(), feed.getTimezone(), relativeFeedInstance);
        Long producerInstanceTime = feedInstanceTime.getTime() + (producerStartDate.getTime()
                - feedInstanceActual.getTime());
        Date producerInstance = new Date(producerInstanceTime);

        //validate that the producerInstance is in the validity range on the provided cluster
        if (producerInstance.before(processCluster.getValidity().getStart())
                || producerInstance.after(processCluster.getValidity().getEnd())) {
            throw new IllegalArgumentException("Instance time provided: " + feedInstanceTime
                    + " for feed " + feed.getName()
                    + " is outside the range of instances produced by the producer process: " + producer.getName()
                    + " in it's validity range on provided cluster: " + cluster.getName());
        }
        return producerInstance;
    }


    public static Set<SchedulableEntityInstance> getConsumerInstances(Feed feed, Date feedInstanceTime,
                  org.apache.falcon.entity.v0.cluster.Cluster cluster) throws FalconException {

        Set<SchedulableEntityInstance> result = new HashSet<>();
        // validate that the feed has this cluster & validate that the instanceTime is a valid instanceTime
        validateFeedInstance(feed, feedInstanceTime, cluster);

        Set<Process> consumers = getConsumerProcesses(feed);
        for (Process p : consumers) {
            Set<Date> consumerInstanceTimes = getConsumerProcessInstanceTimes(feed, feedInstanceTime, p, cluster);
            for (Date date : consumerInstanceTimes) {
                SchedulableEntityInstance in = new SchedulableEntityInstance(p.getName(), cluster.getName(), date,
                        EntityType.PROCESS);
                in.setTag(SchedulableEntityInstance.INPUT);
                result.add(in);
            }
        }
        return result;
    }


    /**
     * Returns the consumer processes for a given feed if any, null otherwise.
     *
     * @param feed input feed
     * @return the set of processes which use the given feed as input, empty set if no consumers.
     */
    public static Set<Process> getConsumerProcesses(Feed feed) throws FalconException {
        Set<Process> result = new HashSet<>();
        EntityList dependencies = EntityUtil.getEntityDependencies(feed);

        for (EntityList.EntityElement e : dependencies.getElements()) {
            if (e.tag.contains(EntityList.INPUT_TAG)) {
                Process consumer = EntityUtil.getEntity(EntityType.PROCESS, e.name);
                result.add(consumer);
            }
        }
        return result;
    }

    // return all instances of a process which will consume the given feed instance
    private static Set<Date> getConsumerProcessInstanceTimes(Feed feed, Date feedInstancetime, Process consumer,
              org.apache.falcon.entity.v0.cluster.Cluster cluster) throws FalconException {

        Set<Date> result = new HashSet<>();
        // find relevant cluster for the process
        org.apache.falcon.entity.v0.process.Cluster processCluster =
                ProcessHelper.getCluster(consumer, cluster.getName());
        if (processCluster == null) {
            throw new IllegalArgumentException("Cluster is not valid for process");
        }
        Date processStartDate = processCluster.getValidity().getStart();
        Cluster feedCluster = getCluster(feed, cluster.getName());
        Date feedStartDate = feedCluster.getValidity().getStart();

        // find all corresponding Inputs as a process may refer same feed multiple times
        List<Input> inputFeeds = new ArrayList<>();
        if (consumer.getInputs() != null && consumer.getInputs().getInputs() != null) {
            for (Input input : consumer.getInputs().getInputs()) {
                if (StringUtils.equals(input.getFeed(), feed.getName())) {
                    inputFeeds.add(input);
                }
            }
        }

        // for each input corresponding to given feed, find corresponding consumer instances
        for (Input in : inputFeeds) {
            /* Algorithm for finding a consumer instance for an input feed instance
            Step 1. Find one instance which will consume the given feed instance.
                    a. take process start date and find last input feed instance time. In this step take care of
                        frequencies being out of sync.
                    b. using the above find the time difference between the process instance and feed instance.
                    c. Adding the above time difference to given feed instance for which we want to find the consumer
                        instances we will get one consumer process instance.
            Step 2. Keep checking for next instances of process till they consume the given feed Instance.
            Step 3. Similarly check for all previous instances of process till they consume the given feed instance.
            */

            // Step 1.a & 1.b
            ExpressionHelper.setReferenceDate(processStartDate);
            ExpressionHelper evaluator = ExpressionHelper.get();
            Date startRelative = evaluator.evaluate(in.getStart(), Date.class);
            Date startTimeActual = EntityUtil.getNextStartTime(feedStartDate,
                    feed.getFrequency(), feed.getTimezone(), startRelative);
            Long offset = processStartDate.getTime() - startTimeActual.getTime();

            // Step 1.c
            Date processInstanceStartRelative = new Date(feedInstancetime.getTime() + offset);
            Date processInstanceStartActual = EntityUtil.getPreviousInstanceTime(processStartDate,
                    consumer.getFrequency(), consumer.getTimezone(), processInstanceStartRelative);


            // Step 2.
            Date currentInstance = processInstanceStartActual;
            while (true) {
                Date nextConsumerInstance = EntityUtil.getNextStartTime(processStartDate,
                        consumer.getFrequency(), consumer.getTimezone(), currentInstance);

                ExpressionHelper.setReferenceDate(nextConsumerInstance);
                evaluator = ExpressionHelper.get();
                Long rangeStart = evaluator.evaluate(in.getStart(), Date.class).getTime();
                Long rangeEnd = evaluator.evaluate(in.getEnd(), Date.class).getTime();
                if (rangeStart <= feedInstancetime.getTime() && feedInstancetime.getTime() < rangeEnd) {
                    result.add(nextConsumerInstance);
                } else {
                    break;
                }
                currentInstance = new Date(nextConsumerInstance.getTime() + ONE_MS);
            }

            // Step 3.
            currentInstance = processInstanceStartActual;
            while (true) {
                Date nextConsumerInstance = EntityUtil.getPreviousInstanceTime(processStartDate,
                        consumer.getFrequency(), consumer.getTimezone(), currentInstance);

                ExpressionHelper.setReferenceDate(nextConsumerInstance);
                evaluator = ExpressionHelper.get();
                Long rangeStart = evaluator.evaluate(in.getStart(), Date.class).getTime();
                Long rangeEnd = evaluator.evaluate(in.getEnd(), Date.class).getTime();
                if (rangeStart <= feedInstancetime.getTime() && feedInstancetime.getTime() < rangeEnd) {
                    result.add(nextConsumerInstance);
                } else {
                    break;
                }
                currentInstance = new Date(nextConsumerInstance.getTime() - ONE_MS);
            }
        }
        return result;
    }

    public static FeedInstanceResult getFeedInstanceListing(Entity entityObject,
                                                            Date start, Date end) throws FalconException {
        Set<String> clusters = EntityUtil.getClustersDefinedInColos(entityObject);
        FeedInstanceResult result = new FeedInstanceResult(APIResult.Status.SUCCEEDED, "Success");
        for (String cluster : clusters) {
            Feed feed = (Feed) entityObject;
            Storage storage = createStorage(cluster, feed);
            List<FeedInstanceStatus> feedListing = storage.getListing(feed, cluster, LocationType.DATA, start, end);
            FeedInstanceResult.Instance[] instances = new FeedInstanceResult.Instance[feedListing.size()];
            int index = 0;
            for (FeedInstanceStatus feedStatus : feedListing) {
                FeedInstanceResult.Instance instance = new
                        FeedInstanceResult.Instance(cluster, feedStatus.getInstance(),
                        feedStatus.getStatus().name());
                instance.creationTime = feedStatus.getCreationTime();
                instance.uri = feedStatus.getUri();
                instance.size = feedStatus.getSize();
                instances[index++] = instance;
            }
            result.setInstances(instances);
        }
        return result;
    }
}
