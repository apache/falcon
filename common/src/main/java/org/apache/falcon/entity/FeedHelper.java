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
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.datasource.DatasourceType;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.ClusterType;
import org.apache.falcon.entity.v0.feed.ExtractMethod;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.FieldIncludeExclude;
import org.apache.falcon.entity.v0.feed.Lifecycle;
import org.apache.falcon.entity.v0.feed.Load;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.feed.MergeType;
import org.apache.falcon.entity.v0.feed.Property;
import org.apache.falcon.entity.v0.feed.RetentionStage;
import org.apache.falcon.entity.v0.feed.Sla;
import org.apache.falcon.entity.v0.feed.Validity;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.lifecycle.FeedLifecycleStage;
import org.apache.falcon.resource.APIResult;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.FeedInstanceResult;
import org.apache.falcon.resource.SchedulableEntityInstance;
import org.apache.falcon.util.BuildProperties;
import org.apache.falcon.util.DateUtil;
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

    public static Sla getSLA(Cluster cluster, Feed feed) {
        final Sla clusterSla = cluster.getSla();
        if (clusterSla != null) {
            return clusterSla;
        }
        final Sla feedSla = feed.getSla();
        return feedSla == null ? null : feedSla;
    }

    public static Sla getSLA(String clusterName, Feed feed) {
        Cluster cluster = FeedHelper.getCluster(feed, clusterName);
        return cluster != null ? getSLA(cluster, feed) : null;
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
            for (org.apache.falcon.entity.v0.cluster.Property property : cluster.getProperties().getProperties()) {
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

    public static Lifecycle getLifecycle(Feed feed, String clusterName) throws FalconException {
        Cluster cluster = getCluster(feed, clusterName);
        if (cluster !=null) {
            return cluster.getLifecycle() != null ? cluster.getLifecycle() : feed.getLifecycle();
        }
        throw new FalconException("Cluster: " + clusterName + " isn't valid for feed: " + feed.getName());
    }

    public static RetentionStage getRetentionStage(Feed feed, String clusterName) throws FalconException {
        if (isLifecycleEnabled(feed, clusterName)) {
            Lifecycle globalLifecycle = feed.getLifecycle();
            Lifecycle clusterLifecycle = getCluster(feed, clusterName).getLifecycle();

            if (clusterLifecycle != null && clusterLifecycle.getRetentionStage() != null) {
                return clusterLifecycle.getRetentionStage();
            } else if (globalLifecycle != null) {
                return globalLifecycle.getRetentionStage();
            }
        }
        return null;
    }

    public static Date getFeedValidityStart(Feed feed, String clusterName) throws FalconException {
        org.apache.falcon.entity.v0.feed.Cluster feedCluster = FeedHelper.getCluster(feed, clusterName);
        if (feedCluster != null) {
            return feedCluster.getValidity().getStart();
        } else {
            throw new FalconException("No matching cluster " + clusterName
                    + "found for feed " + feed.getName());
        }
    }

    public static Date getNextFeedInstanceDate(Date alignedDate, Feed feed) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(alignedDate);
        calendar.add(feed.getFrequency().getTimeUnit().getCalendarUnit(),
                feed.getFrequency().getFrequencyAsInt());
        return calendar.getTime();
    }

    /**
     * Returns various policies applicable for a feed.
     *
     * @param feed
     * @return list of names of lifecycle policies for the given feed, empty list if there are none.
     */
    public static List<String> getPolicies(Feed feed, String clusterName) throws FalconException {
        List<String> result = new ArrayList<>();
        Cluster cluster = getCluster(feed, clusterName);
        if (cluster != null) {
            if (isLifecycleEnabled(feed, clusterName)) {
                String policy = getRetentionStage(feed, clusterName).getPolicy();
                policy = StringUtils.isBlank(policy)
                        ? FeedLifecycleStage.RETENTION.getDefaultPolicyName() : policy;
                result.add(policy);
            }
            return result;
        }
        throw new FalconException("Cluster: " + clusterName + " isn't valid for feed: " + feed.getName());
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
                value = Integer.parseInt(path.substring(pad.length(), pad.length() + pathVar.getValueSize()));
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
                switch (var.getCalendarField()) {
                case Calendar.DAY_OF_MONTH:
                    cal.set(var.getCalendarField(), 1);
                    break;
                default:
                    cal.set(var.getCalendarField(), 0);
                }
            }
            cal.set(Calendar.SECOND, 0);
            cal.set(Calendar.MILLISECOND, 0);
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
                || !feedCluster.getValidity().getEnd().after(instanceTime)) {
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
            org.apache.falcon.entity.v0.process.Cluster processCluster = ProcessHelper.getCluster(process,
                    cluster.getName());
            Date pStart = processCluster.getValidity().getStart();
            Date pEnd = processCluster.getValidity().getEnd();
            Frequency pFrequency = process.getFrequency();
            TimeZone pTz = process.getTimezone();

            try {
                Date processInstanceTime = getProducerInstanceTime(feed, feedInstanceTime, process, cluster);
                boolean isValid = EntityUtil.isValidInstanceTime(pStart, pFrequency, pTz, processInstanceTime);
                if (processInstanceTime.before(pStart) || !processInstanceTime.before(pEnd) || !isValid){
                    return null;
                }

                SchedulableEntityInstance producer = new SchedulableEntityInstance(process.getName(), cluster.getName(),
                        processInstanceTime, EntityType.PROCESS);
                producer.setTags(SchedulableEntityInstance.OUTPUT);
                return producer;
            } catch (FalconException | IllegalArgumentException e) {
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
                in.setTags(SchedulableEntityInstance.INPUT);
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
            Date startTimeActual = EntityUtil.getPreviousInstanceTime(feedStartDate,
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
                Date inputStart = evaluator.evaluate(in.getStart(), Date.class);
                Long rangeStart = EntityUtil.getPreviousInstanceTime(feedStartDate, feed.getFrequency(),
                        feed.getTimezone(), inputStart).getTime();
                Long rangeEnd = evaluator.evaluate(in.getEnd(), Date.class).getTime();
                if (rangeStart <= feedInstancetime.getTime() && feedInstancetime.getTime() <= rangeEnd) {
                    if (!nextConsumerInstance.before(processCluster.getValidity().getStart())
                            && nextConsumerInstance.before(processCluster.getValidity().getEnd())) {
                        result.add(nextConsumerInstance);
                    }
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
                Date inputStart = evaluator.evaluate(in.getStart(), Date.class);
                Long rangeStart = EntityUtil.getPreviousInstanceTime(feedStartDate, feed.getFrequency(),
                        feed.getTimezone(), inputStart).getTime();
                Long rangeEnd = evaluator.evaluate(in.getEnd(), Date.class).getTime();
                if (rangeStart <= feedInstancetime.getTime() && feedInstancetime.getTime() <= rangeEnd) {
                    if (!nextConsumerInstance.before(processCluster.getValidity().getStart())
                            && nextConsumerInstance.before(processCluster.getValidity().getEnd())) {
                        result.add(nextConsumerInstance);
                    }
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
                instance.sizeH = feedStatus.getSizeH();
                instances[index++] = instance;
            }
            result.setInstances(instances);
        }
        return result;
    }


    /**
     * Returns the data source type associated with the Feed's import policy.
     *
     * @param clusterEntity
     * @param feed
     * @return {@link org.apache.falcon.entity.v0.datasource.DatasourceType}
     * @throws FalconException
     */
    public static DatasourceType getImportDatasourceType(
            org.apache.falcon.entity.v0.cluster.Cluster clusterEntity,
            Feed feed) throws FalconException {
        Cluster feedCluster = getCluster(feed, clusterEntity.getName());
        if (isImportEnabled(feedCluster)) {
            return DatasourceHelper.getDatasourceType(getImportDatasourceName(feedCluster));
        } else {
            return null;
        }
    }

    /**
     * Return if Import policy is enabled in the Feed definition.
     *
     * @param feedCluster
     * @return true if import policy is enabled else false
     */

    public static boolean isImportEnabled(org.apache.falcon.entity.v0.feed.Cluster feedCluster) {
        if (feedCluster.getType() == ClusterType.SOURCE) {
            return (feedCluster.getImport() != null);
        }
        return false;
    }



    /**
     * Returns the data source name associated with the Feed's import policy.
     *
     * @param feedCluster
     * @return DataSource name defined in the Datasource Entity
     */
    public static String getImportDatasourceName(org.apache.falcon.entity.v0.feed.Cluster feedCluster) {
        if (isImportEnabled(feedCluster)) {
            return feedCluster.getImport().getSource().getName();
        } else {
            return null;
        }
    }



    /**
     * Returns Datasource table name.
     *
     * @param feedCluster
     * @return Table or Topic name of the Datasource
     */

    public static String getImportDataSourceTableName(org.apache.falcon.entity.v0.feed.Cluster feedCluster) {
        if (isImportEnabled(feedCluster)) {
            return feedCluster.getImport().getSource().getTableName();
        } else {
            return null;
        }
    }



    /**
     * Returns the extract method type.
     *
     * @param feedCluster
     * @return {@link org.apache.falcon.entity.v0.feed.ExtractMethod}
     */

    public static ExtractMethod getImportExtractMethod(org.apache.falcon.entity.v0.feed.Cluster feedCluster) {
        if (isImportEnabled(feedCluster)) {
            return feedCluster.getImport().getSource().getExtract().getType();
        } else {
            return null;
        }
    }



    /**
     * Returns the merge type of the Feed import policy.
     *
     * @param feedCluster
     * @return {@link org.apache.falcon.entity.v0.feed.MergeType}
     */
    public static MergeType getImportMergeType(org.apache.falcon.entity.v0.feed.Cluster feedCluster) {
        if (isImportEnabled(feedCluster)) {
            return feedCluster.getImport().getSource().getExtract().getMergepolicy();
        } else {
            return null;
        }
    }

    /**
     * Returns the initial instance date for the import data set for coorinator.
     *
     * @param feedCluster
     * @return Feed cluster validity start date or recent time
     */
    public static Date getImportInitalInstance(org.apache.falcon.entity.v0.feed.Cluster feedCluster) {
        return feedCluster.getValidity().getStart();
    }


    /**
     * Helper method to check if the merge type is snapshot.
     *
     * @param feedCluster
     * @return true if the feed import policy merge type is snapshot
     *
     */
    public static boolean isSnapshotMergeType(org.apache.falcon.entity.v0.feed.Cluster feedCluster) {
        return MergeType.SNAPSHOT == getImportMergeType(feedCluster);
    }

    /**
     * Returns extra arguments specified in the Feed import policy.
     *
     * @param feedCluster
     * @return
     * @throws FalconException
     */
    public static Map<String, String> getImportArguments(org.apache.falcon.entity.v0.feed.Cluster feedCluster)
        throws FalconException {

        Map<String, String> argsMap = new HashMap<String, String>();
        if (feedCluster.getImport().getArguments() == null) {
            return argsMap;
        }

        for(org.apache.falcon.entity.v0.feed.Argument p : feedCluster.getImport().getArguments().getArguments()) {
            argsMap.put(p.getName().toLowerCase(), p.getValue());
        }
        return argsMap;
    }




    /**
     * Returns Fields list specified in the Import Policy.
     *
     * @param feedCluster
     * @return List of String
     * @throws FalconException
     */
    public static List<String> getImportFieldList(org.apache.falcon.entity.v0.feed.Cluster feedCluster)
        throws FalconException {
        if (feedCluster.getImport().getSource().getFields() == null) {
            return null;
        }
        org.apache.falcon.entity.v0.feed.FieldsType fieldType = feedCluster.getImport().getSource().getFields();
        FieldIncludeExclude includeFileds = fieldType.getIncludes();
        if (includeFileds == null) {
            return null;
        }
        return includeFileds.getFields();
    }


    /**
     * Returns true if exclude field lists are used. This is a TBD feature.
     *
     * @param ds Feed Datasource
     * @return true of exclude field list is used or false.
     * @throws FalconException
     */

    public static boolean isFieldExcludes(org.apache.falcon.entity.v0.feed.Datasource ds)
        throws FalconException {
        if (ds.getFields() != null) {
            org.apache.falcon.entity.v0.feed.FieldsType fieldType = ds.getFields();
            FieldIncludeExclude excludeFileds = fieldType.getExcludes();
            if ((excludeFileds != null) && (excludeFileds.getFields().size() > 0)) {
                return true;
            }
        }
        return false;
    }

    public static FeedInstanceStatus.AvailabilityStatus getFeedInstanceStatus(Feed feed, String clusterName,
                                                                              Date instanceTime)
        throws FalconException {
        Storage storage = createStorage(clusterName, feed);
        return storage.getInstanceAvailabilityStatus(feed, clusterName, LocationType.DATA, instanceTime);
    }

    public static boolean isLifecycleEnabled(Feed feed, String clusterName) {
        Cluster cluster = getCluster(feed, clusterName);
        return cluster != null && (feed.getLifecycle() != null || cluster.getLifecycle() != null);
    }

    public static Frequency getLifecycleRetentionFrequency(Feed feed, String clusterName) throws FalconException {
        Frequency retentionFrequency = null;
        RetentionStage retentionStage = getRetentionStage(feed, clusterName);
        if (retentionStage != null) {
            if (retentionStage.getFrequency() != null) {
                retentionFrequency = retentionStage.getFrequency();
            } else {
                Frequency feedFrequency = feed.getFrequency();
                Frequency defaultFrequency = new Frequency("hours(6)");
                if (DateUtil.getFrequencyInMillis(feedFrequency) < DateUtil.getFrequencyInMillis(defaultFrequency)) {
                    retentionFrequency = defaultFrequency;
                } else {
                    retentionFrequency = new Frequency(feedFrequency.toString());
                }
            }
        }
        return  retentionFrequency;
    }

    /**
     * Returns the hadoop cluster queue name specified for the replication jobs to run in the Lifecycle
     * section of the target cluster section of the feed entity.
     *
     * NOTE: Lifecycle for replication is not implemented. This will return the queueName property value.
     *
     * @param feed
     * @param clusterName
     * @return hadoop cluster queue name specified in the feed entity
     * @throws FalconException
     */

    public static String getLifecycleReplicationQueue(Feed feed, String clusterName) throws FalconException {
        return null;
    }

    /**
     * Returns the hadoop cluster queue name specified for the retention jobs to run in the Lifecycle
     * section of feed entity.
     *
     * @param feed
     * @param clusterName
     * @return hadoop cluster queue name specified in the feed entity
     * @throws FalconException
     */
    public static String getLifecycleRetentionQueue(Feed feed, String clusterName) throws FalconException {
        RetentionStage retentionStage = getRetentionStage(feed, clusterName);
        if (retentionStage != null) {
            return retentionStage.getQueue();
        } else {
            return null;
        }
    }

    /**
     * Returns the data source type associated with the Feed's export policy.
     *
     * @param clusterEntity
     * @param feed
     * @return {@link org.apache.falcon.entity.v0.datasource.DatasourceType}
     * @throws FalconException
     */
    public static DatasourceType getExportDatasourceType(
            org.apache.falcon.entity.v0.cluster.Cluster clusterEntity,
            Feed feed) throws FalconException {
        Cluster feedCluster = getCluster(feed, clusterEntity.getName());
        if (isExportEnabled(feedCluster)) {
            return DatasourceHelper.getDatasourceType(getExportDatasourceName(feedCluster));
        } else {
            return null;
        }
    }

    /**
     * Return if Export policy is enabled in the Feed definition.
     *
     * @param feedCluster
     * @return true if export policy is enabled else false
     */

    public static boolean isExportEnabled(org.apache.falcon.entity.v0.feed.Cluster feedCluster) {
        return (feedCluster.getExport() != null);
    }

    /**
     * Returns the data source name associated with the Feed's export policy.
     *
     * @param feedCluster
     * @return DataSource name defined in the Datasource Entity
     */
    public static String getExportDatasourceName(org.apache.falcon.entity.v0.feed.Cluster feedCluster) {
        if (isExportEnabled(feedCluster)) {
            return feedCluster.getExport().getTarget().getName();
        } else {
            return null;
        }
    }

    /**
     * Returns Datasource table name.
     *
     * @param feedCluster
     * @return Table or Topic name of the Datasource
     */

    public static String getExportDataSourceTableName(org.apache.falcon.entity.v0.feed.Cluster feedCluster) {
        if (isExportEnabled(feedCluster)) {
            return feedCluster.getExport().getTarget().getTableName();
        } else {
            return null;
        }
    }

    /**
     * Returns the export load type.
     *
     * @param feedCluster
     * @return {@link org.apache.falcon.entity.v0.feed.Load}
     */

    public static Load getExportLoadMethod(org.apache.falcon.entity.v0.feed.Cluster feedCluster) {
        if (isExportEnabled(feedCluster)) {
            return feedCluster.getExport().getTarget().getLoad();
        } else {
            return null;
        }
    }

    /**
     * Returns the initial instance date for the export data set for coorinator.
     *
     * @param feedCluster
     * @return Feed cluster validity start date or recent time
     */
    public static Date getExportInitalInstance(org.apache.falcon.entity.v0.feed.Cluster feedCluster) {
        return feedCluster.getValidity().getStart();
    }

    /**
     * Returns extra arguments specified in the Feed export policy.
     *
     * @param feedCluster
     * @return
     * @throws FalconException
     */
    public static Map<String, String> getExportArguments(org.apache.falcon.entity.v0.feed.Cluster feedCluster)
        throws FalconException {

        Map<String, String> argsMap = new HashMap<String, String>();
        if (feedCluster.getExport().getArguments() == null) {
            return argsMap;
        }

        for(org.apache.falcon.entity.v0.feed.Argument p : feedCluster.getExport().getArguments().getArguments()) {
            argsMap.put(p.getName().toLowerCase(), p.getValue());
        }
        return argsMap;
    }

    public static Validity getClusterValidity(Feed feed, String clusterName) throws FalconException {
        Cluster cluster = getCluster(feed, clusterName);
        if (cluster == null) {
            throw new FalconException("Invalid cluster: " + clusterName + " for feed: " + feed.getName());
        }
        return cluster.getValidity();
    }

    public static Frequency getOldRetentionFrequency(Feed feed) {
        Frequency feedFrequency = feed.getFrequency();
        Frequency defaultFrequency = new Frequency("hours(24)");
        if (DateUtil.getFrequencyInMillis(feedFrequency) < DateUtil.getFrequencyInMillis(defaultFrequency)) {
            return new Frequency("hours(6)");
        } else {
            return defaultFrequency;
        }
    }

    public static Frequency getRetentionFrequency(Feed feed, Cluster feedCluster) throws FalconException {
        Frequency retentionFrequency;
        retentionFrequency = getLifecycleRetentionFrequency(feed, feedCluster.getName());
        if (retentionFrequency == null) {
            retentionFrequency = getOldRetentionFrequency(feed);
        }
        return retentionFrequency;
    }

    public static int getRetentionLimitInSeconds(Feed feed, String clusterName) throws FalconException {
        Frequency retentionLimit = new Frequency("minutes(0)");
        RetentionStage retentionStage = getRetentionStage(feed, clusterName);
        if (retentionStage != null) {
            for (Property property : retentionStage.getProperties().getProperties()) {
                if (property.getName().equalsIgnoreCase("retention.policy.agebaseddelete.limit")) {
                    retentionLimit = new Frequency(property.getValue());
                    break;
                }
            }
        } else {
            retentionLimit = getCluster(feed, clusterName).getRetention().getLimit();
        }
        Long freqInMillis = DateUtil.getFrequencyInMillis(retentionLimit);
        return (int) (freqInMillis/1000);
    }

    /**
     * Returns the replication job's queue name specified in the feed entity definition.
     * First looks into the Lifecycle stage if exists. If null, looks into the queueName property specified
     * in the Feed definition.
     *
     * @param feed
     * @param feedCluster
     * @return
     * @throws FalconException
     */
    public static String getReplicationQueue(Feed feed, Cluster feedCluster) throws FalconException {
        String queueName;
        queueName = getLifecycleReplicationQueue(feed, feedCluster.getName());
        if (StringUtils.isBlank(queueName)) {
            queueName = getQueueFromProperties(feed);
        }
        return queueName;
    }

    /**
     * Returns the retention job's queue name specified in the feed entity definition.
     * First looks into the Lifecycle stage. If null, looks into the queueName property specified
     * in the Feed definition.
     *
     * @param feed
     * @param feedCluster
     * @return
     * @throws FalconException
     */
    public static String getRetentionQueue(Feed feed, Cluster feedCluster) throws FalconException {
        String queueName = getLifecycleRetentionQueue(feed, feedCluster.getName());
        if (StringUtils.isBlank(queueName)) {
            queueName = getQueueFromProperties(feed);
        }
        return queueName;
    }

    /**
     * Returns the queue name specified in the Feed entity definition from queueName property.
     *
     * @param feed
     * @return queueName property value
     */
    public static String getQueueFromProperties(Feed feed) {
        return getPropertyValue(feed, EntityUtil.MR_QUEUE_NAME);
    }

    /**
     * Returns value of a feed property given property name.
     * @param feed
     * @param propName
     * @return property value
     */

    public static String getPropertyValue(Feed feed, String propName) {
        if (feed.getProperties() != null) {
            for (Property prop : feed.getProperties().getProperties()) {
                if ((prop != null) && (prop.getName().equals(propName))) {
                    return prop.getValue();
                }
            }
        }
        return null;
    }

    public static List<FeedInstanceStatus> getListing(Feed feed, String clusterName, LocationType locationType,
                                                      Date start, Date end) throws FalconException{
        Storage storage= createStorage(clusterName, feed);
        return storage.getListing(feed, clusterName, locationType, start, end);
    }

}
