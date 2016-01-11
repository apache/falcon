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
package org.apache.falcon.service;

import org.apache.commons.io.IOUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.Pair;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.FeedInstanceStatus;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Sla;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.resource.SchedulableEntityInstance;
import org.apache.falcon.util.DeploymentUtil;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Service to monitor Feed SLAs.
 */
public final class FeedSLAMonitoringService implements ConfigurationChangeListener, FalconService {
    private static final Logger LOG = LoggerFactory.getLogger("FeedSLA");

    private static final String ONE_HOUR = String.valueOf(60 * 60 * 1000);

    private static final int ONE_MS = 1;

    private static final FeedSLAMonitoringService SERVICE = new FeedSLAMonitoringService();

    private FeedSLAMonitoringService() {

    }

    public static FeedSLAMonitoringService get() {
        return SERVICE;
    }

    protected int queueSize;

    /**
     * Permissions for storePath.
     */
    private static final FsPermission STORE_PERMISSION = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

    /**
     * Feeds to be monitored.
     */
    protected Set<String> monitoredFeeds;


    /**
     * Map<Pair<feedName, clusterName>, Set<instanceTime> to store
     * each missing instance of a feed.
     */
    protected Map<Pair<String, String>, BlockingQueue<Date>> pendingInstances;


    /**
     * Used to store the last time when pending instances were checked for SLA.
     */
    private Date lastCheckedAt;


    /**
     * Used to store last time when the state was serialized to the store.
     */
    private Date lastSerializedAt;

    /**
     * Frequency in seconds of "status check" for pending feed instances.
     */
    private int statusCheckFrequencySeconds; // 10 minutes


    /**
     * Time Duration (in milliseconds) in future for generating pending feed instances.
     *
     * In every cycle pending feed instances are added for monitoring, till this time in future.
     */
    private int lookAheadWindowMillis; // 15 MINUTES


    /**
     * Frequency in milliseconds of serializing(for backup) monitoring service's state.
     */
    private int serializationFrequencyMillis;

    /**
     * Filesystem used for serializing and deserializing.
     */
    private FileSystem fileSystem;

    /**
     * Working directory for the feed sla monitoring service.
     */
    private Path storePath;

    /**
     * Path to store the state of the monitoring service.
     */
    private Path filePath;

    @Override
    public void onAdd(Entity entity) throws FalconException {
        if (entity.getEntityType() == EntityType.FEED) {
            Feed feed = (Feed) entity;
            // currently sla service is enabled only for fileSystemStorage
            if (feed.getLocations() != null) {
                Set<String> currentClusters = DeploymentUtil.getCurrentClusters();
                for (Cluster cluster : feed.getClusters().getClusters()) {
                    if (currentClusters.contains(cluster.getName())) {
                        if (FeedHelper.getSLA(cluster, feed) != null) {
                            LOG.debug("Adding feed:{} for monitoring", feed.getName());
                            monitoredFeeds.add(feed.getName());
                        }
                    }
                }
            }
        }
    }

    @Override
    public void onRemove(Entity entity) throws FalconException {
        if (entity.getEntityType() == EntityType.FEED) {
            Feed feed = (Feed) entity;
            // currently sla service is enabled only for fileSystemStorage
            if (feed.getLocations() != null) {
                Set<String> currentClusters = DeploymentUtil.getCurrentClusters();
                for (Cluster cluster : feed.getClusters().getClusters()) {
                    if (currentClusters.contains(cluster.getName()) && FeedHelper.getSLA(cluster, feed) != null) {
                        monitoredFeeds.remove(feed.getName());
                        pendingInstances.remove(new Pair<>(feed.getName(), cluster.getName()));
                    }
                }
            }
        }
    }

    private boolean isSLAMonitoringEnabledInCurrentColo(Feed feed) {
        if (feed.getLocations() != null) {
            Set<String> currentClusters = DeploymentUtil.getCurrentClusters();
            for (Cluster cluster : feed.getClusters().getClusters()) {
                if (currentClusters.contains(cluster.getName()) && FeedHelper.getSLA(cluster, feed) != null) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void onChange(Entity oldEntity, Entity newEntity) throws FalconException {
        if (newEntity.getEntityType() == EntityType.FEED) {
            Feed oldFeed = (Feed) oldEntity;
            Feed newFeed = (Feed) newEntity;
            if (!isSLAMonitoringEnabledInCurrentColo(newFeed)) {
                onRemove(oldFeed);
            } else if (!isSLAMonitoringEnabledInCurrentColo(oldFeed)) {
                onAdd(newFeed);
            } else {
                List<String> slaRemovedClusters = new ArrayList<>();
                for (String oldCluster : EntityUtil.getClustersDefinedInColos(oldFeed)) {
                    if (FeedHelper.getSLA(oldCluster, oldFeed) != null
                        && FeedHelper.getSLA(oldCluster, newFeed) == null) {
                        slaRemovedClusters.add(oldCluster);
                    }
                }

                for (String clusterName : slaRemovedClusters) {
                    pendingInstances.remove(new Pair<>(newFeed.getName(), clusterName));
                }
            }
        }
    }

    @Override
    public void onReload(Entity entity) throws FalconException {
        onAdd(entity);
    }

    @Override
    public String getName() {
        return FeedSLAMonitoringService.class.getSimpleName();
    }

    @Override
    public void init() throws FalconException {
        String uri = StartupProperties.get().getProperty("feed.sla.service.store.uri");
        storePath = new Path(uri);
        filePath = new Path(storePath, "feedSLAMonitoringService");
        fileSystem = initializeFileSystem();

        String freq = StartupProperties.get().getProperty("feed.sla.serialization.frequency.millis", ONE_HOUR);
        serializationFrequencyMillis = Integer.valueOf(freq);

        freq = StartupProperties.get().getProperty("feed.sla.statusCheck.frequency.seconds", "600");
        statusCheckFrequencySeconds = Integer.valueOf(freq);

        freq = StartupProperties.get().getProperty("feed.sla.lookAheadWindow.millis", "900000");
        lookAheadWindowMillis = Integer.valueOf(freq);

        String size = StartupProperties.get().getProperty("feed.sla.queue.size", "288");
        queueSize = Integer.valueOf(size);

        try {
            if (fileSystem.exists(filePath)) {
                deserialize(filePath);
            } else {
                LOG.debug("No old state exists at: {}, Initializing a clean state.", filePath.toString());
                initializeService();
            }
        } catch (IOException e) {
            throw new FalconException("Couldn't check the existence of " + filePath, e);
        }
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.scheduleWithFixedDelay(new Monitor(), 0, statusCheckFrequencySeconds, TimeUnit.SECONDS);
    }

    @Override
    public void destroy() throws FalconException {
        serializeState(); // store the state of monitoring service to the disk.
    }

    public void makeFeedInstanceAvailable(String feedName, String clusterName, Date nominalTime) {
        LOG.info("Removing {} feed's instance {} in cluster {} from pendingSLA", feedName,
                clusterName, nominalTime);
        Pair<String, String> feedCluster = new Pair<>(feedName, clusterName);
        pendingInstances.get(feedCluster).remove(nominalTime);
    }

    private FileSystem initializeFileSystem() {
        try {
            fileSystem = HadoopClientFactory.get().createFalconFileSystem(storePath.toUri());
            if (!fileSystem.exists(storePath)) {
                LOG.info("Creating directory for pending feed instances: {}", storePath);
                // set permissions so config store dir is owned by falcon alone
                HadoopClientFactory.mkdirs(fileSystem, storePath, STORE_PERMISSION);
            }
            return fileSystem;
        } catch (Exception e) {
            throw new RuntimeException("Unable to bring up feed sla store for path: " + storePath, e);
        }
    }

    //Periodically update status of pending instances, add new instances and take backup.
    private class Monitor implements Runnable {

        @Override
        public void run() {
            try {
                if (!monitoredFeeds.isEmpty()) {
                    checkPendingInstanceAvailability();

                    // add Instances from last checked time to 10 minutes from now(some buffer for status check)
                    Date now = new Date();
                    Date newCheckPoint = new Date(now.getTime() + lookAheadWindowMillis);
                    addNewPendingFeedInstances(lastCheckedAt, newCheckPoint);
                    lastCheckedAt = newCheckPoint;

                    //take backup
                    if (now.getTime() - lastSerializedAt.getTime() > serializationFrequencyMillis) {
                        serializeState();
                        lastSerializedAt = new Date();
                    }
                }
            } catch (Throwable e) {
                LOG.error("Feed SLA monitoring failed: ", e);
            }
        }
    }


    void addNewPendingFeedInstances(Date from, Date to) throws FalconException {
        Set<String> currentClusters = DeploymentUtil.getCurrentClusters();
        for (String feedName : monitoredFeeds) {
            Feed feed = EntityUtil.getEntity(EntityType.FEED, feedName);
            for (Cluster feedCluster : feed.getClusters().getClusters()) {
                if (currentClusters.contains(feedCluster.getName())) {
                    Date nextInstanceTime = from;
                    Pair<String, String> key = new Pair<>(feed.getName(), feedCluster.getName());
                    BlockingQueue<Date> instances = pendingInstances.get(key);
                    if (instances == null) {
                        instances = new LinkedBlockingQueue<>(queueSize);
                        Date feedStartTime = feedCluster.getValidity().getStart();
                        Frequency retentionFrequency = FeedHelper.getRetentionFrequency(feed, feedCluster);
                        ExpressionHelper evaluator = ExpressionHelper.get();
                        ExpressionHelper.setReferenceDate(new Date());
                        Date retention = new Date(evaluator.evaluate(retentionFrequency.toString(), Long.class));
                        if (feedStartTime.before(retention)) {
                            feedStartTime = retention;
                        }
                        nextInstanceTime = feedStartTime;
                    }
                    Set<Date> exists = new HashSet<>(instances);
                    org.apache.falcon.entity.v0.cluster.Cluster currentCluster =
                            EntityUtil.getEntity(EntityType.CLUSTER, feedCluster.getName());
                    nextInstanceTime = EntityUtil.getNextStartTime(feed, currentCluster, nextInstanceTime);
                    Date endDate = FeedHelper.getClusterValidity(feed, currentCluster.getName()).getEnd();
                    while (nextInstanceTime.before(to) && nextInstanceTime.before(endDate)) {
                        if (instances.size() >= queueSize) { // if no space, first make some space
                            LOG.debug("Removing instance={} for <feed,cluster>={}", instances.peek(), key);
                            exists.remove(instances.peek());
                            instances.remove();
                        }
                        LOG.debug("Adding instance={} for <feed,cluster>={}", nextInstanceTime, key);
                        if (exists.add(nextInstanceTime)) {
                            instances.add(nextInstanceTime);
                        }
                        nextInstanceTime = new Date(nextInstanceTime.getTime() + ONE_MS);
                        nextInstanceTime = EntityUtil.getNextStartTime(feed, currentCluster, nextInstanceTime);
                    }
                    pendingInstances.put(key, instances);
                }
            }
        }
    }


    /**
     * Checks the availability of all the pendingInstances and removes the ones which have become available.
     */
    private void checkPendingInstanceAvailability() throws FalconException {
        for (Map.Entry<Pair<String, String>, BlockingQueue<Date>> entry: pendingInstances.entrySet()) {
            for (Date date : entry.getValue()) {
                boolean status = checkFeedInstanceAvailability(entry.getKey().first, entry.getKey().second, date);
                if (status) {
                    pendingInstances.get(entry.getKey()).remove(date);
                }
            }
        }
    }

    // checks whether a given feed instance is available or not
    private boolean checkFeedInstanceAvailability(String feedName, String clusterName, Date nominalTime) throws
        FalconException {
        Feed feed = EntityUtil.getEntity(EntityType.FEED, feedName);

        try {
            LOG.debug("Checking instance availability status for feed:{}, cluster:{}, instanceTime:{}", feed.getName(),
                    clusterName, nominalTime);
            FeedInstanceStatus.AvailabilityStatus status = FeedHelper.getFeedInstanceStatus(feed, clusterName,
                    nominalTime);
            if (status.equals(FeedInstanceStatus.AvailabilityStatus.AVAILABLE)
                    || status.equals(FeedInstanceStatus.AvailabilityStatus.EMPTY)) {
                LOG.debug("Feed instance(feed:{}, cluster:{}, instanceTime:{}) is available.", feed.getName(),
                    clusterName, nominalTime);
                return true;
            }
        } catch (Throwable e) {
            LOG.error("Couldn't find status for feed:{}, cluster:{}", feedName, clusterName, e);
        }
        LOG.debug("Feed instance(feed:{}, cluster:{}, instanceTime:{}) is not available.", feed.getName(),
            clusterName, nominalTime);
        return false;
    }

    private void serializeState() throws FalconException{
        LOG.info("Saving context to: [{}]", storePath);

        //create a temporary file and rename it.
        Path tmp = new Path(storePath , "tmp");
        ObjectOutputStream oos = null;
        try {
            OutputStream out = fileSystem.create(tmp);
            oos = new ObjectOutputStream(out);
            Map<String, Object> state = new HashMap<>();
            state.put("lastSerializedAt", lastSerializedAt.getTime());
            state.put("lastCheckedAt", lastCheckedAt.getTime());
            state.put("pendingInstances", pendingInstances);
            oos.writeObject(state);
            fileSystem.rename(tmp, filePath);
        } catch (IOException e) {
            throw new FalconException("Error serializing context to : " + storePath.toUri(),  e);
        } finally {
            IOUtils.closeQuietly(oos);
        }
    }

    @SuppressWarnings("unchecked")
    private void deserialize(Path path) throws FalconException {
        try {
            Map<String, Object> state = deserializeInternal(path);
            pendingInstances = new ConcurrentHashMap<>();
            Map<Pair<String, String>, BlockingQueue<Date>> pendingInstancesCopy =
                    (Map<Pair<String, String>, BlockingQueue<Date>>) state.get("pendingInstances");
            // queue size can change during restarts, hence copy
            for (Map.Entry<Pair<String, String>, BlockingQueue<Date>> entry : pendingInstancesCopy.entrySet()) {
                BlockingQueue<Date> value = new LinkedBlockingQueue<>(queueSize);
                BlockingQueue<Date> oldValue = entry.getValue();
                LOG.debug("Number of old instances:{}, new queue size:{}", oldValue.size(), queueSize);
                while (!oldValue.isEmpty()) {
                    Date instance = oldValue.remove();
                    if (value.size() == queueSize) { // if full
                        LOG.debug("Deserialization: Removing value={} for <feed,cluster>={}", value.peek(),
                            entry.getKey());
                        value.remove();
                    }
                    LOG.debug("Deserialization Adding: key={} to <feed,cluster>={}", entry.getKey(), instance);
                    value.add(instance);
                }
                pendingInstances.put(entry.getKey(), value);
            }
            lastCheckedAt = new Date((Long) state.get("lastCheckedAt"));
            lastSerializedAt = new Date((Long) state.get("lastSerializedAt"));
            monitoredFeeds = new ConcurrentHashSet<>(); // will be populated on the onLoad of entities.
            LOG.debug("Restored the service from old state.");
        } catch (IOException | ClassNotFoundException e) {
            throw new FalconException("Couldn't deserialize the old state", e);
        }
    }

    protected void initializeService() {
        pendingInstances = new ConcurrentHashMap<>();
        lastCheckedAt = new Date();
        lastSerializedAt = new Date();
        monitoredFeeds = new ConcurrentHashSet<>();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> deserializeInternal(Path path) throws IOException, ClassNotFoundException {
        Map<String, Object> state;
        InputStream in = fileSystem.open(path);
        ObjectInputStream ois = new ObjectInputStream(in);
        try {
            state = (Map<String, Object>) ois.readObject();
        } finally {
            IOUtils.closeQuietly(ois);
        }
        return state;
    }

    /**
     * Returns all {@link org.apache.falcon.entity.v0.feed.Feed} instances between given time range which have missed
     * slaLow or slaHigh.
     *
     * Only feeds which have defined sla in their definition are considered.
     * Only the feed instances between the given time range are considered.
     * Start time and end time are both inclusive.
     * @param start start time, inclusive
     * @param end end time, inclusive
     * @return Set of pending feed instances belonging to the given range which have missed SLA
     * @throws FalconException
     */
    public Set<SchedulableEntityInstance> getFeedSLAMissPendingAlerts(Date start, Date end)
        throws FalconException {
        Set<SchedulableEntityInstance> result = new HashSet<>();
        for (Map.Entry<Pair<String, String>, BlockingQueue<Date>> feedInstances : pendingInstances.entrySet()) {
            Pair<String, String> feedClusterPair = feedInstances.getKey();
            Feed feed = EntityUtil.getEntity(EntityType.FEED, feedClusterPair.first);
            Cluster cluster = FeedHelper.getCluster(feed, feedClusterPair.second);
            Sla sla = FeedHelper.getSLA(cluster, feed);
            if (sla != null) {
                Set<Pair<Date, String>> slaStatus = getSLAStatus(sla, start, end, feedInstances.getValue());
                for (Pair<Date, String> status : slaStatus){
                    SchedulableEntityInstance instance = new SchedulableEntityInstance(feedClusterPair.first,
                            feedClusterPair.second, status.first, EntityType.FEED);
                    instance.setTags(status.second);
                    result.add(instance);
                }
            }
        }
        return result;
    }

    /**
     * Returns all {@link org.apache.falcon.entity.v0.feed.Feed} instances of a given feed between the given time range
     * which missed sla.Only those instances are included which have missed either slaLow or slaHigh.
     * @param feedName name of the feed
     * @param clusterName cluster name
     * @param start start time, inclusive
     * @param end end time, inclusive
     * @return Pending feed instances of the given feed which belong to the given time range and have missed SLA.
     * @throws FalconException
     */
    public Set<SchedulableEntityInstance> getFeedSLAMissPendingAlerts(String feedName, String clusterName,
                                                                 Date start, Date end) throws FalconException {

        Set<SchedulableEntityInstance> result = new HashSet<>();
        Pair<String, String> feedClusterPair = new Pair<>(feedName, clusterName);
        BlockingQueue<Date> missingInstances = pendingInstances.get(feedClusterPair);
        Feed feed = EntityUtil.getEntity(EntityType.FEED, feedName);
        Cluster cluster = FeedHelper.getCluster(feed, feedClusterPair.second);
        Sla sla = FeedHelper.getSLA(cluster, feed);
        if (missingInstances != null && sla != null) {
            Set<Pair<Date, String>> slaStatus = getSLAStatus(sla, start, end, missingInstances);
            for (Pair<Date, String> status : slaStatus){
                SchedulableEntityInstance instance = new SchedulableEntityInstance(feedName, clusterName, status.first,
                        EntityType.FEED);
                instance.setTags(status.second);
                result.add(instance);
            }
        }
        return result;
    }

    Set<Pair<Date, String>> getSLAStatus(Sla sla, Date start, Date end, BlockingQueue<Date> missingInstances)
        throws FalconException {
        String tagCritical = "Missed SLA High";
        String tagWarn = "Missed SLA Low";
        Date now = new Date();
        Frequency slaLow = sla.getSlaLow();
        Frequency slaHigh = sla.getSlaHigh();
        Set<Pair<Date, String>> result = new HashSet<>();
        for (Date nominalTime : missingInstances) {
            if (!nominalTime.before(start) && !nominalTime.after(end)) {
                ExpressionHelper.setReferenceDate(nominalTime);
                ExpressionHelper evaluator = ExpressionHelper.get();
                Long slaHighDuration = evaluator.evaluate(slaHigh.toString(), Long.class);
                Long slaLowDuration = evaluator.evaluate(slaLow.toString(), Long.class);
                Date slaCriticalTime = new Date(nominalTime.getTime() + slaHighDuration);
                Date slaWarnTime = new Date(nominalTime.getTime() + slaLowDuration);
                if (slaCriticalTime.before(now)) {
                    result.add(new Pair<>(nominalTime, tagCritical));
                } else if (slaWarnTime.before(now)) {
                    result.add(new Pair<>(nominalTime, tagWarn));
                }
            }
        }
        return result;
    }
}
