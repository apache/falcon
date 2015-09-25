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
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.hadoop.HadoopClientFactory;
import org.apache.falcon.util.DeploymentUtil;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Service to monitor Feed SLAs.
 */
public class FeedSLAMonitoringService implements ConfigurationChangeListener, FalconService {
    private static final Logger LOG = LoggerFactory.getLogger("FeedSLA");

    private static final String ONE_HOUR = String.valueOf(60 * 60 * 1000);

    private static final int ONE_MS = 1;

    /**
     * Permissions for storePath.
     */
    private static final FsPermission STORE_PERMISSION = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);

    /**
     * Feeds to be monitored.
     */
    private static Set<String> monitoredFeeds;


    /**
     * Map<Pair<feedName, clusterName>, Set<instanceTime> to store
     * each missing instance of a feed.
     */
    private static Map<Pair<String, String>, Set<Date>> pendingInstances;


    /**
     * Used to store the last time when pending instances were checked for SLA.
     */
    private static Date lastCheckedAt;


    /**
     * Used to store last time when the state was serialized to the store.
     */
    private static Date lastSerializedAt;

    /**
     * Frequency in seconds of "status check" for pending feed instances.
     */
    private static final int STATUS_CHECK_FREQUENCY_SECS = 10 * 60; // 10 minutes


    /**
     * Time Duration (in milliseconds) in future for generating pending feed instances.
     *
     * In every cycle pending feed instances are added for monitoring, till this time in future.
     */
    private static final int LOOKAHEAD_WINDOW_MILLIS = 15 * 60 * 1000; // 15 MINUTES


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
            if (feed.getLocations() != null && feed.getSla() != null) {
                Set<String> currentClusters = DeploymentUtil.getCurrentClusters();
                for (Cluster cluster : feed.getClusters().getClusters()) {
                    if (currentClusters.contains(cluster.getName())) {
                        LOG.debug("Adding feed:{} for monitoring", feed.getName());
                        monitoredFeeds.add(feed.getName());
                    }
                }
            }
        }
    }

    @Override
    public void onRemove(Entity entity) throws FalconException {
        if (entity.getEntityType() == EntityType.FEED) {
            Feed feed = (Feed) entity;
            if (feed.getSla() != null) {
                Set<String> currentClusters = DeploymentUtil.getCurrentClusters();
                for (Cluster cluster : feed.getClusters().getClusters()) {
                    if (currentClusters.contains(cluster.getName())) {
                        monitoredFeeds.remove(feed.getName());
                        break;
                    }
                }
            }
        }
    }

    @Override
    public void onChange(Entity oldEntity, Entity newEntity) throws FalconException {
        onRemove(oldEntity);
        onAdd(newEntity);
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
        try {
            serializationFrequencyMillis = Integer.valueOf(freq);
        } catch (NumberFormatException e) {
            LOG.error("Invalid value : {} found in startup.properties for the property "
                    + "feed.sla.serialization.frequency.millis Should be an integer", freq);
            throw new FalconException("Invalid integer value for property ", e);
        }
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
        executor.scheduleWithFixedDelay(new Monitor(), 0, STATUS_CHECK_FREQUENCY_SECS, TimeUnit.SECONDS);
    }

    @Override
    public void destroy() throws FalconException {
        serializeState(); // store the state of monitoring service to the disk.
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
                    Date newCheckPoint = new Date(now.getTime() + LOOKAHEAD_WINDOW_MILLIS);
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
                    Set<Date> instances = pendingInstances.get(key);
                    if (instances == null) {
                        instances = new HashSet<>();
                    }

                    org.apache.falcon.entity.v0.cluster.Cluster currentCluster =
                            EntityUtil.getEntity(EntityType.CLUSTER, feedCluster.getName());
                    while (nextInstanceTime.before(to)) {
                        nextInstanceTime = EntityUtil.getNextStartTime(feed, currentCluster, nextInstanceTime);
                        instances.add(nextInstanceTime);
                        nextInstanceTime = new Date(nextInstanceTime.getTime() + ONE_MS);
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
        for (Map.Entry<Pair<String, String>, Set<Date>> entry: pendingInstances.entrySet()) {
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
            if (status == FeedInstanceStatus.AvailabilityStatus.AVAILABLE) {
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
            pendingInstances = (Map<Pair<String, String>, Set<Date>>) state.get("pendingInstances");
            lastCheckedAt = new Date((Long) state.get("lastCheckedAt"));
            lastSerializedAt = new Date((Long) state.get("lastSerializedAt"));
            monitoredFeeds = new HashSet<>(); // will be populated on the onLoad of entities.
            LOG.debug("Restored the service from old state.");
        } catch (IOException | ClassNotFoundException e) {
            throw new FalconException("Couldn't deserialize the old state", e);
        }
    }

    private void initializeService() {
        pendingInstances = new HashMap<>();
        lastCheckedAt = new Date();
        lastSerializedAt = new Date();
        monitoredFeeds = new HashSet<>();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> deserializeInternal(Path path) throws IOException, ClassNotFoundException {
        Map<String, Object> state;
        InputStream in = fileSystem.open(path);
        ObjectInputStream ois = new ObjectInputStream(in);
        try {
            state = (Map<String, Object>) ois.readObject();
        } finally {
            ois.close();
        }
        return state;
    }

}

