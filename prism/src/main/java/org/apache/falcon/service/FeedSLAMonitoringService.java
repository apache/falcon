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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
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
import org.apache.falcon.jdbc.MonitoringJdbcStateStore;
import org.apache.falcon.persistence.MonitoredFeedsBean;
import org.apache.falcon.persistence.PendingInstanceBean;
import org.apache.falcon.resource.SchedulableEntityInstance;
import org.apache.falcon.util.DateUtil;
import org.apache.falcon.util.DeploymentUtil;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Service to monitor Feed SLAs.
 */
public final class FeedSLAMonitoringService implements ConfigurationChangeListener, FalconService {
    private static final Logger LOG = LoggerFactory.getLogger("FeedSLA");

    private static final MonitoringJdbcStateStore MONITORING_JDBC_STATE_STORE = new MonitoringJdbcStateStore();

    private static final int ONE_MS = 1;

    private static final FeedSLAMonitoringService SERVICE = new FeedSLAMonitoringService();

    public static final String TAG_CRITICAL = "Missed-SLA-High";
    public static final String TAG_WARN = "Missed-SLA-Low";

    private FeedSLAMonitoringService() {

    }

    public static FeedSLAMonitoringService get() {
        return SERVICE;
    }

    /**
     * Permissions for storePath.
     */
    private static final FsPermission STORE_PERMISSION = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);


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
                            MONITORING_JDBC_STATE_STORE.putMonitoredFeed(feed.getName());
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
                        MONITORING_JDBC_STATE_STORE.deleteMonitoringFeed(feed.getName());
                        MONITORING_JDBC_STATE_STORE.deletePendingInstances(feed.getName(), cluster.getName());
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
                    MONITORING_JDBC_STATE_STORE.deletePendingInstances(newFeed.getName(), clusterName);
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

        String freq = StartupProperties.get().getProperty("feed.sla.statusCheck.frequency.seconds", "600");
        statusCheckFrequencySeconds = Integer.parseInt(freq);

        freq = StartupProperties.get().getProperty("feed.sla.lookAheadWindow.millis", "900000");
        lookAheadWindowMillis = Integer.parseInt(freq);
        LOG.debug("No old state exists at: {}, Initializing a clean state.", filePath.toString());
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.scheduleWithFixedDelay(new Monitor(), 0, statusCheckFrequencySeconds, TimeUnit.SECONDS);
    }

    public void makeFeedInstanceAvailable(String feedName, String clusterName, Date nominalTime)
        throws FalconException {
        LOG.info("Removing {} feed's instance {} in cluster {} from pendingSLA", feedName,
                clusterName, nominalTime);
        List<Date> instances = (MONITORING_JDBC_STATE_STORE.getNominalInstances(feedName, clusterName));
        // Slas for feeds not having sla tag are not stored.
        if (CollectionUtils.isEmpty(instances)){
            MONITORING_JDBC_STATE_STORE.deletePendingInstance(feedName, clusterName, nominalTime);
        }
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

    @Override
    public void destroy() throws FalconException {
    }

    //Periodically update status of pending instances, add new instances and take backup.
    private class Monitor implements Runnable {

        @Override
        public void run() {
            try {
                if (MONITORING_JDBC_STATE_STORE.getAllMonitoredFeed().size() > 0) {
                    checkPendingInstanceAvailability();

                    // add Instances from last checked time to 10 minutes from now(some buffer for status check)
                    Date now = new Date();
                    Date newCheckPoint = new Date(now.getTime() + lookAheadWindowMillis);
                    addNewPendingFeedInstances(newCheckPoint);
                }
            } catch (Throwable e) {
                LOG.error("Feed SLA monitoring failed: ", e);
            }
        }
    }


    void addNewPendingFeedInstances(Date to) throws FalconException {
        Set<String> currentClusters = DeploymentUtil.getCurrentClusters();
        List<MonitoredFeedsBean> feedsBeanList = MONITORING_JDBC_STATE_STORE.getAllMonitoredFeed();
        for(MonitoredFeedsBean monitoredFeedsBean : feedsBeanList) {
            String feedName = monitoredFeedsBean.getFeedName();
            Feed feed = EntityUtil.getEntity(EntityType.FEED, feedName);
            for (Cluster feedCluster : feed.getClusters().getClusters()) {
                if (currentClusters.contains(feedCluster.getName())) {
                    // get start of instances from the database
                    Date nextInstanceTime = MONITORING_JDBC_STATE_STORE.getLastInstanceTime(feedName);
                    Pair<String, String> key = new Pair<>(feed.getName(), feedCluster.getName());
                    if (nextInstanceTime == null) {
                        nextInstanceTime = getInitialStartTime(feed, feedCluster.getName());
                    } else {
                        nextInstanceTime = new Date(nextInstanceTime.getTime() + ONE_MS);
                    }

                    Set<Date> instances = new HashSet<>();
                    org.apache.falcon.entity.v0.cluster.Cluster currentCluster =
                            EntityUtil.getEntity(EntityType.CLUSTER, feedCluster.getName());
                    nextInstanceTime = EntityUtil.getNextStartTime(feed, currentCluster, nextInstanceTime);
                    Date endDate = FeedHelper.getClusterValidity(feed, currentCluster.getName()).getEnd();
                    while (nextInstanceTime.before(to) && nextInstanceTime.before(endDate)) {
                        LOG.debug("Adding instance={} for <feed,cluster>={}", nextInstanceTime, key);
                        instances.add(nextInstanceTime);
                        nextInstanceTime = new Date(nextInstanceTime.getTime() + ONE_MS);
                        nextInstanceTime = EntityUtil.getNextStartTime(feed, currentCluster, nextInstanceTime);
                    }

                    for(Date date:instances){
                        MONITORING_JDBC_STATE_STORE.putPendingInstances(feed.getName(), feedCluster.getName(), date);
                    }
                }
            }
        }
    }


    /**
     * Checks the availability of all the pendingInstances and removes the ones which have become available.
     */
    private void checkPendingInstanceAvailability() throws FalconException {
        for(PendingInstanceBean pendingInstanceBean : MONITORING_JDBC_STATE_STORE.getAllInstances()){
            for (Date date : MONITORING_JDBC_STATE_STORE.getNominalInstances(pendingInstanceBean.getFeedName(),
                    pendingInstanceBean.getClusterName())) {
                boolean status = checkFeedInstanceAvailability(pendingInstanceBean.getFeedName(),
                        pendingInstanceBean.getClusterName(), date);
                if (status) {
                    MONITORING_JDBC_STATE_STORE.deletePendingInstance(pendingInstanceBean.getFeedName(),
                            pendingInstanceBean.getClusterName(), date);
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
        for(PendingInstanceBean pendingInstanceBean : MONITORING_JDBC_STATE_STORE.getAllInstances()){
            Pair<String, String> feedClusterPair = new Pair<>(pendingInstanceBean.getFeedName(),
                    pendingInstanceBean.getClusterName());
            Feed feed = EntityUtil.getEntity(EntityType.FEED, feedClusterPair.first);
            Cluster cluster = FeedHelper.getCluster(feed, feedClusterPair.second);
            Sla sla = FeedHelper.getSLA(cluster, feed);
            if (sla != null) {
                Set<Pair<Date, String>> slaStatus = getSLAStatus(sla, start, end,
                    MONITORING_JDBC_STATE_STORE.getNominalInstances(pendingInstanceBean.getFeedName(),
                        pendingInstanceBean.getClusterName()));
                for (Pair<Date, String> status : slaStatus) {
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
        List<Date> missingInstances = MONITORING_JDBC_STATE_STORE.getNominalInstances(feedName, clusterName);
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

    Set<Pair<Date, String>> getSLAStatus(Sla sla, Date start, Date end, List<Date> missingInstances)
        throws FalconException {
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
                    result.add(new Pair<>(nominalTime, TAG_CRITICAL));
                } else if (slaWarnTime.before(now)) {
                    result.add(new Pair<>(nominalTime, TAG_WARN));
                }
            }
        }
        return result;
    }

    @VisibleForTesting
    Date getInitialStartTime(Feed feed, String clusterName) throws FalconException {
        Sla sla = FeedHelper.getSLA(clusterName, feed);
        if (sla == null) {
            throw new IllegalStateException("InitialStartTime can not be determined as the feed: "
                + feed.getName() + " and cluster: " + clusterName + " does not have any sla");
        }
        Date startTime = FeedHelper.getFeedValidityStart(feed, clusterName);
        Frequency slaLow = sla.getSlaLow();
        Date slaTime = new Date(DateUtil.now().getTime() - DateUtil.getFrequencyInMillis(slaLow));
        return startTime.before(slaTime) ? startTime : slaTime;
    }
}
