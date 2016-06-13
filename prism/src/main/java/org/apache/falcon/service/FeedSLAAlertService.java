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

import java.util.*;
import java.util.concurrent.*;

import javax.annotation.Nonnull;
import javax.persistence.EntityManager;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import org.apache.commons.lang3.StringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.*;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.*;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.jdbc.MonitoringJdbcStateStore;
import org.apache.falcon.persistence.FeedSLAAlertBean;
import org.apache.falcon.persistence.PendingInstanceBean;
import org.apache.falcon.resource.SchedulableEntityInstance;
import org.apache.falcon.util.DateUtil;
import org.apache.falcon.util.DeploymentUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  *
  */
public final class FeedSLAAlertService implements FalconService {

    private static final String NAME = "FeedSLAAlertService";

    private static final Logger LOG = LoggerFactory.getLogger(FeedSLAAlertService.class);

    DelayQueue<AlertObject> slaLowCandidates;

    DelayQueue<AlertObject> slaHighCandidates;

    Multimap<Pair,Date> feedInstancesToBeMonitored ;

    MonitoringJdbcStateStore store = new MonitoringJdbcStateStore();

/**
  * Permissions for storePath.
  */
    private static final FsPermission STORE_PERMISSION = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);


    private static final FeedSLAAlertService SERVICE = new FeedSLAAlertService();

    public static FeedSLAAlertService get() {
        return SERVICE;
    }

    private EntityManager getEntityManager() {
        return FalconJPAService.get().getEntityManager();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void init() throws FalconException {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.scheduleWithFixedDelay(new Monitor(), 0, 20, TimeUnit.SECONDS);
    }

    @Override
    public void destroy() throws FalconException {

    }

//    @Override
//    public void onAdd(Entity entity) throws FalconException {
//        if (entity.getEntityType() == EntityType.FEED) {
//            Feed feed = (Feed)entity;
//            Sla sla = feed.getSla();
//    //TODO  get first eligible instance for sla alert and put it in database.
//            if (feed.getLocations() != null && sla != null) {
//                Set<String> currentClusters = DeploymentUtil.getCurrentClusters();
//                for (Cluster cluster : feed.getClusters().getClusters()) {
//                    if (currentClusters.contains(cluster.getName())) {
//                        addSLACandidates(feed, cluster);
//                    }
//                }
//            }
//        }
//    }
//
//    @Override
//    public void onRemove(Entity entity) throws FalconException {
//        if (entity.getEntityType() == EntityType.FEED) {
//            Feed feed = (Feed)entity;
//    // currently sla service is enabled only for fileSystemStorage
//            if (feed.getLocations() != null) {
//                Set<String> currentClusters = DeploymentUtil.getCurrentClusters();
//                for (Cluster cluster : feed.getClusters().getClusters()) {
//                    if (currentClusters.contains(cluster.getName())) {
//                        removeSLACandidates(feed, cluster);
//                    }
//                }
//            }
//        }
//    }
//
//    @Override
//    public void onChange(Entity oldEntity, Entity newEntity) throws FalconException {
//        // if sla is removed or sla is added then update slaLowCandidates & slaHighCandidates accordingly.
//        if (newEntity.getEntityType() == EntityType.FEED) {
//            Feed oldFeed = (Feed)oldEntity;
//            Feed newFeed = (Feed)newEntity;
//    // currently sla service is enabled only for fileSystemStorage
//            if (oldFeed.getLocations() != null) {
//    // for those clusters from which sla is removed, remove the alerts from candidates
//                Set<Cluster> addedClusters = FeedHelper.getAddedClusters(oldFeed, newFeed);
//                for (Cluster cluster : addedClusters) {
//                    addSLACandidates(newFeed, cluster);
//                }
//     // for those clusters on which sla is added, add to candidates
//                Set<Cluster> removedClusters = FeedHelper.getRemovedClusters(oldFeed, newFeed);
//                for (Cluster cluster : removedClusters) {
//                    removeSLACandidates(oldFeed, cluster);
//                }
//            }
//        }
//    }
//
//    @Override
//    public void onReload(Entity entity) throws FalconException {
//        onAdd(entity);
//    }

    public class Pair<L,R> {

        private final L left;
        private final R right;

        public Pair(L left, R right) {
            this.left = left;
            this.right = right;
        }

        public L getLeft() { return left; }
        public R getRight() { return right; }

        @Override
        public int hashCode() { return left.hashCode() ^ right.hashCode(); }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Pair)) return false;
            Pair pairo = (Pair) o;
            return this.left.equals(pairo.getLeft()) &&
                    this.right.equals(pairo.getRight());
        }

    }

    class AlertObject implements Delayed {

        private SchedulableEntityInstance instance;

        private Date slaTime;

        public AlertObject(SchedulableEntityInstance instance, Frequency slaDuration, TimeZone timeZone)
            throws FalconException {

            this.instance = instance;
            this.slaTime = DateUtil.addFrequency(instance.getInstanceTime(), slaDuration, timeZone);
        }

        @Override
        public long getDelay(@Nonnull TimeUnit unit) {
            return unit.convert((slaTime.getTime() - System.currentTimeMillis()), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(@Nonnull Delayed delayed) {
            if (delayed == this) {
                return 0;
            }

            long diff = (getDelay(TimeUnit.MILLISECONDS) - delayed.getDelay(TimeUnit.MILLISECONDS));
            return diff == 0 ? 0 : diff < 0 ? -1 : 1;
        }

        @Override
        public boolean equals(Object o) {
        // equals tests only for the entity name and cluster because
        // we want to remove the instance of the entity irrespective of the instanceTime
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            AlertObject that = (AlertObject) o;

            return StringUtils.equals(instance.getEntityName(), that.instance.getEntityName())
                    && StringUtils.equals(instance.getCluster(), that.instance.getCluster());
        }

        @Override
        public int hashCode() {
            int result = this.instance.getCluster().hashCode();
            result = 31 * result + this.instance.getEntityName().hashCode();
            return result;
        }
    }

    private class Monitor implements Runnable {

        @Override
        public void run() {
            processSLALowCandidates();
            processSLAHighCandidates();
        }
    }

    void processSLALowCandidates(){
//Get all feeds instances to be monitored
        List<PendingInstanceBean> pendingInstanceBeanList = store.getAllInstances();

        if(!pendingInstanceBeanList.isEmpty()){
            for (PendingInstanceBean pendingInstanceBean : pendingInstanceBeanList) {
                Pair pair = new Pair(pendingInstanceBean.getFeedName(),pendingInstanceBean.getClusterName());
                feedInstancesToBeMonitored.put(pair, pendingInstanceBean.getNominalTime()) ;
            }

        try{
            int length = feedInstancesToBeMonitored.size();

            Callable callable ;
            for(Map.Entry<Pair, Date> pairDateEntry : feedInstancesToBeMonitored.entries()) {

                Pair pair = pairDateEntry.getKey();
                Feed feed = ConfigurationStore.get().get(EntityType.FEED, (String) pair.getLeft());

                Cluster cluster =  FeedHelper.getCluster(feed,(String)pair.getRight());

                FileSystemStorage fileSystemStorage = new FileSystemStorage(FileSystemStorage.FILE_SYSTEM_URL,
                        cluster.getLocations());

                // Get the status of the feed instance from HDFS if it is available look for the time stamp wheather it missed the sla
                //if its not available look if the instance has missed the SLA low mark
                List<FeedInstanceStatus> feedStatus = fileSystemStorage.
                        getListing(feed, cluster.getName(), LocationType.DATA, pairDateEntry.getValue(),
                                pairDateEntry.getValue());
                //Only one result would always be retured as start and end data is same.
                FeedInstanceStatus feedInstanceStatus =  feedStatus.get(0);
                Long creationTime =  feedInstanceStatus.getCreationTime() == 0L ? System.nanoTime()
                        : feedInstanceStatus.getCreationTime();
                Date createdTime =  new Date(creationTime*1000);
                Frequency slaLow  = feed.getSla().getSlaLow();
                ExpressionHelper evaluator = ExpressionHelper.get();
                Long slaLowDuration = evaluator.evaluate(slaLow.toString(), Long.class);
                Date slaLowTime = new Date(pairDateEntry.getValue().getTime() + slaLowDuration);

                if(createdTime.after(slaLowTime)) {
                    store.putSLALowCandidate(pair.getLeft().toString(),pair.getRight().toString(),
                            pairDateEntry.getValue(),true,false);
                //Mark in DB as SLA missed
                    LOG.info( "Feed :"+ pairDateEntry.getKey().getLeft() +
                            "Cluster:" + pairDateEntry.getKey().getRight() + "Nominal Time:" + pairDateEntry.getValue()
                            + "missed SLA");
                }
                else{
                    feedInstancesToBeMonitored.remove(pairDateEntry.getKey(),pairDateEntry.getValue());
                }

            }
        } catch (FalconException e){
            LOG.error("Exception in FeedSLAALertService:"+ e);

            }
        }

    }

    class abc implements Callable{
        public abc(List<>){

        }
        public Integer  call(){
            return 1;
        }
    }

    void processSLAHighCandidates(){
        List<FeedSLAAlertBean> feedSLAAlertBeanList = store.getSLAHighCandidates();

        if(!feedSLAAlertBeanList.isEmpty()) {
            for (FeedSLAAlertBean bean : feedSLAAlertBeanList) {
                try {
                    Feed feed = ConfigurationStore.get().get(EntityType.FEED, (String) bean.getFeedName());
                    Cluster cluster =  FeedHelper.getCluster(feed,bean.getClusterName());
                    Date nominalTime = bean.getNominalTime();

                    Frequency slaHigh = feed.getSla().getSlaHigh();
                    ExpressionHelper evaluator = ExpressionHelper.get();
                    Long slaHighDuration = evaluator.evaluate(slaHigh.toString(), Long.class);
                    Date slaHighTime = new Date(nominalTime.getTime() + slaHighDuration);
                    FileSystemStorage fileSystemStorage = new FileSystemStorage(FileSystemStorage.FILE_SYSTEM_URL,
                            cluster.getLocations());

                    List<FeedInstanceStatus> feedStatus = fileSystemStorage.
                            getListing(feed, cluster.getName(), LocationType.DATA, nominalTime,nominalTime);

                    FeedInstanceStatus feedInstanceStatus = feedStatus.get(0);

                    Long creationTime =  feedInstanceStatus.getCreationTime() == 0L ? System.nanoTime()
                            : feedInstanceStatus.getCreationTime();
                    Date createdTime = new Date(creationTime*1000);

                    if(slaHighTime.before(createdTime)){
                        store.updateSLAHighCandidate(bean.getFeedName(),bean.getClusterName(),nominalTime);
                    }


                } catch (FalconException e){
                    LOG.error("Exception in FeedSLAALertService processSLAHighCandidates:" + e);
                }

            }
        }

    }

//    void processSLALowCandidates() {
//        MonitoringJdbcStateStore store = new MonitoringJdbcStateStore();
//        AlertObject next;
//        while (((next = slaLowCandidates.poll()) != null)) {
//
//            SchedulableEntityInstance instance = next.instance;
//            // check if this instance is still pending
//            PendingInstanceBean pendingInstanceBean = store.getParticularPendingInstance(instance.getEntityName(),
//                    instance.getCluster(), instance.getInstanceTime());
//            if (pendingInstanceBean != null) {
//            // send alert - for now just log it.
//                LOG.info("SLA Low Missed for feed: {}, cluster: {}, instanceTime: {}", instance.getEntityName(),
//                    instance.getCluster(), DateUtil.getDateStringFromDate(instance.getInstanceTime()));
//                try {
//                    Feed feed = ConfigurationStore.get().get(EntityType.FEED, instance.getEntityName());
//                    Sla sla = FeedHelper.getSLA(instance.getCluster(), feed);
//                    if (sla.getSlaHigh() != null) {
//                        slaHighCandidates.add(next);
//                    }
//                    Cluster cluster = FeedHelper.getCluster(feed, instance.getCluster());
//                    if (cluster == null) {
//                        throw new FalconException("Invalid cluster: " + instance.getCluster() + " for the feed: "
//                                + feed.getName());
//                    }
//                    Date referenceTime = DateUtil.getNextMinute(instance.getInstanceTime());
//                    Date nextInstanceTime = EntityUtil.getNextStartTime(cluster.getValidity().getStart(),
//                            feed.getFrequency(), feed.getTimezone(), referenceTime);
//                    SchedulableEntityInstance newInstance = new SchedulableEntityInstance(instance.getEntityName(),
//                            instance.getCluster(),
//                        nextInstanceTime, EntityType.FEED);
//                    AlertObject newObject = new AlertObject(newInstance, sla.getSlaLow(), feed.getTimezone());
//                    slaLowCandidates.add(newObject);
//                } catch (FalconException e) {
//                    LOG.error("Couldn't find feed:{} matching to SLA alert", instance.getEntityName());
//                }
//
//
//            }
//        }
//    }

//    void processSLAHighCandidates() {
//        MonitoringJdbcStateStore store = new MonitoringJdbcStateStore();
//        AlertObject next;
//        while (((next = slaHighCandidates.poll()) != null)) {
//            SchedulableEntityInstance instance = next.instance;
//
//// check if this instance is still pending
//            if (true) {
//// send alert - for now just log it.
//                LOG.info("SLA Low Missed for feed: {}, cluster: {}, instanceTime: {}", instance.getEntityName(),
//                        instance.getCluster(), DateUtil.getDateStringFromDate(instance.getInstanceTime()));
//                try {
//                    Feed feed = ConfigurationStore.get().get(EntityType.FEED, instance.getEntityName());
//                    Sla sla = FeedHelper.getSLA(instance.getCluster(), feed);
//                    Cluster cluster = FeedHelper.getCluster(feed, instance.getCluster());
//                    if (cluster == null) {
//                        throw new FalconException("Invalid cluster: " + instance.getCluster() + " for the feed: "
//                                + feed.getName());
//                    }
//
//                    Date referenceTime = DateUtil.getNextMinute(instance.getInstanceTime());
//                    Date nextInstanceTime = EntityUtil.getNextStartTime(cluster.getValidity().getStart(),
//                        feed.getFrequency(), feed.getTimezone(), referenceTime);
//                    SchedulableEntityInstance newInstance = new SchedulableEntityInstance(instance.getEntityName(),
//                        instance.getCluster(), nextInstanceTime, EntityType.FEED);
//                    AlertObject newObject = new AlertObject(newInstance, sla.getSlaHigh(), feed.getTimezone());
//                    slaHighCandidates.add(newObject);
//                } catch (FalconException e) {
//                    LOG.error("Couldn't find feed:{}, cluster:{} matching to SLA alert", instance.getEntityName(),
//                            instance.getCluster());
//                }
//            }
//        }
//    }

    @SuppressWarnings("unchecked")
    private void deserialize(Path path) throws FalconException {
    }

// TODO: sla candidates should be calcualted from database of pending instances or database of feed alerts
// slaLow candidate is the first sla alert feed with the least nominal time where the slaLow is not missed for each
// calculating sla low candidate
// for every feed find the pending instance with the lowest instance time.
// how do we know which are sla high candidates among these, first make an entry for sla low candidates
// any sla high would have missed sla low also.

// Keep your own table for sla low candidates and sla high candidates
// in this table the candidates will have sla low missed and sla high missed as false.
// when a candidate misses sla low, we will mark its sla low to be present
// a candidate which is marked as sla low missed is a candidate for sla high
// if a sla low candidate is not missed then we should update its nominal time to be of next instance

// we should add two more fields - is sla low alerted, is sla high alerted and is alerts suppressed
// (to stop deluge of alerts)
// after processing sla low, pick sla what will happen after a long time of server start, many feeds missed sla in that time
// but when falcon came up it found that those are available so it deleted those entries and we will miss some
// alerts in such cases.
    private void addSLACandidates(Feed feed, Cluster cluster) throws FalconException {
        LOG.debug("Adding feed: {}, cluster: {} for sla alerts", feed.getName(), cluster.getName());
        Sla sla = FeedHelper.getSLA(cluster, feed);
        if (sla != null) {
            if (sla.getSlaLow() != null) {
                SchedulableEntityInstance instance = new SchedulableEntityInstance(feed.getName(),
                        cluster.getName(), cluster.getValidity().getStart(), EntityType.FEED);
                AlertObject alert = new AlertObject(instance, sla.getSlaLow(), feed.getTimezone());
                if (!slaLowCandidates.contains(alert)) {
                    slaLowCandidates.add(alert);
                }
            }

            if (sla.getSlaHigh() != null) {
                SchedulableEntityInstance instance = new SchedulableEntityInstance(feed.getName(),
                        cluster.getName(), cluster.getValidity().getStart(), EntityType.FEED);
                AlertObject alert = new AlertObject(instance, sla.getSlaHigh(), feed.getTimezone());
                if (!slaHighCandidates.contains(alert)) {
                    slaHighCandidates.add(alert);
                }
            }
        }
    }

    private void removeSLACandidates(Feed feed, Cluster cluster) throws FalconException {
//TODO remove all pending instances of the sla candidates from the table, note that it might cause some failures
// in queries while trying to update database with next instance.
        Sla sla = FeedHelper.getSLA(cluster, feed);
        if (sla != null){
            if (sla.getSlaLow() != null) {

                SchedulableEntityInstance entityObject = new SchedulableEntityInstance(feed.getName(),
                        cluster.getName(), new Date(), EntityType.FEED);
                slaLowCandidates.remove(new AlertObject(entityObject, new Frequency("hours(1)"),
                        TimeZone.getTimeZone("UTC")));
            }

            if (sla.getSlaHigh() != null) {
                SchedulableEntityInstance entityObject = new SchedulableEntityInstance(feed.getName(),
                        cluster.getName(), new Date(), EntityType.FEED);
                slaHighCandidates.remove(new AlertObject(entityObject, new Frequency("hours(1)"),
                        TimeZone.getTimeZone("UTC")));
            }
        }
    }

}