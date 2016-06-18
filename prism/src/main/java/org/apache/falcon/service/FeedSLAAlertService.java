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
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.persistence.EntityManager;

import com.google.common.collect.Multimap;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.FeedInstanceStatus;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.jdbc.MonitoringJdbcStateStore;
import org.apache.falcon.persistence.FeedSLAAlertBean;
import org.apache.falcon.persistence.PendingInstanceBean;
import org.apache.falcon.util.Pair;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.util.StartupProperties;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  *
  */
public final class FeedSLAAlertService implements FalconService,FeedSLAAlert {

    private static final String NAME = "FeedSLAAlertService";

    private static final Logger LOG = LoggerFactory.getLogger(FeedSLAAlertService.class);

    private Multimap<Pair, Date> feedInstancesToBeMonitored;

    private MonitoringJdbcStateStore store = new MonitoringJdbcStateStore();

    private Set<FeedSLAAlert> listeners = new LinkedHashSet<FeedSLAAlert>();

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
        String listenerClassNames = StartupProperties.get().
                getProperty("feedAlert.listeners");
        for (String listenerClassName : listenerClassNames.split(",")) {
            listenerClassName = listenerClassName.trim();
            if (listenerClassName.isEmpty()) {
                continue;
            }
            FeedSLAAlert listener = ReflectionUtils.getInstanceByClassName(listenerClassName);
            registerListener(listener);
        }
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.scheduleWithFixedDelay(new Monitor(), 0, 20, TimeUnit.SECONDS);
    }

    public void registerListener(FeedSLAAlert listener) {
        listeners.add(listener);
    }

    @Override
    public void destroy() throws FalconException {

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
        if (pendingInstanceBeanList.isEmpty()){
            return;
        }

        for (PendingInstanceBean pendingInstanceBean : pendingInstanceBeanList) {
            Pair pair = new Pair(pendingInstanceBean.getFeedName(), pendingInstanceBean.getClusterName());
            feedInstancesToBeMonitored.put(pair, pendingInstanceBean.getNominalTime());
        }

        try{
            for(Map.Entry<Pair, Date> pairDateEntry : feedInstancesToBeMonitored.entries()) {

                Pair pair = pairDateEntry.getKey();
                Feed feed = ConfigurationStore.get().get(EntityType.FEED, (String) pair.getLeft());

                Cluster cluster =  FeedHelper.getCluster(feed, (String)pair.getRight());


                // Get the status of the feed instance from HDFS if it is available look
                // for the time stamp wheather it missed the sla
                //if its not available look if the instance has missed the SLA low mark
                List<FeedInstanceStatus> feedStatus = FeedHelper.
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

                if (createdTime.after(slaLowTime)) {
                    store.putSLALowCandidate(pair.getLeft().toString(), pair.getRight().toString(),
                            pairDateEntry.getValue(), true, false);
                    //Mark in DB as SLA missed
                    LOG.info("Feed :"+ pairDateEntry.getKey().getLeft()
                                + "Cluster:" + pairDateEntry.getKey().getRight() + "Nominal Time:"
                                + pairDateEntry.getValue() + "missed SLA");
                }
                feedInstancesToBeMonitored.remove(pairDateEntry.getKey(), pairDateEntry.getValue());

            }
        } catch (FalconException e){
            LOG.error("Exception in FeedSLAALertService:", e);
        }

    }

    void processSLAHighCandidates(){
        List<FeedSLAAlertBean> feedSLAAlertBeanList = store.getSLAHighCandidates();

        if (!feedSLAAlertBeanList.isEmpty()) {
            for (FeedSLAAlertBean bean : feedSLAAlertBeanList) {
                try {
                    Feed feed = ConfigurationStore.get().get(EntityType.FEED, (String) bean.getFeedName());
                    Cluster cluster =  FeedHelper.getCluster(feed, bean.getClusterName());
                    Date nominalTime = bean.getNominalTime();

                    Frequency slaHigh = feed.getSla().getSlaHigh();
                    ExpressionHelper evaluator = ExpressionHelper.get();
                    Long slaHighDuration = evaluator.evaluate(slaHigh.toString(), Long.class);
                    Date slaHighTime = new Date(nominalTime.getTime() + slaHighDuration);

                    List<FeedInstanceStatus> feedStatus = FeedHelper.
                            getListing(feed, cluster.getName(), LocationType.DATA, nominalTime, nominalTime);

                    FeedInstanceStatus feedInstanceStatus = feedStatus.get(0);

                    Long creationTime =  feedInstanceStatus.getCreationTime() == 0L ? System.nanoTime()
                            : feedInstanceStatus.getCreationTime();
                    Date createdTime = new Date(creationTime*1000);

                    if (slaHighTime.before(createdTime)){
                        store.updateSLAHighCandidate(bean.getFeedName(), bean.getClusterName(), nominalTime);
                        highSLAMissed(bean.getFeedName(), bean.getClusterName(), nominalTime);
                    }

                } catch (FalconException e){
                    LOG.error("Exception in FeedSLAALertService processSLAHighCandidates:" , e);
                }

            }
        }
    }
    @Override
    public void highSLAMissed(String feedName , String clusterName, Date nominalTime) throws FalconException{
        for (FeedSLAAlert listener : listeners) {
            listener.highSLAMissed(feedName,clusterName,nominalTime);
        }
    }
}
