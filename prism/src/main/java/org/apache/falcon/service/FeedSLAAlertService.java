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


import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.feed.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.jdbc.MonitoringJdbcStateStore;
import org.apache.falcon.persistence.PendingInstanceBean;
import org.apache.falcon.resource.SchedulableEntityInstance;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.util.StartupProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  * Service to know which all feeds have missed SLA.
  */
public final class FeedSLAAlertService implements FalconService, EntitySLAListener {

    private static final String NAME = "FeedSLAAlertService";

    private static final Logger LOG = LoggerFactory.getLogger(FeedSLAAlertService.class);

    private MonitoringJdbcStateStore store = new MonitoringJdbcStateStore();

    private Set<EntitySLAListener> listeners = new LinkedHashSet<EntitySLAListener>();

    private static final FeedSLAAlertService SERVICE = new FeedSLAAlertService();

    public static FeedSLAAlertService get() {
        return SERVICE;
    }

    private FeedSLAAlertService(){}


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
            EntitySLAListener listener = ReflectionUtils.getInstanceByClassName(listenerClassName);
            registerListener(listener);
        }

        String freq = StartupProperties.get().getProperty("feed.sla.statusCheck.frequency.seconds", "600");
        int statusCheckFrequencySeconds = Integer.parseInt(freq);

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.scheduleWithFixedDelay(new Monitor(), 0, statusCheckFrequencySeconds + 10, TimeUnit.SECONDS);
    }

    public void registerListener(EntitySLAListener listener) {
        listeners.add(listener);
    }

    @Override
    public void destroy() throws FalconException {

    }


    private class Monitor implements Runnable {

        @Override
        public void run() {
            processSLACandidates();
        }
    }

    void processSLACandidates(){
        //Get all feeds instances to be monitored
        List<PendingInstanceBean> pendingInstanceBeanList = store.getAllInstances();
        if (pendingInstanceBeanList.isEmpty()){
            return;
        }

        LOG.debug("In processSLACandidates :" + pendingInstanceBeanList.size());
        try{
            for (PendingInstanceBean pendingInstanceBean : pendingInstanceBeanList) {

                String feedName = pendingInstanceBean.getFeedName();
                String clusterName = pendingInstanceBean.getClusterName();
                Date nominalTime = pendingInstanceBean.getNominalTime();
                Feed feed = ConfigurationStore.get().get(EntityType.FEED, feedName);

                Cluster cluster =  FeedHelper.getCluster(feed, clusterName);

                Set<SchedulableEntityInstance> schedulableEntityInstances= FeedSLAMonitoringService.get().
                        getFeedSLAMissPendingAlerts(feed.getName(), cluster.getName(), nominalTime, nominalTime);
                if (schedulableEntityInstances.isEmpty()){
                    store.deleteFeedAlertInstance(feed.getName(), cluster.getName(), nominalTime);
                    return;
                }
                List<SchedulableEntityInstance> schedulableEntityList = new ArrayList<>(schedulableEntityInstances);
                SchedulableEntityInstance schedulableEntityInstance = schedulableEntityList.get(0);


                if (schedulableEntityInstance.getTags().contains(FeedSLAMonitoringService.get().TAG_WARN)) {
                    store.putSLAAlertInstance(feedName, clusterName, nominalTime, true, false);
                    //Mark in DB as SLA missed
                    LOG.info("Feed :"+ feedName
                                + "Cluster:" + clusterName + "Nominal Time:" + nominalTime + "missed SLALow");
                } else if (schedulableEntityInstance.getTags().contains(FeedSLAMonitoringService.get().TAG_CRITICAL)){
                    store.updateSLAAlertInstance(feedName, clusterName, nominalTime);
                    LOG.info("Feed :"+ feedName
                            + "Cluster:" + clusterName + "Nominal Time:" + nominalTime + "missed SLAHigh");
                    highSLAMissed(feedName, clusterName, nominalTime);
                }
            }
        } catch (FalconException e){
            LOG.error("Exception in FeedSLAALertService:", e);
        }

    }

    @Override
    public void highSLAMissed(String feedName , String clusterName, Date nominalTime) throws FalconException{
        for (EntitySLAListener listener : listeners) {
            listener.highSLAMissed(feedName, clusterName, nominalTime);
        }
        store.deleteFeedAlertInstance(feedName, clusterName, nominalTime);
    }
}
