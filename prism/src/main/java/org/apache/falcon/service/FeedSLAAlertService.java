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

import javax.persistence.EntityManager;

import com.google.common.collect.Multimap;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.*;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.*;
import org.apache.falcon.expression.ExpressionHelper;
import org.apache.falcon.jdbc.MonitoringJdbcStateStore;
import org.apache.falcon.persistence.FeedSLAAlertBean;
import org.apache.falcon.persistence.PendingInstanceBean;
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
            for(Map.Entry<Pair, Date> pairDateEntry : feedInstancesToBeMonitored.entries()) {

                Pair pair = pairDateEntry.getKey();
                Feed feed = ConfigurationStore.get().get(EntityType.FEED, (String) pair.getLeft());

                Cluster cluster =  FeedHelper.getCluster(feed,(String)pair.getRight());


                // Get the status of the feed instance from HDFS if it is available look for the time stamp wheather it missed the sla
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

                    List<FeedInstanceStatus> feedStatus = FeedHelper.
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
}