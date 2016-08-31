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
import org.apache.falcon.entity.ClusterHelper;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.jdbc.MonitoringJdbcStateStore;
import org.apache.falcon.persistence.PendingInstanceBean;
import org.apache.falcon.resource.SchedulableEntityInstance;
import org.apache.falcon.util.ReflectionUtils;
import org.apache.falcon.util.StartupProperties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  * Service to know which all entities have missed SLA.
  */
public final class EntitySLAAlertService implements FalconService, EntitySLAListener {

    private static final String NAME = "EntitySLAAlertService";

    private static final Logger LOG = LoggerFactory.getLogger(EntitySLAAlertService.class);

    private MonitoringJdbcStateStore store = new MonitoringJdbcStateStore();

    private Set<EntitySLAListener> listeners = new LinkedHashSet<EntitySLAListener>();

    private static final EntitySLAAlertService SERVICE = new EntitySLAAlertService();

    public static EntitySLAAlertService get() {
        return SERVICE;
    }

    private EntitySLAAlertService(){}


    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void init() throws FalconException {
        String listenerClassNames = StartupProperties.get().
                getProperty("entityAlert.listeners");
        if (listenerClassNames != null && !listenerClassNames.isEmpty()) {
            for (String listenerClassName : listenerClassNames.split(",")) {
                listenerClassName = listenerClassName.trim();
                if (listenerClassName.isEmpty()) {
                    continue;
                }
                EntitySLAListener listener = ReflectionUtils.getInstanceByClassName(listenerClassName);
                registerListener(listener);
            }
        }

        String freq = StartupProperties.get().getProperty("entity.sla.statusCheck.frequency.seconds", "600");
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
        //Get all entity instances to be monitored
        List<PendingInstanceBean> pendingInstanceBeanList = store.getAllPendingInstances();
        if (pendingInstanceBeanList == null || pendingInstanceBeanList.isEmpty()){
            return;
        }

        LOG.debug("In processSLACandidates :" + pendingInstanceBeanList.size());
        try{
            for (PendingInstanceBean pendingInstanceBean : pendingInstanceBeanList) {

                String entityName = pendingInstanceBean.getEntityName();
                String clusterName = pendingInstanceBean.getClusterName();
                Date nominalTime = pendingInstanceBean.getNominalTime();
                String entityType = pendingInstanceBean.getEntityType();

                org.apache.falcon.entity.v0.cluster.Cluster cluster = ClusterHelper.getCluster(clusterName);

                Set<SchedulableEntityInstance> schedulableEntityInstances= EntitySLAMonitoringService.get().
                        getEntitySLAMissPendingAlerts(entityName, cluster.getName(), nominalTime, nominalTime
                                , entityType);
                if (schedulableEntityInstances.isEmpty()){
                    store.deleteEntityAlertInstance(entityName, cluster.getName(), nominalTime,
                            entityType);
                    return;
                }
                List<SchedulableEntityInstance> schedulableEntityList = new ArrayList<>(schedulableEntityInstances);
                SchedulableEntityInstance schedulableEntityInstance = schedulableEntityList.get(0);


                if (schedulableEntityInstance.getTags().contains(EntitySLAMonitoringService.get().TAG_WARN)) {
                    store.putSLAAlertInstance(entityName, clusterName, entityType,
                            nominalTime, true, false);
                    //Mark in DB as SLA missed
                    LOG.info("Entity : {} Cluster : {} Nominal Time : {} missed SLALow", entityName, entityType,
                            clusterName, nominalTime);
                } else if (schedulableEntityInstance.getTags().contains(EntitySLAMonitoringService.get().TAG_CRITICAL)){
                    if (entityType.equals(EntityType.PROCESS.name())){
                        store.putSLAAlertInstance(entityName, clusterName, entityType,
                                nominalTime, true, false);
                    }
                    store.updateSLAAlertInstance(entityName, clusterName, nominalTime, entityType);
                    LOG.info("Entity :{} EntityType : {} Cluster: {} Nominal Time: {} missed SLAHigh", entityName,
                            entityType , clusterName , nominalTime);
                    highSLAMissed(entityName, clusterName, EntityType.valueOf(entityType), nominalTime);
                }
            }
        } catch (FalconException e){
            LOG.error("Exception in EntitySLAALertService:", e);
        }

    }

    @Override
    public void highSLAMissed(String entityName, String clusterName, EntityType entityType , Date nominalTime
                              ) throws FalconException {
        LOG.debug("Listners called...");
        for (EntitySLAListener listener : listeners) {
            listener.highSLAMissed(entityName, clusterName, entityType, nominalTime);
            store.deleteEntityAlertInstance(entityName, clusterName, nominalTime, entityType.name());
        }
    }
}
