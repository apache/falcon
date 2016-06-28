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
package org.apache.falcon.jdbc;

import org.apache.commons.collections.CollectionUtils;

import org.apache.falcon.persistence.MonitoredEntityBean;
import org.apache.falcon.persistence.PendingInstanceBean;
import org.apache.falcon.persistence.PersistenceConstants;
import org.apache.falcon.persistence.ResultNotFoundException;
import org.apache.falcon.persistence.EntitySLAAlertBean;
import org.apache.falcon.service.FalconJPAService;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import java.util.Date;
import java.util.List;

/**
* StateStore for MonitoringFeeds and PendingFeedInstances.
*/

public class MonitoringJdbcStateStore {

    private EntityManager getEntityManager() {
        return FalconJPAService.get().getEntityManager();
    }


    public void putMonitoredFeed(String feedName, String entityType){

        MonitoredEntityBean monitoredEntityBean = new MonitoredEntityBean();
        monitoredEntityBean.setFeedName(feedName);
        monitoredEntityBean.setEntityType(entityType);
        EntityManager entityManager = getEntityManager();
        try {
            beginTransaction(entityManager);
            entityManager.persist(monitoredEntityBean);
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public MonitoredEntityBean getMonitoredFeed(String entityName, String entityType){
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_MONITERED_INSTANCE);
        q.setParameter("entityName", entityName);
        q.setParameter("entityType", entityType);
        List result = q.getResultList();
        try {
            if (result.isEmpty()) {
                return null;
            }
        } finally {
            entityManager.close();
        }
        return ((MonitoredEntityBean)result.get(0));
    }

    public void deleteMonitoringFeed(String entityName, String entityType) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_MONITORED_INSTANCES);
        q.setParameter("entityName", entityName);
        q.setParameter("entityType", entityType);
        try{
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public List<MonitoredEntityBean> getAllMonitoredFeed() throws ResultNotFoundException {
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_ALL_MONITORING_FEEDS);
        List result = q.getResultList();
        entityManager.close();
        return result;
    }

    public Date getLastInstanceTime(String feedName , String entityType) throws ResultNotFoundException {
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_LATEST_INSTANCE_TIME, Date.class);
        q.setParameter("entityName", feedName);
        q.setParameter("entityType", entityType);
        Date result = (Date)q.getSingleResult();
        entityManager.close();
        return result;
    }

    public void deletePendingInstance(String feedName, String clusterName , Date nominalTime, String entityType){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_PENDING_NOMINAL_INSTANCES);
        q.setParameter("entityName", feedName);
        q.setParameter("clusterName", clusterName);
        q.setParameter("nominalTime", nominalTime);
        q.setParameter("entityType", entityType);
        try{
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void deletePendingInstances(String feedName, String clusterName, String entityType){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_ALL_INSTANCES_FOR_FEED);
        q.setParameter("entityName", feedName);
        q.setParameter("clusterName", clusterName);
        q.setParameter("entityType", entityType);
        try{
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void putPendingInstances(String feed, String clusterName, Date nominalTime, String entityType){
        EntityManager entityManager = getEntityManager();
        PendingInstanceBean pendingInstanceBean = new PendingInstanceBean();
        pendingInstanceBean.setEntityName(feed);
        pendingInstanceBean.setClusterName(clusterName);
        pendingInstanceBean.setNominalTime(nominalTime);
        pendingInstanceBean.setEntityType(entityType);

        beginTransaction(entityManager);
        entityManager.persist(pendingInstanceBean);
        commitAndCloseTransaction(entityManager);
    }

    public List<Date> getNominalInstances(String feedName, String clusterName, String entityType) {
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_DATE_FOR_PENDING_INSTANCES);
        q.setParameter("entityName", feedName);
        q.setParameter("clusterName", clusterName);
        q.setParameter("entityType", entityType);
        List result = q.getResultList();
        entityManager.close();
        return result;
    }

    public List<PendingInstanceBean> getAllInstances(){
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_ALL_PENDING_INSTANCES);
        List result = q.getResultList();

        try {
            if (CollectionUtils.isEmpty(result)) {
                return null;
            }
        } finally{
            entityManager.close();
        }
        return result;
    }

    private void commitAndCloseTransaction(EntityManager entityManager) {
        entityManager.getTransaction().commit();
        entityManager.close();
    }

    public PendingInstanceBean getPendingInstance(String feedName, String clusterName, Date nominalTime,
                                                  String entityType) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        TypedQuery<PendingInstanceBean> q = entityManager.createNamedQuery(PersistenceConstants.GET_PENDING_INSTANCE,
                            PendingInstanceBean.class);
        q.setParameter("feedName", feedName);

        q.setParameter("clusterName", clusterName);
        q.setParameter("nominalTime", nominalTime);
        q.setParameter("entityType", entityType);
        try {
            return q.getSingleResult();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public EntitySLAAlertBean getFeedAlertInstance(String feedName, String clusterName, Date nominalTime,
                                                   String entityType) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        TypedQuery<EntitySLAAlertBean> q = entityManager.createNamedQuery(PersistenceConstants.GET_FEED_ALERT_INSTANCE,
                EntitySLAAlertBean.class);
        q.setParameter("entityName", feedName);
        q.setParameter("clusterName", clusterName);
        q.setParameter("nominalTime", nominalTime);
        q.setParameter("entityType", entityType);
        try {
            return q.getSingleResult();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void putSLAAlertInstance(String feedName, String cluster, String entityType, Date nominalTime,
                                    Boolean isSLALowMissed, Boolean isSLAHighMissed) {
        EntityManager entityManager = getEntityManager();
        EntitySLAAlertBean entitySLAAlertBean = new EntitySLAAlertBean();
        entitySLAAlertBean.setEntityName(feedName);
        entitySLAAlertBean.setClusterName(cluster);
        entitySLAAlertBean.setNominalTime(nominalTime);
        entitySLAAlertBean.setIsSLALowMissed(isSLALowMissed);
        entitySLAAlertBean.setIsSLAHighMissed(isSLAHighMissed);
        entitySLAAlertBean.setEntityType(entityType);
        try {
            beginTransaction(entityManager);
            entityManager.persist(entitySLAAlertBean);
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void updateSLAAlertInstance(String feedName, String clusterName, Date nominalTime) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.UPDATE_SLA_HIGH);
        q.setParameter("entityName", feedName);
        q.setParameter("clusterName", clusterName);
        q.setParameter("nominalTime", nominalTime);
        try{
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void deleteFeedAlertInstance(String feedName, String clusterName, Date nominalTime, String entityType){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_FEED_ALERT_INSTANCE);
        q.setParameter("feedName", feedName);
        q.setParameter("clusterName", clusterName);
        q.setParameter("nominalTime", nominalTime);
        q.setParameter("entityType", entityType);
        try{
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }


    public List<EntitySLAAlertBean> getSLAHighCandidates() {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_SLA_HIGH_CANDIDATES);
        List result = q.getResultList();

        try {
            if (CollectionUtils.isEmpty(result)) {
                return null;
            }
        } finally{
            entityManager.close();
        }
        return result;
    }


    private void beginTransaction(EntityManager entityManager) {
        entityManager.getTransaction().begin();
    }

}
