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

import org.apache.falcon.FalconException;
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
* StateStore for MonitoringEntity and PendingEntityInstances.
*/

public class MonitoringJdbcStateStore {

    private EntityManager getEntityManager() {
        return FalconJPAService.get().getEntityManager();
    }


    public void putMonitoredEntity(String entityName, String entityType, Date lastMonitoredTime) throws FalconException{

        MonitoredEntityBean monitoredEntityBean = new MonitoredEntityBean();
        monitoredEntityBean.setEntityName(entityName);
        monitoredEntityBean.setEntityType(entityType);
        monitoredEntityBean.setLastMonitoredTime(lastMonitoredTime);
        EntityManager entityManager = getEntityManager();
        try {
            beginTransaction(entityManager);
            entityManager.persist(monitoredEntityBean);
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void updateLastMonitoredTime(String entityName, String entityType, Date lastCheckedTime) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.UPDATE_LAST_MONITORED_TIME);
        q.setParameter(MonitoredEntityBean.ENTITY_NAME, entityName);
        q.setParameter(MonitoredEntityBean.ENTITY_TYPE, entityType.toLowerCase());
        q.setParameter(MonitoredEntityBean.LAST_MONITORED_TIME, lastCheckedTime);
        try{
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public MonitoredEntityBean getMonitoredEntity(String entityName, String entityType){
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_MONITORED_ENTITY);
        q.setParameter(MonitoredEntityBean.ENTITY_NAME, entityName);
        q.setParameter(MonitoredEntityBean.ENTITY_TYPE, entityType.toLowerCase());
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

    public void deleteMonitoringEntity(String entityName, String entityType) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_MONITORED_ENTITIES);
        q.setParameter(MonitoredEntityBean.ENTITY_NAME, entityName);
        q.setParameter(MonitoredEntityBean.ENTITY_TYPE, entityType.toLowerCase());
        try{
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public List<MonitoredEntityBean> getAllMonitoredEntities() throws ResultNotFoundException {
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_ALL_MONITORING_ENTITY);
        List result = q.getResultList();
        entityManager.close();
        return result;
    }

    public List<MonitoredEntityBean> getAllMonitoredEntities(String entityType) throws ResultNotFoundException {
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_ALL_MONITORING_ENTITIES_FOR_TYPE);
        q.setParameter(PendingInstanceBean.ENTITY_TYPE, entityType.toLowerCase());
        List result = q.getResultList();
        entityManager.close();
        return result;
    }

    public Date getLastInstanceTime(String entityName , String entityType) throws ResultNotFoundException {
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_LATEST_INSTANCE_TIME, Date.class);
        q.setParameter(PendingInstanceBean.ENTITY_NAME, entityName);
        q.setParameter(PendingInstanceBean.ENTITY_TYPE, entityType.toLowerCase());
        Date result = (Date)q.getSingleResult();
        entityManager.close();
        return result;
    }

    public void deletePendingInstance(String entityName, String clusterName , Date nominalTime, String entityType){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_PENDING_NOMINAL_INSTANCES);
        q.setParameter(PendingInstanceBean.ENTITY_NAME, entityName);
        q.setParameter(PendingInstanceBean.CLUSTER_NAME, clusterName);
        q.setParameter(PendingInstanceBean.NOMINAL_TIME, nominalTime);
        q.setParameter(PendingInstanceBean.ENTITY_TYPE, entityType.toLowerCase());
        try{
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void deletePendingInstances(String entityName, String clusterName, String entityType){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_ALL_PENDING_INSTANCES_FOR_ENTITY);
        q.setParameter(PendingInstanceBean.ENTITY_NAME, entityName);
        q.setParameter(PendingInstanceBean.CLUSTER_NAME, clusterName);
        q.setParameter(PendingInstanceBean.ENTITY_TYPE, entityType.toLowerCase());
        try{
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void putPendingInstances(String entity, String clusterName, Date nominalTime, String entityType)
        throws FalconException{
        EntityManager entityManager = getEntityManager();
        PendingInstanceBean pendingInstanceBean = new PendingInstanceBean();
        pendingInstanceBean.setEntityName(entity);
        pendingInstanceBean.setClusterName(clusterName);
        pendingInstanceBean.setNominalTime(nominalTime);
        pendingInstanceBean.setEntityType(entityType);

        beginTransaction(entityManager);
        entityManager.persist(pendingInstanceBean);
        commitAndCloseTransaction(entityManager);
    }

    public List<Date> getNominalInstances(String entityName, String clusterName, String entityType) {
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_DATE_FOR_PENDING_INSTANCES);
        q.setParameter(PendingInstanceBean.ENTITY_NAME, entityName);
        q.setParameter(PendingInstanceBean.CLUSTER_NAME, clusterName);
        q.setParameter(PendingInstanceBean.ENTITY_TYPE, entityType.toLowerCase());
        List result = q.getResultList();
        entityManager.close();
        return result;
    }

    public List<PendingInstanceBean> getAllPendingInstances(){
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_ALL_PENDING_INSTANCES);
        List result = q.getResultList();
        entityManager.close();
        return result;
    }

    private void commitAndCloseTransaction(EntityManager entityManager) {
        entityManager.getTransaction().commit();
        entityManager.close();
    }

    public EntitySLAAlertBean getEntityAlertInstance(String entityName, String clusterName, Date nominalTime,
                                                     String entityType) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        TypedQuery<EntitySLAAlertBean> q = entityManager.createNamedQuery(PersistenceConstants.
                GET_ENTITY_ALERT_INSTANCE, EntitySLAAlertBean.class);
        q.setParameter(EntitySLAAlertBean.ENTITY_NAME, entityName);
        q.setParameter(EntitySLAAlertBean.CLUSTER_NAME, clusterName);
        q.setParameter(EntitySLAAlertBean.NOMINAL_TIME, nominalTime);
        q.setParameter(EntitySLAAlertBean.ENTITY_TYPE, entityType.toLowerCase());
        try {
            return q.getSingleResult();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void putSLAAlertInstance(String entityName, String cluster, String entityType, Date nominalTime,
                                    Boolean isSLALowMissed, Boolean isSLAHighMissed) throws FalconException{
        EntityManager entityManager = getEntityManager();
        EntitySLAAlertBean entitySLAAlertBean = new EntitySLAAlertBean();
        entitySLAAlertBean.setEntityName(entityName);
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

    public void updateSLAAlertInstance(String entityName, String clusterName, Date nominalTime, String entityType) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.UPDATE_SLA_HIGH);
        q.setParameter(EntitySLAAlertBean.ENTITY_NAME, entityName);
        q.setParameter(EntitySLAAlertBean.CLUSTER_NAME, clusterName);
        q.setParameter(EntitySLAAlertBean.NOMINAL_TIME, nominalTime);
        q.setParameter(EntitySLAAlertBean.ENTITY_TYPE, entityType.toLowerCase());
        try{
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public void deleteEntityAlertInstance(String entityName, String clusterName, Date nominalTime, String entityType){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_ENTITY_ALERT_INSTANCE);
        q.setParameter(EntitySLAAlertBean.ENTITY_NAME, entityName);
        q.setParameter(EntitySLAAlertBean.CLUSTER_NAME, clusterName);
        q.setParameter(EntitySLAAlertBean.NOMINAL_TIME, nominalTime);
        q.setParameter(EntitySLAAlertBean.ENTITY_TYPE, entityType.toLowerCase());
        try{
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    private void beginTransaction(EntityManager entityManager) {
        entityManager.getTransaction().begin();
    }

}
