package org.apache.falcon.jdbc;

import org.apache.commons.collections.CollectionUtils;
import org.apache.falcon.persistence.EntityBean;
import org.apache.falcon.persistence.MonitoredFeedsBean;
import org.apache.falcon.persistence.PendingInstanceBean;
import org.apache.falcon.persistence.PersistenceConstants;
import org.apache.falcon.service.FalconJPAService;
import org.apache.openjpa.persistence.OpenJPAEntityManager;
import org.apache.openjpa.persistence.OpenJPAPersistence;

import javax.persistence.*;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Created by praveen on 8/3/16.
 */
public class MonitoringJdbcStateStore {

    private EntityManager getEntityManager() {
        return FalconJPAService.get().getEntityManager();
    }


    public void putMonitoredFeed (String feedName){
        MonitoredFeedsBean monitoredFeedsBean = new MonitoredFeedsBean();
        monitoredFeedsBean.setFeedName(feedName);

        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        entityManager.persist(monitoredFeedsBean);
        commitAndCloseTransaction(entityManager);
    }

    public MonitoredFeedsBean getMonitoredFeed(String feedName){
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_MONITERED_INSTANCE);
        q.setParameter("feedName", feedName);
        List result = q.getResultList();
        if (result.isEmpty()) {
            return null;
        }
        entityManager.close();
        return ((MonitoredFeedsBean)result.get(0));
    }

    public void deleteMonitoringFeed (String feedName) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_MONITORED_INSTANCES);
        q.setParameter("feedName", feedName);
        q.executeUpdate();
        commitAndCloseTransaction(entityManager);
    }

    public List<MonitoredFeedsBean> getAllMonitoredFeed(){
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_ALL_MONITORING_FEEDS);
        List result = q.getResultList();
        if (result.isEmpty()) {
            return null;
        }
        entityManager.close();
        return result;
    }

    public void deletePendingNominalInstances (String feedName, String clusterName ,Date nominalTime){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_PENDING_NOMINAL_INSTANCES);
        q.setParameter("feedName", feedName);
        q.setParameter("clusterName", clusterName);
        q.setParameter("nominalTime",nominalTime);
        q.executeUpdate();
        commitAndCloseTransaction(entityManager);
    }

    public void deletePendingInstances (String feedName, String clusterName ){
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_ALL_PENDING_INSTANCES);
        q.setParameter("feedName", feedName);
        q.setParameter("clusterName", clusterName);
        q.executeUpdate();
        commitAndCloseTransaction(entityManager);
    }

    public void putPendingInstances (String feed,String clusterName ,Date nominalTime){
        PendingInstanceBean pendingInstanceBean = new PendingInstanceBean();
        pendingInstanceBean.setFeedName(feed);
        pendingInstanceBean.setClusterName(clusterName);
        pendingInstanceBean.setNominalTime(nominalTime);

        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        entityManager.persist(pendingInstanceBean);
        commitAndCloseTransaction(entityManager);
    }

    public List<Date> getNominalInstances(String feedName,String clusterName){
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_DATE_FOR_PENDING_INSTANCES);
        q.setParameter("feedName", feedName);
        q.setParameter("clusterName", clusterName);
        List result = q.getResultList();
        if (CollectionUtils.isEmpty(result)) {
            return null;
        }
        entityManager.close();
        return result;
    }
    public List<PendingInstanceBean> getAllInstances(){
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_ALL_PENDING_INSTANCES);
        List result = q.getResultList();
        if (CollectionUtils.isEmpty(result)) {
            return null;
        }
        entityManager.close();
        return result;


    }

    private void commitAndCloseTransaction(EntityManager entityManager) {
        entityManager.getTransaction().commit();
        entityManager.close();
    }

    private void beginTransaction(EntityManager entityManager) {
        entityManager.getTransaction().begin();
    }

}
