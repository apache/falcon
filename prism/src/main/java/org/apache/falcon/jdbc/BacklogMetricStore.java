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
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.persistence.BacklogMetricBean;
import org.apache.falcon.persistence.PersistenceConstants;
import org.apache.falcon.service.BacklogMetricEmitterService;
import org.apache.falcon.service.FalconJPAService;
import org.apache.falcon.util.MetricInfo;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Backlog Metric Store for entitties.
 */
public class BacklogMetricStore {

    private EntityManager getEntityManager() {
        return FalconJPAService.get().getEntityManager();
    }


    public void addInstance(String entityName, String cluster, Date nominalTime, EntityType entityType) {
        BacklogMetricBean backlogMetricBean = new BacklogMetricBean();
        backlogMetricBean.setClusterName(cluster);
        backlogMetricBean.setEntityName(entityName);
        backlogMetricBean.setNominalTime(nominalTime);
        backlogMetricBean.setEntityType(entityType.name());
        EntityManager entityManager = getEntityManager();
        try {
            beginTransaction(entityManager);
            entityManager.persist(backlogMetricBean);
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }

    public synchronized void deleteMetricInstance(String entityName, String cluster, Date nominalTime,
                                                  EntityType entityType) {
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery(PersistenceConstants.DELETE_BACKLOG_METRIC_INSTANCE);
        q.setParameter("entityName", entityName);
        q.setParameter("clusterName", cluster);
        q.setParameter("nominalTime", nominalTime);
        q.setParameter("entityType", entityType.name());
        try{
            q.executeUpdate();
        } finally {
            commitAndCloseTransaction(entityManager);
        }
    }


    private void beginTransaction(EntityManager entityManager) {
        entityManager.getTransaction().begin();
    }

    private void commitAndCloseTransaction(EntityManager entityManager) {
        if (entityManager != null) {
            entityManager.getTransaction().commit();
            entityManager.close();
        }
    }

    public Map<Entity, List<MetricInfo>> getAllInstances() throws FalconException {
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery(PersistenceConstants.GET_ALL_BACKLOG_INSTANCES);
        List<BacklogMetricBean> result = q.getResultList();

        try {
            if (CollectionUtils.isEmpty(result)) {
                return null;
            }
        } finally{
            entityManager.close();
        }

        Map<Entity, List<MetricInfo>> backlogMetrics = new HashMap<>();
        for (BacklogMetricBean backlogMetricBean : result) {
            Entity entity = EntityUtil.getEntity(backlogMetricBean.getEntityType(),
                    backlogMetricBean.getEntityName());
            if (!backlogMetrics.containsKey(entity)) {
                backlogMetrics.put(entity, new ArrayList<MetricInfo>());
            }
            List<MetricInfo> metrics =  backlogMetrics.get(entity);
            MetricInfo metricInfo = new MetricInfo(BacklogMetricEmitterService.DATE_FORMAT.get()
                    .format(backlogMetricBean.getNominalTime()),
                    backlogMetricBean.getClusterName());
            metrics.add(metricInfo);
            backlogMetrics.put(entity, metrics);
        }
        return backlogMetrics;
    }
}
