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
package org.apache.falcon.state.store.jdbc;

import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.exception.StateStoreException;
import org.apache.falcon.execution.ExecutionInstance;
import org.apache.falcon.state.EntityClusterID;
import org.apache.falcon.state.EntityID;
import org.apache.falcon.state.EntityState;
import org.apache.falcon.state.ID;
import org.apache.falcon.state.InstanceID;
import org.apache.falcon.state.InstanceState;
import org.apache.falcon.state.store.AbstractStateStore;
import org.apache.falcon.state.store.StateStore;
import org.apache.falcon.state.store.service.FalconJPAService;
import org.apache.falcon.util.StartupProperties;
import org.joda.time.DateTime;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Persistent Data Store for Entities and Instances.
 */
public final class JDBCStateStore extends AbstractStateStore {

    private static final StateStore STORE = new JDBCStateStore();
    private static final String DEBUG = "debug";

    private JDBCStateStore() {}

    public static StateStore get() {
        return STORE;
    }

    @Override
    public void clear() throws StateStoreException {
        if (!isModeDebug()) {
            throw new UnsupportedOperationException("Clear Method not supported");
        }
        deleteExecutionInstances();
        deleteEntities();
    }

    @Override
    public void putEntity(EntityState entityState) throws StateStoreException {
        EntityID entityID = new EntityID(entityState.getEntity());
        String key = entityID.getKey();
        if (entityExists(entityID)) {
            throw new StateStoreException("Entity with key, " + key + " already exists.");
        }
        EntityBean entityBean = BeanMapperUtil.convertToEntityBean(entityState);
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        entityManager.persist(entityBean);
        commitAndCloseTransaction(entityManager);
    }


    @Override
    public EntityState getEntity(EntityID entityID) throws StateStoreException {
        EntityState entityState = getEntityByKey(entityID);
        if (entityState == null) {
            throw new StateStoreException("Entity with key, " + entityID + " does not exist.");
        }
        return entityState;
    }

    private EntityState getEntityByKey(EntityID id) throws StateStoreException {
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery("GET_ENTITY");
        q.setParameter("id", id.getKey());
        List result = q.getResultList();
        if (result.isEmpty()) {
            return null;
        }
        entityManager.close();
        return BeanMapperUtil.convertToEntityState((EntityBean) result.get(0));
    }

    @Override
    public boolean entityExists(EntityID entityID) throws StateStoreException {
        return getEntityByKey(entityID) == null ? false : true;
    }

    @Override
    public Collection<Entity> getEntities(EntityState.STATE state) throws StateStoreException {
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery("GET_ENTITY_FOR_STATE");
        q.setParameter("state", state.toString());
        List result = q.getResultList();
        entityManager.close();
        return BeanMapperUtil.convertToEntities(result);
    }

    @Override
    public Collection<EntityState> getAllEntities() throws StateStoreException {
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery("GET_ENTITIES");
        List result = q.getResultList();
        entityManager.close();
        return BeanMapperUtil.convertToEntityState(result);
    }

    @Override
    public void updateEntity(EntityState entityState) throws StateStoreException {
        EntityID entityID = new EntityID(entityState.getEntity());
        if (!entityExists(entityID)) {
            throw new StateStoreException("Entity with key, " + entityID + " doesn't exists.");
        }
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery("UPDATE_ENTITY");
        q.setParameter("id", entityID.getKey());
        if (entityState.getCurrentState() != null) {
            q.setParameter("state", entityState.getCurrentState().toString());
        }
        q.setParameter("type", entityState.getEntity().getEntityType().toString());
        q.setParameter("name", entityState.getEntity().getName());
        q.executeUpdate();
        commitAndCloseTransaction(entityManager);
    }

    @Override
    public void deleteEntity(EntityID entityID) throws StateStoreException {
        if (!entityExists(entityID)) {
            throw new StateStoreException("Entity with key, " + entityID.getKey() + " does not exist.");
        }
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery("DELETE_ENTITY");
        q.setParameter("id", entityID.getKey());
        q.executeUpdate();
        commitAndCloseTransaction(entityManager);
    }

    @Override
    public void deleteEntities() throws StateStoreException {
        if (!isModeDebug()) {
            throw new UnsupportedOperationException("Delete Entities Table not supported");
        }
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery("DELETE_ENTITIES");
        q.executeUpdate();
        commitAndCloseTransaction(entityManager);
    }

    @Override
    public boolean isEntityCompleted(EntityID entityId) {
        // ToDo need to implement this, currently returning false.
        return false;
    }

    @Override
    public void putExecutionInstance(InstanceState instanceState) throws StateStoreException {
        InstanceID instanceID = new InstanceID(instanceState.getInstance());
        if (executionInstanceExists(instanceID)) {
            throw new StateStoreException("Instance with key, " + instanceID + " already exists.");
        }
        try {
            InstanceBean instanceBean = BeanMapperUtil.convertToInstanceBean(instanceState);
            EntityManager entityManager = getEntityManager();
            beginTransaction(entityManager);
            entityManager.persist(instanceBean);
            commitAndCloseTransaction(entityManager);
        } catch (IOException e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public InstanceState getExecutionInstance(InstanceID instanceId) throws StateStoreException {
        InstanceState instanceState = getExecutionInstanceByKey(instanceId);
        if (instanceState == null) {
            throw new StateStoreException("Instance with key, " + instanceId.toString() + " does not exist.");
        }
        return instanceState;
    }

    private InstanceState getExecutionInstanceByKey(ID instanceKey) throws StateStoreException {
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery("GET_INSTANCE");
        q.setParameter("id", instanceKey.toString());
        List result = q.getResultList();
        entityManager.close();
        if (result.isEmpty()) {
            return null;
        }
        try {
            InstanceBean instanceBean = (InstanceBean)(result.get(0));
            return BeanMapperUtil.convertToInstanceState(instanceBean);
        } catch (IOException e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public void updateExecutionInstance(InstanceState instanceState) throws StateStoreException {
        InstanceID id = new InstanceID(instanceState.getInstance());
        String key = id.toString();
        if (!executionInstanceExists(id)) {
            throw new StateStoreException("Instance with key, " + key + " does not exist.");
        }
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery("UPDATE_INSTANCE");
        ExecutionInstance instance = instanceState.getInstance();
        q.setParameter("id", key);
        q.setParameter("cluster", instance.getCluster());
        q.setParameter("externalID", instance.getExternalID());
        q.setParameter("instanceTime", new Timestamp(instance.getInstanceTime().getMillis()));
        q.setParameter("creationTime", new Timestamp(instance.getCreationTime().getMillis()));
        if (instance.getActualEnd() != null) {
            q.setParameter("actualEndTime", new Timestamp(instance.getActualEnd().getMillis()));
        }
        q.setParameter("currentState", instanceState.getCurrentState().toString());
        if (instance.getActualStart() != null) {
            q.setParameter("actualStartTime", new Timestamp(instance.getActualStart().getMillis()));
        }
        q.setParameter("instanceSequence", instance.getInstanceSequence());
        if (instanceState.getInstance().getAwaitingPredicates() != null
                && !instanceState.getInstance().getAwaitingPredicates().isEmpty()) {
            try {
                q.setParameter("awaitedPredicates", BeanMapperUtil.getAwaitedPredicates(instanceState));
            } catch (IOException e) {
                throw new StateStoreException(e);
            }
        }
        q.executeUpdate();
        commitAndCloseTransaction(entityManager);
    }

    @Override
    public Collection<InstanceState> getAllExecutionInstances(Entity entity, String cluster)
        throws StateStoreException {
        EntityClusterID id = new EntityClusterID(entity, cluster);
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery("GET_INSTANCES_FOR_ENTITY_CLUSTER");
        q.setParameter("entityId", id.getEntityID().getKey());
        q.setParameter("cluster", cluster);
        List result  = q.getResultList();
        entityManager.close();
        try {
            return BeanMapperUtil.convertToInstanceState(result);
        } catch (IOException e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public Collection<InstanceState> getExecutionInstances(Entity entity, String cluster,
                                                           Collection<InstanceState.STATE> states)
        throws StateStoreException {
        EntityClusterID entityClusterID = new EntityClusterID(entity, cluster);
        String entityKey = entityClusterID.getEntityID().getKey();
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery("GET_INSTANCES_FOR_ENTITY_CLUSTER_FOR_STATES");
        q.setParameter("entityId", entityKey);
        q.setParameter("cluster", cluster);
        List<String> instanceStates = new ArrayList<>();
        for (InstanceState.STATE state : states) {
            instanceStates.add(state.toString());
        }
        q.setParameter("currentState", instanceStates);
        List result  = q.getResultList();
        entityManager.close();
        try {
            return BeanMapperUtil.convertToInstanceState(result);
        } catch (IOException e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public Collection<InstanceState> getExecutionInstances(EntityClusterID id,
                                                           Collection<InstanceState.STATE> states)
        throws StateStoreException {
        String entityKey = id.getEntityID().getKey();
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery("GET_INSTANCES_FOR_ENTITY_FOR_STATES");
        q.setParameter("entityId", entityKey);
        List<String> instanceStates = new ArrayList<>();
        for (InstanceState.STATE state : states) {
            instanceStates.add(state.toString());
        }
        q.setParameter("currentState", instanceStates);
        List result  = q.getResultList();
        entityManager.close();
        try {
            return BeanMapperUtil.convertToInstanceState(result);
        } catch (IOException e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public Collection<InstanceState> getExecutionInstances(Entity entity, String cluster,
                                                           Collection<InstanceState.STATE> states, DateTime start,
                                                           DateTime end) throws StateStoreException {
        String entityKey = new EntityClusterID(entity, cluster).getEntityID().getKey();
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery("GET_INSTANCES_FOR_ENTITY_FOR_STATES_WITH_RANGE");
        q.setParameter("entityId", entityKey);
        List<String> instanceStates = new ArrayList<>();
        for (InstanceState.STATE state : states) {
            instanceStates.add(state.toString());
        }
        q.setParameter("currentState", instanceStates);
        q.setParameter("startTime", new Timestamp(start.getMillis()));
        q.setParameter("endTime", new Timestamp(end.getMillis()));
        List result  = q.getResultList();
        entityManager.close();
        try {
            return BeanMapperUtil.convertToInstanceState(result);
        } catch (IOException e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public InstanceState getLastExecutionInstance(Entity entity, String cluster) throws StateStoreException {
        String key = new EntityClusterID(entity, cluster).getEntityID().getKey();
        EntityManager entityManager = getEntityManager();
        Query q = entityManager.createNamedQuery("GET_LAST_INSTANCE_FOR_ENTITY_CLUSTER");
        q.setParameter("entityId", key);
        q.setParameter("cluster", cluster);
        q.setMaxResults(1);
        List result = q.getResultList();
        entityManager.close();
        if (!result.isEmpty()) {
            try {
                return BeanMapperUtil.convertToInstanceState((InstanceBean) result.get(0));
            } catch (IOException e) {
                throw new StateStoreException(e);
            }
        }
        return null;
    }

    @Override
    public boolean executionInstanceExists(InstanceID instanceKey) throws StateStoreException {
        return getExecutionInstanceByKey(instanceKey) == null ? false : true;
    }

    @Override
    public void deleteExecutionInstance(InstanceID instanceID) throws StateStoreException {
        String instanceKey = instanceID.toString();
        if (!executionInstanceExists(instanceID)) {
            throw new StateStoreException("Instance with key, " + instanceKey + " does not exist.");
        }
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery("DELETE_INSTANCE");
        q.setParameter("id", instanceKey);
        q.executeUpdate();
        commitAndCloseTransaction(entityManager);
    }

    @Override
    public void deleteExecutionInstances(EntityID entityID) {
        String entityKey = entityID.getKey();
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery("DELETE_INSTANCE_FOR_ENTITY");
        q.setParameter("entityId", entityKey);
        q.executeUpdate();
        commitAndCloseTransaction(entityManager);
    }

    @Override
    public void deleteExecutionInstances() {
        if (!isModeDebug()) {
            throw new UnsupportedOperationException("Delete Instances Table not supported");
        }
        EntityManager entityManager = getEntityManager();
        beginTransaction(entityManager);
        Query q = entityManager.createNamedQuery("DELETE_INSTANCES_TABLE");
        q.executeUpdate();
        commitAndCloseTransaction(entityManager);
    }

    // Debug enabled for test cases
    private boolean isModeDebug() {
        return DEBUG.equals(StartupProperties.get().getProperty("domain")) ? true : false;
    }

    private void commitAndCloseTransaction(EntityManager entityManager) {
        entityManager.getTransaction().commit();
        entityManager.close();
    }

    private void beginTransaction(EntityManager entityManager) {
        entityManager.getTransaction().begin();
    }

    private EntityManager getEntityManager() {
        return FalconJPAService.get().getEntityManager();
    }

}
