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

import org.apache.commons.io.IOUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.exception.StateStoreException;
import org.apache.falcon.execution.ExecutionInstance;
import org.apache.falcon.execution.ProcessExecutionInstance;
import org.apache.falcon.predicate.Predicate;
import org.apache.falcon.state.EntityID;
import org.apache.falcon.state.EntityState;
import org.apache.falcon.state.InstanceID;
import org.apache.falcon.state.InstanceState;
import org.joda.time.DateTime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Mapping util for Persistent Store.
 */
public final class BeanMapperUtil {
    private BeanMapperUtil() {
    }

    /**
     * Converts Entity object to EntityBean which will be stored in DB.
     * @param entityState
     * @return
     */
    public static EntityBean convertToEntityBean(EntityState entityState) {
        EntityBean entityBean = new EntityBean();
        Entity entity = entityState.getEntity();
        String id = new EntityID(entity).getKey();
        entityBean.setId(id);
        entityBean.setName(entity.getName());
        entityBean.setState(entityState.getCurrentState().toString());
        entityBean.setType(entity.getEntityType().toString());
        return entityBean;
    }

    /**
     * Converts EntityBean of Data Base to EntityState.
     * @param entityBean
     * @return
     * @throws StateStoreException
     */
    public static EntityState convertToEntityState(EntityBean entityBean) throws StateStoreException {
        try {
            Entity entity = EntityUtil.getEntity(entityBean.getType(), entityBean.getName());
            EntityState entityState = new EntityState(entity);
            entityState.setCurrentState(EntityState.STATE.valueOf(entityBean.getState()));
            return entityState;
        } catch (FalconException e) {
            throw new StateStoreException(e);
        }
    }

    /**
     * Converts list of EntityBeans of Data Base to EntityStates.
     * @param entityBeans
     * @return
     * @throws StateStoreException
     */
    public static Collection<EntityState> convertToEntityState(Collection<EntityBean> entityBeans)
        throws StateStoreException {
        List<EntityState> entityStates = new ArrayList<>();
        if (entityBeans != null && !entityBeans.isEmpty()) {
            for (EntityBean entityBean : entityBeans) {
                entityStates.add(convertToEntityState(entityBean));
            }
        }
        return entityStates;
    }

    /**
     * Converts list of EntityBeans of Data Base to Entities.
     * @param entityBeans
     * @return
     * @throws StateStoreException
     */
    public static Collection<Entity> convertToEntities(Collection<EntityBean> entityBeans) throws StateStoreException {
        List<Entity> entities = new ArrayList<>();
        try {
            if (entityBeans != null && !entityBeans.isEmpty()) {
                for (EntityBean entityBean : entityBeans) {
                    Entity entity = EntityUtil.getEntity(entityBean.getType(), entityBean.getName());
                    entities.add(entity);
                }
            }
            return entities;
        } catch (FalconException e) {
            throw new StateStoreException(e);
        }
    }

    /**
     * Convert instance of Entity's instance to InstanceBean of DB.
     * @param instanceState
     * @return
     * @throws StateStoreException
     * @throws IOException
     */
    public static InstanceBean convertToInstanceBean(InstanceState instanceState) throws StateStoreException,
            IOException {
        InstanceBean instanceBean = new InstanceBean();
        ExecutionInstance instance = instanceState.getInstance();
        if (instance.getActualEnd() != null) {
            instanceBean.setActualEndTime(new Timestamp(instance.getActualEnd().getMillis()));
        }
        if (instance.getActualStart() != null) {
            instanceBean.setActualStartTime(new Timestamp(instance.getActualStart().getMillis()));
        }
        if (instanceState.getCurrentState() != null) {
            instanceBean.setCurrentState(instanceState.getCurrentState().toString());
        }
        if (instance.getExternalID() != null) {
            instanceBean.setExternalID(instanceState.getInstance().getExternalID());
        }

        instanceBean.setCluster(instance.getCluster());
        instanceBean.setCreationTime(new Timestamp(instance.getCreationTime().getMillis()));
        instanceBean.setId(instance.getId().toString());
        instanceBean.setInstanceTime(new Timestamp(instance.getInstanceTime().getMillis()));
        instanceBean.setEntityId(new InstanceID(instance).getEntityID().toString());

        instanceBean.setInstanceSequence(instance.getInstanceSequence());
        if (instance.getAwaitingPredicates() != null && !instance.getAwaitingPredicates().isEmpty()) {
            ObjectOutputStream out = null;
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            try {
                out = new ObjectOutputStream(byteArrayOutputStream);
                out.writeInt(instance.getAwaitingPredicates().size());
                for (Predicate predicate : instance.getAwaitingPredicates()) {
                    out.writeObject(predicate);
                }
                instanceBean.setAwaitedPredicates(byteArrayOutputStream.toByteArray());
            } finally {
                IOUtils.closeQuietly(out);
            }
        }
        return instanceBean;
    }

    /**
     * Converts instance entry of DB to instance of ExecutionInstance.
     * @param instanceBean
     * @return
     * @throws StateStoreException
     * @throws IOException
     */
    public static InstanceState convertToInstanceState(InstanceBean instanceBean) throws StateStoreException,
            IOException {
        EntityType entityType = InstanceID.getEntityType(instanceBean.getId());
        ExecutionInstance executionInstance = getExecutionInstance(entityType, instanceBean);
        if (instanceBean.getActualEndTime() != null) {
            executionInstance.setActualEnd(new DateTime(instanceBean.getActualEndTime().getTime()));
        }
        if (instanceBean.getActualStartTime() != null) {
            executionInstance.setActualStart(new DateTime(instanceBean.getActualStartTime().getTime()));
        }
        executionInstance.setExternalID(instanceBean.getExternalID());
        executionInstance.setInstanceSequence(instanceBean.getInstanceSequence());

        byte[] result = instanceBean.getAwaitedPredicates();
        List<Predicate> predicates = new ArrayList<>();
        if (result != null && result.length != 0) {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(result);
            ObjectInputStream in = null;
            try {
                in = new ObjectInputStream(byteArrayInputStream);
                int length = in.readInt();
                for (int i = 0; i < length; i++) {
                    Predicate predicate = (Predicate) in.readObject();
                    predicates.add(predicate);
                }
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            } finally {
                IOUtils.closeQuietly(in);
            }
        }
        executionInstance.setAwaitingPredicates(predicates);
        InstanceState instanceState = new InstanceState(executionInstance);
        instanceState.setCurrentState(InstanceState.STATE.valueOf(instanceBean.getCurrentState()));
        return instanceState;
    }

    /**
     * Converting list of instance entries of DB to instance of ExecutionInstance.
     * @param instanceBeanList
     * @return
     * @throws StateStoreException
     * @throws IOException
     */
    public static Collection<InstanceState> convertToInstanceState(List<InstanceBean> instanceBeanList)
        throws StateStoreException, IOException {
        List<InstanceState> instanceStates = new ArrayList<>();
        for (InstanceBean instanceBean : instanceBeanList) {
            instanceStates.add(convertToInstanceState(instanceBean));
        }
        return instanceStates;
    }

    private static ExecutionInstance getExecutionInstance(EntityType entityType,
                                                          InstanceBean instanceBean) throws StateStoreException {
        try {
            Entity entity = EntityUtil.getEntity(entityType, InstanceID.getEntityName(instanceBean.getId()));
            return getExecutionInstance(entityType, entity, instanceBean.getInstanceTime().getTime(),
                    instanceBean.getCluster(), instanceBean.getCreationTime().getTime());
        } catch (FalconException e) {
            throw new StateStoreException(e);
        }
    }

    public static ExecutionInstance getExecutionInstance(EntityType entityType, Entity entity, long instanceTime,
                                                         String cluster, long creationTime) throws StateStoreException {
        if (entityType == EntityType.PROCESS) {
            try {
                return new ProcessExecutionInstance((org.apache.falcon.entity.v0.process.Process) entity,
                        new DateTime(instanceTime), cluster, new DateTime(creationTime));
            } catch (FalconException e) {
                throw new StateStoreException("Entity not found");
            }
        } else {
            throw new UnsupportedOperationException("Not supported for entity type " + entityType.toString());
        }
    }


    public static byte[] getAwaitedPredicates(InstanceState instanceState) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream out = null;
        try {
            out = new ObjectOutputStream(byteArrayOutputStream);
            out.writeInt(instanceState.getInstance().getAwaitingPredicates().size());
            for (Predicate predicate : instanceState.getInstance().getAwaitingPredicates()) {
                out.writeObject(predicate);
            }
            return byteArrayOutputStream.toByteArray();
        } finally {
            IOUtils.closeQuietly(out);
        }
    }
}
