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
package org.apache.falcon.state.store;

import com.google.common.collect.Lists;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.exception.StateStoreException;
import org.apache.falcon.execution.ExecutionInstance;
import org.apache.falcon.state.EntityState;
import org.apache.falcon.state.ID;
import org.apache.falcon.state.InstanceState;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * An in memory state store mostly intended for unit tests.
 * Singleton.
 */
public final class InMemoryStateStore extends AbstractStateStore {

    private Map<String, EntityState> entityStates = new HashMap<>();
    // Keep it sorted
    private SortedMap<String, InstanceState> instanceStates = Collections
            .synchronizedSortedMap(new TreeMap<String, InstanceState>());

    private static final StateStore STORE = new InMemoryStateStore();

    private InMemoryStateStore() {
    }

    public static StateStore get() {
        return STORE;
    }

    @Override
    public void putEntity(EntityState entityState) throws StateStoreException {
        String key = new ID(entityState.getEntity()).getEntityKey();
        if (entityStates.containsKey(key)) {
            throw new StateStoreException("Entity with key, " + key + " already exists.");
        }
        entityStates.put(key, entityState);
    }

    @Override
    public EntityState getEntity(ID entityId) throws StateStoreException {
        if (!entityStates.containsKey(entityId.getEntityKey())) {
            throw new StateStoreException("Entity with key, " + entityId + " does not exist.");
        }
        return entityStates.get(entityId.getEntityKey());
    }

    @Override
    public boolean entityExists(ID entityId) {
        return entityStates.containsKey(entityId.getEntityKey());
    }

    @Override
    public Collection<Entity> getEntities(EntityState.STATE state) {
        Collection<Entity> entities = new ArrayList<>();
        for (EntityState entityState : entityStates.values()) {
            if (entityState.getCurrentState().equals(state)) {
                entities.add(entityState.getEntity());
            }
        }
        return entities;
    }

    @Override
    public Collection<EntityState> getAllEntities() {
        return entityStates.values();
    }

    @Override
    public void updateEntity(EntityState entityState) throws StateStoreException {
        String key = new ID(entityState.getEntity()).getEntityKey();
        if (!entityStates.containsKey(key)) {
            throw new StateStoreException("Entity with key, " + key + " does not exist.");
        }
        entityStates.put(key, entityState);
    }

    @Override
    public void deleteEntity(ID entityId) throws StateStoreException {
        if (!entityStates.containsKey(entityId.getEntityKey())) {
            throw new StateStoreException("Entity with key, " + entityId + " does not exist.");
        }
        deleteExecutionInstances(entityId);
        entityStates.remove(entityId.getEntityKey());
    }

    @Override
    public void putExecutionInstance(InstanceState instanceState) throws StateStoreException {
        String key = new ID(instanceState.getInstance()).toString();
        if (instanceStates.containsKey(key)) {
            throw new StateStoreException("Instance with key, " + key + " already exists.");
        }
        instanceStates.put(key, instanceState);
    }

    @Override
    public InstanceState getExecutionInstance(ID instanceId) throws StateStoreException {
        if (!instanceStates.containsKey(instanceId.toString())) {
            throw new StateStoreException("Instance with key, " + instanceId + " does not exist.");
        }
        return instanceStates.get(instanceId.toString());
    }

    @Override
    public void updateExecutionInstance(InstanceState instanceState) throws StateStoreException {
        String key = new ID(instanceState.getInstance()).toString();
        if (!instanceStates.containsKey(key)) {
            throw new StateStoreException("Instance with key, " + key + " does not exist.");
        }
        instanceStates.put(key, instanceState);
    }

    @Override
    public Collection<InstanceState> getAllExecutionInstances(Entity entity, String cluster)
        throws StateStoreException {
        ID id = new ID(entity, cluster);
        if (!entityStates.containsKey(id.getEntityKey())) {
            throw new StateStoreException("Entity with key, " + id.getEntityKey() + " does not exist.");
        }
        Collection<InstanceState> instances = new ArrayList<InstanceState>();
        for (Map.Entry<String, InstanceState> instanceState : instanceStates.entrySet()) {
            if (instanceState.getKey().startsWith(id.toString())) {
                instances.add(instanceState.getValue());
            }
        }
        return instances;
    }

    @Override
    public Collection<InstanceState> getExecutionInstances(Entity entity, String cluster,
            Collection<InstanceState.STATE> states) throws StateStoreException {
        ID id = new ID(entity, cluster);
        return getExecutionInstances(id, states);
    }

    @Override
    public Collection<InstanceState> getExecutionInstances(Entity entity, String cluster,
            Collection<InstanceState.STATE> states, DateTime start, DateTime end) throws StateStoreException {
        List<InstanceState> instancesToReturn = new ArrayList<>();
        ID id = new ID(entity, cluster);
        for (InstanceState state : getExecutionInstances(id, states)) {
            ExecutionInstance instance = state.getInstance();
            DateTime instanceTime = instance.getInstanceTime();
            // Start date inclusive and end date exclusive.
            // If start date and end date are equal no instances will be added.
            if ((instanceTime.isEqual(start) || instanceTime.isAfter(start))
                    && instanceTime.isBefore(end)) {
                instancesToReturn.add(state);
            }
        }
        return instancesToReturn;
    }

    @Override
    public Collection<InstanceState> getExecutionInstances(ID entityId, Collection<InstanceState.STATE> states)
        throws StateStoreException {
        Collection<InstanceState> instances = new ArrayList<InstanceState>();
        for (Map.Entry<String, InstanceState> instanceState : instanceStates.entrySet()) {
            if (instanceState.getKey().startsWith(entityId.toString())
                    && states.contains(instanceState.getValue().getCurrentState())) {
                instances.add(instanceState.getValue());
            }
        }
        return instances;
    }

    @Override
    public InstanceState getLastExecutionInstance(Entity entity, String cluster) throws StateStoreException {
        ID id = new ID(entity, cluster);
        if (!entityStates.containsKey(id.getEntityKey())) {
            throw new StateStoreException("Entity with key, " + id.getEntityKey() + " does not exist.");
        }
        InstanceState latestState = null;
        // TODO : Very crude. Iterating over all entries and getting the last one.
        for (Map.Entry<String, InstanceState> instanceState : instanceStates.entrySet()) {
            if (instanceState.getKey().startsWith(id.toString())) {
                latestState = instanceState.getValue();
            }
        }
        return latestState;
    }

    @Override
    public boolean executionInstanceExists(ID instanceId) {
        return instanceStates.containsKey(instanceId.toString());
    }

    @Override
    public void deleteExecutionInstances(ID entityId) {
        for (String instanceKey : Lists.newArrayList(instanceStates.keySet())) {
            if (instanceKey.startsWith(entityId.getEntityKey())) {
                instanceStates.remove(instanceKey);
            }
        }
    }

    public void clear() {
        entityStates.clear();
        instanceStates.clear();
    }
}
