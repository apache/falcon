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

import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.exception.StateStoreException;
import org.apache.falcon.state.EntityClusterID;
import org.apache.falcon.state.EntityID;
import org.apache.falcon.state.InstanceID;
import org.apache.falcon.state.InstanceState;
import org.joda.time.DateTime;

import java.util.Collection;

/**
 * Interface to abstract out instance store API.
 */
// TODO : Add order and limit capabilities to the API
public interface InstanceStateStore {
    /**
     * Adds an execution instance to the store.
     *
     * @param instanceState
     * @throws StateStoreException
     */
    void putExecutionInstance(InstanceState instanceState) throws StateStoreException;

    /**
     * @param instanceId
     * @return Execution instance corresponding to the name.
     * @throws StateStoreException - When instance does not exist
     */
    InstanceState getExecutionInstance(InstanceID instanceId) throws StateStoreException;

    /**
     * Updates an execution instance in the store.
     *
     * @param instanceState
     * @throws StateStoreException - if the instance does not exist.
     */
    void updateExecutionInstance(InstanceState instanceState) throws StateStoreException;

    /**
     * @param entity
     * @param cluster
     * @return - All execution instances for the given entity and cluster.
     * @throws StateStoreException
     */
    Collection<InstanceState> getAllExecutionInstances(Entity entity, String cluster) throws StateStoreException;

    /**
     * @param entity
     * @param cluster
     * @param states
     * @return - All execution instances for the given entity and cluster and states
     * @throws StateStoreException
     */
    Collection<InstanceState> getExecutionInstances(Entity entity, String cluster,
                                                    Collection<InstanceState.STATE> states) throws StateStoreException;

    /**
     * @param entity
     * @param cluster
     * @param states
     * @return - All execution instances for the given entity and cluster and states
     * @throws StateStoreException
     */
    Collection<InstanceState> getExecutionInstances(Entity entity, String cluster,
                                                    Collection<InstanceState.STATE> states,
                                                    DateTime start, DateTime end) throws StateStoreException;

    /**
     * @param entityClusterID
     * @param states
     * @return - All execution instance for an given entityKey (that includes the cluster name)
     * @throws StateStoreException
     */
    Collection<InstanceState> getExecutionInstances(EntityClusterID entityClusterID,
                                                    Collection<InstanceState.STATE> states) throws StateStoreException;
    /**
     * @param entity
     * @param cluster
     * @return - The latest execution instance
     * @throws StateStoreException
     */
    InstanceState getLastExecutionInstance(Entity entity, String cluster) throws StateStoreException;

    /**
     * @param instanceId
     * @return true, if instance exists.
     */
    boolean executionInstanceExists(InstanceID instanceId) throws StateStoreException;

    /**
     * Delete instances of a given entity.
     *
     * @param entityId
     */
    void deleteExecutionInstances(EntityID entityId) throws StateStoreException;


    /**
     * Delete an instance based on ID.
     *
     * @param instanceID
     * @throws StateStoreException
     */
    void deleteExecutionInstance(InstanceID instanceID) throws StateStoreException;

    /**
     * Delete all instances.
     */
    void deleteExecutionInstances();
}
