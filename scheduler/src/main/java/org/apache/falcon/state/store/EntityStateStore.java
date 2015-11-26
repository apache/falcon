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
import org.apache.falcon.state.EntityID;
import org.apache.falcon.state.EntityState;

import java.util.Collection;

/**
 * Interface to abstract out Entity store API.
 */
public interface EntityStateStore {
    /**
     * @param entityState
     * @throws StateStoreException
     */
    void putEntity(EntityState entityState) throws StateStoreException;

    /**
     * @param entityId
     * @return Entity corresponding to the key
     * @throws StateStoreException - If entity does not exist.
     */
    EntityState getEntity(EntityID entityId) throws StateStoreException;

    /**
     * @param entityId
     * @return true, if entity exists in store.
     */
    boolean entityExists(EntityID entityId) throws StateStoreException;;

    /**
     * @param state
     * @return Entities in a given state.
     */
    Collection<Entity> getEntities(EntityState.STATE state) throws StateStoreException;

    /**
     * @return All Entities in the store.
     */
    Collection<EntityState> getAllEntities() throws StateStoreException;

    /**
     * Update an existing entity with the new values.
     *
     * @param entityState
     * @throws StateStoreException when entity does not exist.
     */
    void updateEntity(EntityState entityState) throws StateStoreException;

    /**
     * Removes the entity and its instances from the store.
     *
     * @param entityId
     * @throws StateStoreException
     */
    void deleteEntity(EntityID entityId) throws StateStoreException;


    /**
     * Removes all entities and its instances from the store.
     *
     * @throws StateStoreException
     */
    void deleteEntities() throws StateStoreException;

    /**
     * Checks whether entity completed or not.
     * @param entityId
     * @return
     */
    boolean isEntityCompleted(EntityID entityId);
}
