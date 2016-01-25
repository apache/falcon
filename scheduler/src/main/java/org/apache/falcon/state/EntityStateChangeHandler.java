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
package org.apache.falcon.state;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.v0.Entity;

/**
 * Any handler interested in handling state changes of entities must implement this interface.
 */
public interface EntityStateChangeHandler {

    /**
     * Invoked when an entity is submitted.
     *
     * @param entity
     * @throws FalconException
     */
    void onSubmit(Entity entity) throws FalconException;

    /**
     * Invoked when an entity is scheduled.
     *
     * @param entity
     * @throws FalconException
     */
    void onSchedule(Entity entity) throws FalconException;

    /**
     * Invoked when an entity is suspended.
     *
     * @param entity
     * @throws FalconException
     */
    void onSuspend(Entity entity) throws FalconException;

    /**
     * Invoked when the an entity is resumed.
     *
     * @param entity
     * @throws FalconException
     */
    void onResume(Entity entity) throws FalconException;

    /**
     * Invoked when and entity is killed/deleted.
     * @param entity
     * @throws FalconException
     */
    void onKill(Entity entity) throws FalconException;
}
