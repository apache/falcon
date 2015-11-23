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
package org.apache.falcon.execution;

import org.apache.falcon.FalconException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.state.EntityClusterID;
import org.apache.falcon.state.InstanceStateChangeHandler;
import org.apache.falcon.state.store.AbstractStateStore;
import org.apache.falcon.state.store.StateStore;

/**
 * This class is responsible for creation of execution instances for a given entity.
 * An execution instance is created upon receipt of a "trigger event".
 * It also handles the state transition of each execution instance.
 * This class is also responsible for handling user interrupts for an entity such as suspend, kill etc.
 */
public abstract class EntityExecutor implements NotificationHandler, InstanceStateChangeHandler {
    protected static final ConfigurationStore STORE = ConfigurationStore.get();
    // The number of execution instances to be cached by default
    public static final String DEFAULT_CACHE_SIZE = "20";
    protected String cluster;
    protected static final StateStore STATE_STORE = AbstractStateStore.get();
    protected EntityClusterID id;

    /**
     * Schedules execution instances for the entity. Idempotent operation.
     *
     * @throws FalconException
     */
    public abstract void schedule() throws FalconException;

    /**
     * Suspends all "active" execution instances of the entity.  Idempotent operation.
     * The operation can fail on certain instances. In such cases, the operation is partially successful.
     *
     * @throws FalconException - When the operation on an instance fails
     */
    public abstract void suspendAll() throws FalconException;

    /**
     * Resumes all suspended execution instances of the entity.  Idempotent operation.
     * The operation can fail on certain instances. In such cases, the operation is partially successful.
     *
     * @throws FalconException - When the operation on an instance fails
     */
    public abstract void resumeAll() throws FalconException;

    /**
     * Deletes all execution instances of an entity, even from the store.  Idempotent operation.
     * The operation can fail on certain instances. In such cases, the operation is partially successful.
     *
     * @throws FalconException - When the operation on an instance fails
     */
    public abstract void killAll() throws FalconException;

    /**
     * Suspends a specified set of execution instances.  Idempotent operation.
     * The operation can fail on certain instances. In such cases, the operation is partially successful.
     *
     * @param instance
     * @throws FalconException
     */
    public abstract void suspend(ExecutionInstance instance) throws FalconException;

    /**
     * Resumes a specified set of execution instances.  Idempotent operation.
     * The operation can fail on certain instances. In such cases, the operation is partially successful.
     *
     * @param instance
     * @throws FalconException
     */
    public abstract void resume(ExecutionInstance instance) throws FalconException;

    /**
     * Kills a specified set of execution instances.  Idempotent operation.
     * The operation can fail on certain instances. In such cases, the operation is partially successful.
     *
     * @param instance
     * @throws FalconException
     */
    public abstract void kill(ExecutionInstance instance) throws FalconException;

    /**
     * @return The entity
     */
    public abstract Entity getEntity();

    /**
     * @return ID of the entity
     */
    public EntityClusterID getId() {
        return id;
    }
}
