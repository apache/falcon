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
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.exception.StateStoreException;
import org.apache.falcon.notification.service.event.Event;
import org.apache.falcon.service.FalconService;
import org.apache.falcon.state.EntityClusterID;
import org.apache.falcon.state.EntityState;
import org.apache.falcon.state.EntityStateChangeHandler;
import org.apache.falcon.state.InstanceID;
import org.apache.falcon.state.StateService;
import org.apache.falcon.state.store.AbstractStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This singleton is the entry point for all callbacks from the notification services.
 * The execution service handles any system level events that apply to all entities.
 * It is responsible for creation of entity executors one per entity, per cluster.
 */
public final class FalconExecutionService implements FalconService, EntityStateChangeHandler, NotificationHandler {

    private static final Logger LOG = LoggerFactory.getLogger(FalconExecutionService.class);

    // Stores all entity executors in memory
    private ConcurrentMap<EntityClusterID, EntityExecutor> executors = new ConcurrentHashMap<>();

    private static FalconExecutionService executionService = new FalconExecutionService();

    @Override
    public String getName() {
        return "FalconExecutionService";
    }

    public void init() {
        LOG.debug("State store instance being used : {}", AbstractStateStore.get());
        // Initialize all executors from store
        try {
            for (Entity entity : AbstractStateStore.get().getEntities(EntityState.STATE.SCHEDULED)) {
                try {
                    for (String cluster : EntityUtil.getClustersDefinedInColos(entity)) {
                        EntityExecutor executor = createEntityExecutor(entity, cluster);
                        executors.put(new EntityClusterID(entity, cluster), executor);
                        executor.schedule();
                    }
                } catch (FalconException e) {
                    LOG.error("Unable to load entity : " + entity.getName(), e);
                    throw new RuntimeException(e);
                }
            }
        } catch (StateStoreException e) {
            LOG.error("Unable to get Entities from State Store ", e);
            throw new RuntimeException(e);
        }
        // TODO : During migration, the state store itself may not have been completely bootstrapped.
    }

    /**
     * Returns an EntityExecutor implementation based on the entity type.
     *
     * @param entity
     * @param cluster
     * @return
     * @throws FalconException
     */
    private EntityExecutor createEntityExecutor(Entity entity, String cluster) throws FalconException {
        switch (entity.getEntityType()) {
        case FEED:
            throw new UnsupportedOperationException("No support yet for feed.");
        case PROCESS:
            return new ProcessExecutor(((Process)entity), cluster);
        default:
            throw new IllegalArgumentException("Unhandled type " + entity.getEntityType().name());
        }
    }

    @Override
    public void destroy() throws FalconException {

    }

    /**
     * @return - An instance(singleton) of FalconExecutionService
     */
    public static FalconExecutionService get() {
        return executionService;
    }

    private FalconExecutionService() {}

    @Override
    public void onEvent(Event event) throws FalconException {
        // Currently, simply passes along the event to the appropriate executor
        EntityClusterID id = null;
        if (event.getTarget() instanceof EntityClusterID) {
            id = (EntityClusterID) event.getTarget();
        } else if (event.getTarget() instanceof InstanceID) {
            id = ((InstanceID) event.getTarget()).getEntityClusterID();
        }

        if (id != null) {
            EntityExecutor executor = executors.get(id);
            if (executor == null) {
                // The executor has gone away, throw an exception so the notification service knows
                throw new FalconException("Target executor for " + event.getTarget() + " does not exist.");
            }
            executor.onEvent(event);
        }
    }

    @Override
    public void onSubmit(Entity entity) throws FalconException {
        // Do nothing
    }

    @Override
    public void onSchedule(Entity entity) throws FalconException {
        for (String cluster : EntityUtil.getClustersDefinedInColos(entity)) {
            EntityExecutor executor = createEntityExecutor(entity, cluster);
            EntityClusterID id = new EntityClusterID(entity, cluster);
            executors.put(id, executor);
            LOG.info("Scheduling entity {}.", id);
            executor.schedule();
        }
    }

    @Override
    public void onSuspend(Entity entity) throws FalconException {
        for (String cluster : EntityUtil.getClustersDefinedInColos(entity)) {
            EntityExecutor executor = getEntityExecutor(entity, cluster);
            executor.suspendAll();
        }
    }

    @Override
    public void onResume(Entity entity) throws FalconException {
        for (String cluster : EntityUtil.getClustersDefinedInColos(entity)) {
            EntityExecutor executor = createEntityExecutor(entity, cluster);
            executors.put(new EntityClusterID(entity, cluster), executor);
            LOG.info("Resuming entity, {} of type {} on cluster {}.", entity.getName(),
                    entity.getEntityType(), cluster);
            executor.resumeAll();
        }
    }

    /**
     * Schedules an entity.
     *
     * @param entity
     * @throws FalconException
     */
    public void schedule(Entity entity) throws FalconException {
        StateService.get().handleStateChange(entity, EntityState.EVENT.SCHEDULE, this);
    }

    /**
     * Suspends an entity.
     *
     * @param entity
     * @throws FalconException
     */
    public void suspend(Entity entity) throws FalconException {
        StateService.get().handleStateChange(entity, EntityState.EVENT.SUSPEND, this);
    }

    /**
     * Resumes an entity.
     *
     * @param entity
     * @throws FalconException
     */
    public void resume(Entity entity) throws FalconException {
        StateService.get().handleStateChange(entity, EntityState.EVENT.RESUME, this);
    }

    /**
     * Deletes an entity from the execution service.
     *
     * @param entity
     * @throws FalconException
     */
    public void delete(Entity entity) throws FalconException {
        for (String cluster : EntityUtil.getClustersDefinedInColos(entity)) {
            EntityExecutor executor = getEntityExecutor(entity, cluster);
            executor.killAll();
            executors.remove(executor.getId());
        }
    }

    /**
     * Returns the instance of {@link EntityExecutor} for a given entity and cluster.
     *
     * @param entity
     * @param cluster
     * @return
     * @throws FalconException
     */
    public EntityExecutor getEntityExecutor(Entity entity, String cluster) throws FalconException {
        EntityClusterID id = new EntityClusterID(entity, cluster);
        if (executors.containsKey(id)) {
            return executors.get(id);
        } else {
            throw new FalconException("Entity executor for entity cluster key : " + id.getKey() + " does not exist.");
        }
    }
}
