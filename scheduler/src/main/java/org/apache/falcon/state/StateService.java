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
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.execution.ExecutionInstance;
import org.apache.falcon.state.store.AbstractStateStore;
import org.apache.falcon.state.store.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service that fetches state from state store, handles state transitions of entities and instances,
 * invokes state change handler and finally persists the new state in the state store.
 */
public final class StateService {
    private static final Logger LOG = LoggerFactory.getLogger(StateService.class);
    private static final StateService LIFE_CYCLE_SERVICE = new StateService();
    private final StateStore stateStore;

    private StateService() {
        stateStore = AbstractStateStore.get();
    }

    /**
     * @return - Singleton instance of StateService
     */
    public static StateService get() {
        return LIFE_CYCLE_SERVICE;
    }

    /**
     * @return - Name of the service
     */
    public String getName() {
        return "EntityLifeCycleService";
    }

    /**
     * Fetches the entity from state store, applies state transitions, calls appropriate method on the handler and
     * persists the final state in the store.
     *
     * @param entity
     * @param event
     * @param handler
     * @throws FalconException
     */
    public void handleStateChange(Entity entity, EntityState.EVENT event, EntityStateChangeHandler handler)
        throws FalconException {
        EntityID id = new EntityID(entity);
        if (!stateStore.entityExists(id)) {
            // New entity
            if (event == EntityState.EVENT.SUBMIT) {
                callbackHandler(entity, EntityState.EVENT.SUBMIT, handler);
                stateStore.putEntity(new EntityState(entity));
                LOG.debug("Entity {} submitted due to event {}.", id, event.name());
            } else {
                throw new FalconException("Entity " + id + " does not exist in state store.");
            }
        } else {
            if (entity.getEntityType() == EntityType.CLUSTER) {
                throw new FalconException("Cluster entity " + entity.getName() + " can only be submitted.");
            }
            EntityState entityState = stateStore.getEntity(id);
            EntityState.STATE newState = entityState.nextTransition(event);
            callbackHandler(entity, event, handler);
            entityState.setCurrentState(newState);
            stateStore.updateEntity(entityState);
            LOG.debug("State of entity: {} changed to: {} as a result of event: {}.", id,
                    entityState.getCurrentState(), event.name());
        }
    }

    // Invokes the right method on the state change handler
    private void callbackHandler(Entity entity, EntityState.EVENT event, EntityStateChangeHandler handler)
        throws FalconException {
        if (handler == null) {
            return;
        }
        switch (event) {
        case SUBMIT:
            handler.onSubmit(entity);
            break;
        case SCHEDULE:
            handler.onSchedule(entity);
            break;
        case SUSPEND:
            handler.onSuspend(entity);
            break;
        case RESUME:
            handler.onResume(entity);
            break;
        default: // Do nothing, only propagate events that originate from user
        }
    }

    /**
     * Fetches the instance from state store, applies state transitions, calls appropriate method on the handler and
     * persists the final state in the store.
     *
     * @param instance
     * @param event
     * @param handler
     * @throws FalconException
     */
    public void handleStateChange(ExecutionInstance instance, InstanceState.EVENT event,
                                  InstanceStateChangeHandler handler) throws FalconException {
        InstanceID id = new InstanceID(instance);
        if (!stateStore.executionInstanceExists(id)) {
            // New instance
            if (event == InstanceState.EVENT.TRIGGER) {
                callbackHandler(instance, InstanceState.EVENT.TRIGGER, handler);
                stateStore.putExecutionInstance(new InstanceState(instance));
                LOG.debug("Instance {} triggered due to event {}.", id, event.name());
            } else {
                throw new FalconException("Instance " + id + "does not exist.");
            }
        } else {
            InstanceState instanceState = stateStore.getExecutionInstance(id);
            InstanceState.STATE newState = instanceState.nextTransition(event);
            callbackHandler(instance, event, handler);
            instanceState = new InstanceState(instance);
            instanceState.setCurrentState(newState);
            stateStore.updateExecutionInstance(instanceState);
            LOG.debug("State of instance: {} changed to: {} as a result of event: {}.", id,
                    instanceState.getCurrentState(), event.name());
        }
    }

    // Invokes the right method on the state change handler
    private void callbackHandler(ExecutionInstance instance, InstanceState.EVENT event,
                                 InstanceStateChangeHandler handler) throws FalconException {
        if (handler == null) {
            return;
        }
        switch (event) {
        case TRIGGER:
            handler.onTrigger(instance);
            break;
        case CONDITIONS_MET:
            handler.onConditionsMet(instance);
            break;
        case TIME_OUT:
            handler.onTimeOut(instance);
            break;
        case SCHEDULE:
            handler.onSchedule(instance);
            break;
        case SUSPEND:
            handler.onSuspend(instance);
            break;
        case RESUME_WAITING:
        case RESUME_READY:
        case RESUME_RUNNING:
            handler.onResume(instance);
            break;
        case KILL:
            handler.onKill(instance);
            break;
        case SUCCEED:
            handler.onSuccess(instance);
            break;
        case FAIL:
            handler.onFailure(instance);
            break;
        default: // Do nothing
        }
    }
}
