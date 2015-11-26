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

import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.exception.InvalidStateTransitionException;

/**
 * Represents the state of a schedulable entity.
 * Implements {@link org.apache.falcon.state.StateMachine} for an entity.
 */
public class EntityState implements StateMachine<EntityState.STATE, EntityState.EVENT> {
    private Entity entity;
    private STATE currentState;
    private static final STATE INITIAL_STATE = STATE.SUBMITTED;

    /**
     * Enumerates all the valid states of a schedulable entity and the valid transitions from that state.
     */
    public enum STATE implements StateMachine<EntityState.STATE, EntityState.EVENT> {
        SUBMITTED {
            @Override
            public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
                switch (event) {
                case SCHEDULE:
                    return STATE.SCHEDULED;
                case SUBMIT:
                    return this;
                default:
                    throw new InvalidStateTransitionException("Submitted entities can only be scheduled.");
                }
            }
        },
        SCHEDULED {
            @Override
            public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
                switch (event) {
                case SUSPEND:
                    return STATE.SUSPENDED;
                case SCHEDULE:
                    return this;
                default:
                    throw new InvalidStateTransitionException("Scheduled entities can only be suspended.");
                }
            }
        },
        SUSPENDED {
            @Override
            public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
                switch (event) {
                case RESUME:
                    return STATE.SCHEDULED;
                case SUSPEND:
                    return this;
                default:
                    throw new InvalidStateTransitionException("Suspended entities can only be resumed.");
                }
            }
        }
    }

    /**
     * Enumerates all the valid events that can cause a state transition.
     */
    public enum EVENT {
        SUBMIT,
        SCHEDULE,
        SUSPEND,
        RESUME
    }

    /**
     * Constructor.
     *
     * @param e - Entity
     */
    public EntityState(Entity e) {
        this.entity = e;
        currentState = INITIAL_STATE;
    }

    /**
     * @return - The entity
     */
    public Entity getEntity() {
        return entity;
    }

    /**
     * @param e - entity
     * @return - This instance
     */
    public EntityState setEntity(Entity e) {
        this.entity = e;
        return this;
    }

    /**
     * @return - Current state of the entity.
     */
    public STATE getCurrentState() {
        return currentState;
    }

    /**
     * @param state
     * @return - This instance
     */
    public EntityState setCurrentState(STATE state) {
        this.currentState = state;
        return this;
    }

    @Override
    public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
        return currentState.nextTransition(event);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EntityState other = (EntityState) o;

        if (this.getCurrentState() != null ? !this.getCurrentState().equals(other.getCurrentState())
                : other.getCurrentState() != null) {
            return false;
        }

        if (this.getEntity() != null ? !this.getEntity().equals(other.getEntity())
                : other.getEntity() != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = currentState != null ? currentState.hashCode() : 0;
        result = 31 * result + (entity != null ? entity.hashCode() : 0);
        return result;
    }
}
