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

import org.apache.falcon.exception.InvalidStateTransitionException;
import org.apache.falcon.execution.ExecutionInstance;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents the state of an execution instance.
 * Implements {@link org.apache.falcon.state.StateMachine} for an instance.
 */
public class InstanceState implements StateMachine<InstanceState.STATE, InstanceState.EVENT> {
    private ExecutionInstance instance;
    private STATE currentState;
    private static final STATE INITIAL_STATE = STATE.WAITING;

    /**
     * Enumerates all the valid states of an instance and the valid transitions from that state.
     */
    public enum STATE implements StateMachine<InstanceState.STATE, InstanceState.EVENT> {
        WAITING {
            @Override
            public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
                switch (event) {
                case SUSPEND:
                    return SUSPENDED;
                case KILL:
                    return KILLED;
                case CONDITIONS_MET:
                    return READY;
                case TIME_OUT:
                    return TIMED_OUT;
                case TRIGGER:
                    return this;
                default:
                    throw new InvalidStateTransitionException("Event " + event.name() + " not valid for state, "
                            + STATE.WAITING.name());
                }
            }
        },
        READY {
            @Override
            public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
                switch (event) {
                case SUSPEND:
                    return SUSPENDED;
                case KILL:
                    return KILLED;
                case SCHEDULE:
                    return RUNNING;
                case CONDITIONS_MET:
                    return this;
                default:
                    throw new InvalidStateTransitionException("Event " + event.name()
                            + " not valid for state, " + this.name());
                }
            }
        },
        RUNNING {
            @Override
            public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
                switch (event) {
                case SUSPEND:
                    return SUSPENDED;
                case KILL:
                    return KILLED;
                case SUCCEED:
                    return SUCCEEDED;
                case FAIL:
                    return FAILED;
                case SCHEDULE:
                    return this;
                default:
                    throw new InvalidStateTransitionException("Event " + event.name()
                            + " not valid for state, " + this.name());
                }
            }
        }, SUCCEEDED {
            @Override
            public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
                if (event == EVENT.SUCCEED) {
                    return this;
                }
                throw new InvalidStateTransitionException("Instance is in terminal state, " + this.name()
                        + ". Cannot apply transitions.");
            }
        },
        FAILED {
            @Override
            public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
                if (event == EVENT.FAIL) {
                    return this;
                }
                throw new InvalidStateTransitionException("Instance is in terminal state, " + this.name()
                        + ". Cannot apply transitions.");
            }
        },
        KILLED {
            @Override
            public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
                if (event == EVENT.KILL) {
                    return this;
                }
                throw new InvalidStateTransitionException("Instance is in terminal state, " + this.name()
                        + ". Cannot apply transitions.");
            }
        },
        TIMED_OUT {
            @Override
            public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
                if (event == EVENT.TIME_OUT) {
                    return this;
                }
                throw new InvalidStateTransitionException("Instance is in terminal state, " + this.name()
                        + ". Cannot apply transitions.");
            }
        },
        SUSPENDED {
            @Override
            public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
                switch (event) {
                case RESUME_WAITING:
                    return WAITING;
                case RESUME_READY:
                    return READY;
                case RESUME_RUNNING:
                    return RUNNING;
                case SUSPEND:
                    return this;
                // The instance can complete execution on DAG engine, just after a suspend was issued.
                // Especially with Oozie, it finishes execution of current action before suspending.
                // Hence need to allow terminal states too.
                case SUCCEED:
                    return SUCCEEDED;
                case FAIL:
                    return FAILED;
                case KILL:
                    return KILLED;
                default:
                    throw new InvalidStateTransitionException("Event " + event.name()
                            + " not valid for state, " + this.name());
                }
            }
        }
    }

    /**
     * Enumerates all the valid events that can cause a state transition.
     */
    public enum EVENT {
        TRIGGER,
        CONDITIONS_MET,
        TIME_OUT,
        SCHEDULE,
        SUSPEND,
        RESUME_WAITING,
        RESUME_READY,
        RESUME_RUNNING,
        KILL,
        SUCCEED,
        FAIL
    }

    /**
     * Constructor.
     *
     * @param instance
     */
    public InstanceState(ExecutionInstance instance) {
        this.instance = instance;
        currentState = INITIAL_STATE;
    }

    /**
     * @return execution instance
     */
    public ExecutionInstance getInstance() {
        return instance;
    }

    /**
     * @return current state
     */
    public STATE getCurrentState() {
        return currentState;
    }

    /**
     * @param state
     * @return This instance
     */
    public InstanceState setCurrentState(STATE state) {
        this.currentState = state;
        return this;
    }

    @Override
    public STATE nextTransition(EVENT event) throws InvalidStateTransitionException {
        return currentState.nextTransition(event);
    }

    /**
     * @return "active" states of an instance.
     */
    public static List<STATE> getActiveStates() {
        List<InstanceState.STATE> states = new ArrayList<STATE>();
        states.add(STATE.RUNNING);
        states.add(STATE.READY);
        states.add(STATE.WAITING);
        return states;
    }

    /**
     * @return "running" states of an instance.
     */
    public static List<STATE> getRunningStates() {
        List<InstanceState.STATE> states = new ArrayList<STATE>();
        states.add(STATE.RUNNING);
        return states;
    }

    /**
     *  @return "terminal" states of an instance.
     */
    public static List<STATE> getTerminalStates() {
        List<InstanceState.STATE> states = new ArrayList<STATE>();
        states.add(STATE.FAILED);
        states.add(STATE.KILLED);
        states.add(STATE.SUCCEEDED);
        return states;
    }

    @Override
    public String toString() {
        return instance.getId().toString() + "STATE: " + currentState.toString();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        InstanceState other = (InstanceState) o;

        if (this.getCurrentState() != null ? !this.getCurrentState().equals(other.getCurrentState())
                : other.getCurrentState() != null) {
            return false;
        }

        if (this.getInstance() != null ? !this.getInstance().equals(other.getInstance())
                : other.getInstance() != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = currentState != null ? currentState.hashCode() : 0;
        result = 31 * result + (instance != null ? instance.hashCode() : 0);
        return result;
    }

}
