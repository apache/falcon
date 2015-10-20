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
import org.apache.falcon.execution.ExecutionInstance;

/**
 * Any handler interested in handling state changes of instances must implement this interface.
 */
public interface InstanceStateChangeHandler {

    /**
     * Invoked when an instance is created.
     *
     * @param instance
     * @throws FalconException
     */
    void onTrigger(ExecutionInstance instance) throws FalconException;

    /**
     * Invoked when all gating conditions are satisfied.
     *
     * @param instance
     * @throws FalconException
     */
    void onConditionsMet(ExecutionInstance instance) throws FalconException;

    /**
     * Invoked when an instance scheduled on a DAG Engine.
     *
     * @param instance
     * @throws FalconException
     */
    void onSchedule(ExecutionInstance instance) throws FalconException;

    /**
     * Invoked on suspension of an instance.
     *
     * @param instance
     * @throws FalconException
     */
    void onSuspend(ExecutionInstance instance) throws FalconException;

    /**
     * Invoked when an instance is resumed.
     *
     * @param instance
     * @throws FalconException
     */
    void onResume(ExecutionInstance instance) throws FalconException;

    /**
     * Invoked when an instance is killed.
     *
     * @param instance
     * @throws FalconException
     */
    void onKill(ExecutionInstance instance) throws FalconException;

    /**
     * Invoked when an instance completes successfully.
     *
     * @param instance
     * @throws FalconException
     */
    void onSuccess(ExecutionInstance instance) throws FalconException;

    /**
     * Invoked when execution of an instance fails.
     *
     * @param instance
     * @throws FalconException
     */
    void onFailure(ExecutionInstance instance) throws FalconException;

    /**
     * Invoked when an instance times out waiting for gating conditions to be satisfied.
     *
     * @param instance
     * @throws FalconException
     */
    void onTimeOut(ExecutionInstance instance) throws FalconException;
}
