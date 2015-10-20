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
package org.apache.falcon.workflow.engine;

import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.exception.DAGEngineException;
import org.apache.falcon.execution.ExecutionInstance;
import org.apache.falcon.resource.InstancesResult;

import java.util.List;
import java.util.Properties;


/**
 * Interface for an implementation that executes a DAG.
 */
public interface DAGEngine {

    /**
     * Run an instance for execution.
     *
     * @param instance
     * @return
     * @throws DAGEngineException
     */
    String run(ExecutionInstance instance) throws DAGEngineException;

    /**
     * @param instance
     * @return true if an instance is scheduled for execution.
     * @throws DAGEngineException
     */
    boolean isScheduled(ExecutionInstance instance) throws DAGEngineException;

    /**
     * Suspend a running instance.
     *
     * @param instance
     * @throws DAGEngineException
     */
    void suspend(ExecutionInstance instance) throws DAGEngineException;

    /**
     * Resume a suspended instance.
     *
     * @param instance
     * @throws DAGEngineException
     */
    void resume(ExecutionInstance instance) throws DAGEngineException;

    /**
     * Kill a running instance.
     *
     * @param instance
     * @throws DAGEngineException
     */
    void kill(ExecutionInstance instance) throws DAGEngineException;

    /**
     * Re-run a terminated instance.
     *
     * @param instance
     * @throws DAGEngineException
     */
    void reRun(ExecutionInstance instance) throws DAGEngineException;

    /**
     * Perform dryrun of an instance.
     *
     * @param entity
     * @throws DAGEngineException
     */
    void submit(Entity entity) throws DAGEngineException;

    /**
     * Returns info about the Job.
     * @param externalID
     * @return
     */
    InstancesResult.Instance info(String externalID) throws DAGEngineException;

    /**
     * @param externalID
     * @return status of each individual node in the DAG.
     * @throws DAGEngineException
     */
    List<InstancesResult.InstanceAction> getJobDetails(String externalID) throws DAGEngineException;

    /**
     * @return true if DAG Engine is up and running.
     */
    boolean isAlive() throws DAGEngineException;

    /**
     * @param externalID
     * @return Configuration used to run the job.
     * @throws DAGEngineException
     */
    Properties getConfiguration(String externalID) throws DAGEngineException;
}
