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

import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.exception.DAGEngineException;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.workflow.engine.DAGEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * A Mock DAG Execution Engine used for tests.
 */
public class MockDAGEngine implements DAGEngine {
    private List<ExecutionInstance> failInstances = new ArrayList<>();
    private Map<ExecutionInstance, Integer> runInvocations =  new HashMap<>();

    public MockDAGEngine(String cluster) {

    }

    @Override
    public String run(ExecutionInstance instance) throws DAGEngineException {
        if (failInstances.contains(instance)) {
            throw new DAGEngineException("Mock failure.");
        }
        Integer count = 1;
        if (runInvocations.containsKey(instance)) {
            // Increment count
            count = runInvocations.get(instance) + 1;
        }

        runInvocations.put(instance, count);
        return "123";
    }

    @Override
    public boolean isScheduled(ExecutionInstance instance) throws DAGEngineException {
        return true;
    }

    @Override
    public void suspend(ExecutionInstance instance) throws DAGEngineException {
        if (failInstances.contains(instance)) {
            throw new DAGEngineException("mock failure.");
        }
    }

    @Override
    public void resume(ExecutionInstance instance) throws DAGEngineException {

    }

    @Override
    public void kill(ExecutionInstance instance) throws DAGEngineException {
        if (failInstances.contains(instance)) {
            throw new DAGEngineException("mock failure.");
        }
    }

    @Override
    public void reRun(ExecutionInstance instance, Properties props, boolean isForced) throws DAGEngineException {
        if (failInstances.contains(instance)) {
            throw new DAGEngineException("mock failure.");
        }
    }

    @Override
    public void submit(Entity entity) throws DAGEngineException {

    }

    @Override
    public InstancesResult.Instance info(String externalID) throws DAGEngineException {
        return new InstancesResult.Instance();
    }

    @Override
    public List<InstancesResult.InstanceAction> getJobDetails(String externalID) throws DAGEngineException {
        return null;
    }

    @Override
    public boolean isAlive() throws DAGEngineException {
        return false;
    }

    @Override
    public Properties getConfiguration(String externalID) throws DAGEngineException {
        return null;
    }

    @Override
    public void touch(Entity entity, Boolean skipDryRun) throws DAGEngineException {
    }

    public void addFailInstance(ExecutionInstance failInstance) {
        this.failInstances.add(failInstance);
    }

    public void removeFailInstance(ExecutionInstance failInstance) {
        this.failInstances.remove(failInstance);
    }

    public Integer getTotalRuns(ExecutionInstance instance) {
        return runInvocations.get(instance);
    }
}
