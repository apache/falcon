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

package org.apache.falcon.resource;

import org.apache.falcon.entity.v0.EntityType;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

/**
 * Tests for Instance operations using Falcon Native Scheduler.
 */
public class InstanceSchedulerManagerJerseyIT extends AbstractSchedulerManagerJerseyIT {


    private static final String END_TIME = "2012-04-21T00:00Z";
    private static final String HELLO_WORLD_WORKFLOW = "helloworldworkflow.xml";

    @BeforeClass
    public void setup() throws Exception {
        updateStartUpProps();
        super.setup();
    }

    @Test
    public void testProcessInstanceExecution() throws Exception {
        UnitTestContext context = new UnitTestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        String colo = overlay.get(COLO);
        String cluster = overlay.get(CLUSTER);

        submitCluster(colo, cluster, null);
        context.prepare(HELLO_WORLD_WORKFLOW);
        submitProcess(overlay);

        String processName = overlay.get(PROCESS_NAME);
        scheduleProcess(processName, cluster, START_INSTANCE, 1);

        waitForStatus(EntityType.PROCESS.toString(), processName,
                START_INSTANCE, InstancesResult.WorkflowStatus.SUCCEEDED);

        InstancesResult.WorkflowStatus status = getClient().getInstanceStatus(EntityType.PROCESS.name(),
                processName, START_INSTANCE);
        Assert.assertEquals(status, InstancesResult.WorkflowStatus.SUCCEEDED);

    }

    @Test
    public void testKillInstances() throws Exception {
        UnitTestContext context = new UnitTestContext();
        Map<String, String> overlay = context.getUniqueOverlay();

        setupProcessExecution(context, overlay, 1);

        String processName = overlay.get(PROCESS_NAME);
        String colo = overlay.get(COLO);

        waitForStatus(EntityType.PROCESS.name(), context.processName, START_INSTANCE,
                InstancesResult.WorkflowStatus.RUNNING);

        InstancesResult result = falconUnitClient.killInstances(EntityType.PROCESS.toString(),
                processName, START_INSTANCE, END_TIME, colo, null, null, null, null);
        assertStatus(result);

        InstancesResult.WorkflowStatus status = getClient().getInstanceStatus(EntityType.PROCESS.name(),
                processName, START_INSTANCE);
        Assert.assertEquals(status, InstancesResult.WorkflowStatus.KILLED);


    }

    @Test
    public void testSuspendResumeInstances() throws Exception {
        UnitTestContext context = new UnitTestContext();
        Map<String, String> overlay = context.getUniqueOverlay();

        setupProcessExecution(context, overlay, 1);

        String processName = overlay.get(PROCESS_NAME);
        String colo = overlay.get(COLO);

        waitForStatus(EntityType.PROCESS.toString(), processName,
                START_INSTANCE, InstancesResult.WorkflowStatus.RUNNING);

        falconUnitClient.suspendInstances(EntityType.PROCESS.toString(), processName, START_INSTANCE,
                END_TIME, colo, null, null, null, null);

        InstancesResult.WorkflowStatus status = getClient().getInstanceStatus(EntityType.PROCESS.name(),
                processName, START_INSTANCE);
        Assert.assertEquals(status, InstancesResult.WorkflowStatus.SUSPENDED);

        falconUnitClient.resumeInstances(EntityType.PROCESS.toString(), processName, START_INSTANCE,
                END_TIME, colo, null, null, null, null);
        status = getClient().getInstanceStatus(EntityType.PROCESS.name(),
                processName, START_INSTANCE);
        Assert.assertEquals(status, InstancesResult.WorkflowStatus.RUNNING);
    }

    @Test
    public void testListInstances() throws Exception {
        UnitTestContext context = new UnitTestContext();
        Map<String, String> overlay = context.getUniqueOverlay();

        setupProcessExecution(context, overlay, 4);

        String processName = overlay.get(PROCESS_NAME);
        String colo = overlay.get(COLO);

        waitForStatus(EntityType.PROCESS.toString(), processName,
                START_INSTANCE, InstancesResult.WorkflowStatus.RUNNING);

        InstancesResult result = falconUnitClient.getStatusOfInstances(EntityType.PROCESS.toString(), processName,
                START_INSTANCE, "2012-04-23T00:00Z", colo, null, null, null, null, 0, 3, null);
        Assert.assertEquals(3, result.getInstances().length);
    }
}
