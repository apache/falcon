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
 * Test cases for Entity operations using Falcon Native Scheduler.
 */
public class EntitySchedulerManagerJerseyIT extends AbstractSchedulerManagerJerseyIT {


    @BeforeClass
    public void setup() throws Exception {
        updateStartUpProps();
        super.setup();
    }

    @Test
    public void testEntitySubmitAndSchedule() throws Exception {
        UnitTestContext context = new UnitTestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        String colo = overlay.get(COLO);
        String cluster = overlay.get(CLUSTER);
        submitCluster(colo, cluster, null);
        context.prepare();

        submitProcess(overlay);

        String processName = overlay.get(PROCESS_NAME);
        APIResult result = falconUnitClient.getStatus(EntityType.PROCESS, overlay.get(PROCESS_NAME), cluster, null);
        assertStatus(result);
        Assert.assertEquals(AbstractEntityManager.EntityStatus.SUBMITTED.name(), result.getMessage());

        scheduleProcess(processName, cluster, START_INSTANCE, 1);

        result = falconUnitClient.getStatus(EntityType.PROCESS, processName, cluster, null);
        assertStatus(result);
        Assert.assertEquals(AbstractEntityManager.EntityStatus.RUNNING.name(), result.getMessage());

    }

    @Test
    public void testEntitySuspendResume() throws Exception {
        UnitTestContext context = new UnitTestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        String colo = overlay.get(COLO);
        String cluster = overlay.get(CLUSTER);
        submitCluster(colo, cluster, null);
        context.prepare();

        submitProcess(overlay);

        String processName = overlay.get(PROCESS_NAME);
        APIResult result = falconUnitClient.getStatus(EntityType.PROCESS, processName, cluster, null);
        assertStatus(result);
        Assert.assertEquals(AbstractEntityManager.EntityStatus.SUBMITTED.name(), result.getMessage());

        scheduleProcess(processName, cluster, START_INSTANCE, 1);

        result = falconUnitClient.suspend(EntityType.PROCESS, processName, cluster, null);
        assertStatus(result);

        result = falconUnitClient.getStatus(EntityType.PROCESS, processName, cluster, null);
        assertStatus(result);
        Assert.assertEquals(AbstractEntityManager.EntityStatus.SUSPENDED.name(), result.getMessage());

        result = falconUnitClient.resume(EntityType.PROCESS, processName, cluster, null);
        assertStatus(result);

        result = falconUnitClient.getStatus(EntityType.PROCESS, processName, cluster, null);
        assertStatus(result);
        Assert.assertEquals(AbstractEntityManager.EntityStatus.RUNNING.name(), result.getMessage());

    }


    @Test
    public void testProcessDelete() throws Exception {
        UnitTestContext context = new UnitTestContext();
        Map<String, String> overlay = context.getUniqueOverlay();
        String colo = overlay.get(COLO);
        String cluster = overlay.get(CLUSTER);
        submitCluster(colo, cluster, null);
        context.prepare();

        submitProcess(overlay);

        String processName = overlay.get(PROCESS_NAME);
        APIResult result = falconUnitClient.getStatus(EntityType.PROCESS, processName, cluster, null);
        assertStatus(result);
        Assert.assertEquals(AbstractEntityManager.EntityStatus.SUBMITTED.name(), result.getMessage());

        scheduleProcess(processName, cluster, START_INSTANCE, 1);

        result = falconUnitClient.getStatus(EntityType.PROCESS, processName, cluster, null);
        assertStatus(result);

        result = falconUnitClient.delete(EntityType.PROCESS, processName, cluster);
        assertStatus(result);

    }

}
