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

import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.state.EntityID;
import org.apache.falcon.state.EntityState;
import org.apache.falcon.state.store.AbstractStateStore;
import org.apache.falcon.state.store.StateStore;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowEngineFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests the WorkflowEngineFactory class.
 */
public class WorkflowEngineFactoryTest extends AbstractTestBase {

    private StateStore stateStore = null;

    @BeforeClass
    public void init() throws Exception {
        this.dfsCluster = EmbeddedCluster.newCluster("testCluster");
        this.conf = dfsCluster.getConf();
        StartupProperties.get().setProperty("falcon.state.store.impl",
                "org.apache.falcon.state.store.InMemoryStateStore");
        setupConfigStore();
    }

    @AfterClass
    public void tearDown() {
        this.dfsCluster.shutdown();
    }

    // State store is set up to sync with Config Store. That gets tested too.
    public void setupConfigStore() throws Exception {
        stateStore = AbstractStateStore.get();
        getStore().registerListener(stateStore);
        storeEntity(EntityType.CLUSTER, "testCluster");
        storeEntity(EntityType.FEED, "clicksFeed");
        storeEntity(EntityType.FEED, "clicksSummary");
        storeEntity(EntityType.PROCESS, "summarize");
    }

    @Test
    public void testGetEngineByEntity() throws FalconException {
        // When entity is not specified, return oozie
        AbstractWorkflowEngine engine = WorkflowEngineFactory.getWorkflowEngine(null);
        Assert.assertTrue(engine instanceof OozieWorkflowEngine);

        // When entity not active on native, return oozie
        Process process = getStore().get(EntityType.PROCESS, "summarize");
        engine = WorkflowEngineFactory.getWorkflowEngine(process);
        Assert.assertTrue(engine instanceof OozieWorkflowEngine);

        // When entity active on native, return native
        stateStore.getEntity(new EntityID(process)).setCurrentState(EntityState.STATE.SCHEDULED);
        engine = WorkflowEngineFactory.getWorkflowEngine(process);
        Assert.assertTrue(engine instanceof FalconWorkflowEngine);
    }

    @Test
    public void testGetEngineByEntityAndProps() throws FalconException {
        // When entity is not specified, return oozie
        AbstractWorkflowEngine engine = WorkflowEngineFactory.getWorkflowEngine(null, null);
        Assert.assertTrue(engine instanceof OozieWorkflowEngine);

        // When entity specified, but, no props, return oozie
        Process process = getStore().get(EntityType.PROCESS, "summarize");
        stateStore.getEntity(new EntityID(process)).setCurrentState(EntityState.STATE.SUBMITTED);
        engine = WorkflowEngineFactory.getWorkflowEngine(process, null);
        Assert.assertTrue(engine instanceof OozieWorkflowEngine);

        // When entity specified, props set to oozie, return oozie
        Map<String, String> props = new HashMap<>();
        props.put(WorkflowEngineFactory.ENGINE_PROP, "oozie");
        engine = WorkflowEngineFactory.getWorkflowEngine(process, props);
        Assert.assertTrue(engine instanceof OozieWorkflowEngine);
    }

    @Test (expectedExceptions = FalconException.class,
            expectedExceptionsMessageRegExp = ".* is already scheduled on native engine.")
    public void testGetEngineError() throws FalconException {
        Process process = getStore().get(EntityType.PROCESS, "summarize");
        // When entity specified, props set to oozie, but scheduled on native, exception
        stateStore.getEntity(new EntityID(process)).setCurrentState(EntityState.STATE.SCHEDULED);
        Map<String, String> props = new HashMap<>();
        props.put(WorkflowEngineFactory.ENGINE_PROP, "oozie");
        WorkflowEngineFactory.getWorkflowEngine(process, props);
    }

    @Test
    public void testGetEngineForCluster() throws FalconException {
        AbstractWorkflowEngine engine =
                WorkflowEngineFactory.getWorkflowEngine(getStore().get(EntityType.CLUSTER, "testCluster"));
        Assert.assertTrue(engine instanceof OozieWorkflowEngine);
    }
}
