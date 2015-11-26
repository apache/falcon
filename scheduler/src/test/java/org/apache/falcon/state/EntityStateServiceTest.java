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
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.exception.InvalidStateTransitionException;
import org.apache.falcon.exception.StateStoreException;
import org.apache.falcon.state.store.AbstractStateStore;
import org.apache.falcon.util.StartupProperties;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests to ensure entity state changes happen correctly.
 */
public class EntityStateServiceTest extends AbstractSchedulerTestBase{

    private EntityStateChangeHandler listener = Mockito.mock(EntityStateChangeHandler.class);

    @BeforeClass
    public void setup() throws Exception {
        StartupProperties.get().setProperty("falcon.state.store.impl",
                "org.apache.falcon.state.store.InMemoryStateStore");
        super.setup();
        this.dfsCluster = EmbeddedCluster.newCluster("testCluster");
        this.conf = dfsCluster.getConf();
    }

    @AfterMethod
    public void setUp() throws StateStoreException {
        AbstractStateStore.get().clear();
    }

    // Tests a schedulable entity's lifecycle : Submit -> run -> suspend -> resume
    @Test
    public void testLifeCycle() throws Exception {
        Process mockEntity = new Process();
        mockEntity.setName("test");
        storeEntity(EntityType.PROCESS, "test");
        StateService.get().handleStateChange(mockEntity, EntityState.EVENT.SUBMIT, listener);
        EntityState entityFromStore = AbstractStateStore.get().getAllEntities().iterator().next();
        Mockito.verify(listener).onSubmit(mockEntity);
        Assert.assertTrue(entityFromStore.getCurrentState().equals(EntityState.STATE.SUBMITTED));
        StateService.get().handleStateChange(mockEntity, EntityState.EVENT.SCHEDULE, listener);
        Mockito.verify(listener).onSchedule(mockEntity);
        entityFromStore = AbstractStateStore.get().getAllEntities().iterator().next();
        Assert.assertTrue(entityFromStore.getCurrentState().equals(EntityState.STATE.SCHEDULED));
        StateService.get().handleStateChange(mockEntity, EntityState.EVENT.SUSPEND, listener);
        Mockito.verify(listener).onSuspend(mockEntity);
        entityFromStore = AbstractStateStore.get().getAllEntities().iterator().next();
        Assert.assertTrue(entityFromStore.getCurrentState().equals(EntityState.STATE.SUSPENDED));
        StateService.get().handleStateChange(mockEntity, EntityState.EVENT.RESUME, listener);
        Mockito.verify(listener).onResume(mockEntity);
        entityFromStore = AbstractStateStore.get().getAllEntities().iterator().next();
        Assert.assertTrue(entityFromStore.getCurrentState().equals(EntityState.STATE.SCHEDULED));
    }

    @Test
    public void testInvalidTransitions() throws Exception {
        Feed mockEntity = new Feed();
        mockEntity.setName("test");
        storeEntity(EntityType.FEED, "test");
        StateService.get().handleStateChange(mockEntity, EntityState.EVENT.SUBMIT, listener);
        // Attempt suspending a submitted entity
        try {
            StateService.get().handleStateChange(mockEntity, EntityState.EVENT.SUSPEND, listener);
            Assert.fail("Exception expected");
        } catch (InvalidStateTransitionException e) {
            // Do nothing
        }

        StateService.get().handleStateChange(mockEntity, EntityState.EVENT.SCHEDULE, listener);
        // Attempt resuming a scheduled entity
        try {
            StateService.get().handleStateChange(mockEntity, EntityState.EVENT.RESUME, listener);
            Assert.fail("Exception expected");
        } catch (InvalidStateTransitionException e) {
            // Do nothing
        }

        // Attempt scheduling a cluster
        Cluster mockCluster = new Cluster();
        mockCluster.setName("test");
        StateService.get().handleStateChange(mockCluster, EntityState.EVENT.SUBMIT, listener);
        try {
            StateService.get().handleStateChange(mockCluster, EntityState.EVENT.SCHEDULE, listener);
            Assert.fail("Exception expected");
        } catch (FalconException e) {
            // Do nothing
        }
    }

    @Test(dataProvider = "state_and_events")
    public void testIdempotency(EntityState.STATE state, EntityState.EVENT event)
        throws Exception {
        Process mockEntity = new Process();
        mockEntity.setName("test");
        storeEntity(EntityType.PROCESS, "test");
        EntityState entityState = new EntityState(mockEntity).setCurrentState(state);
        entityState.nextTransition(event);
        Assert.assertEquals(entityState.getCurrentState(), state);
    }

    @DataProvider(name = "state_and_events")
    public Object[][] stateAndEvents() {
        return new Object[][]{
            {EntityState.STATE.SCHEDULED, EntityState.EVENT.SCHEDULE},
            {EntityState.STATE.SUBMITTED, EntityState.EVENT.SUBMIT},
            {EntityState.STATE.SUSPENDED, EntityState.EVENT.SUSPEND},
        };
    }
}
