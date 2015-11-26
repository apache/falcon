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
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.exception.InvalidStateTransitionException;
import org.apache.falcon.exception.StateStoreException;
import org.apache.falcon.execution.ProcessExecutionInstance;
import org.apache.falcon.state.store.AbstractStateStore;
import org.apache.falcon.util.StartupProperties;
import org.joda.time.DateTime;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests the state changes of an instance.
 */
public class InstanceStateServiceTest {

    private InstanceStateChangeHandler listener = Mockito.mock(InstanceStateChangeHandler.class);
    private ProcessExecutionInstance mockInstance;

    @BeforeClass
    public void init() {
        StartupProperties.get().setProperty("falcon.state.store.impl",
                "org.apache.falcon.state.store.InMemoryStateStore");
    }

    @BeforeMethod
    public void setup() {
        Process testProcess = new Process();
        testProcess.setName("test");
        // Setup new mocks so we can verify the no. of invocations
        mockInstance = Mockito.mock(ProcessExecutionInstance.class);
        Mockito.when(mockInstance.getEntity()).thenReturn(testProcess);
        Mockito.when(mockInstance.getCreationTime()).thenReturn(DateTime.now());
        Mockito.when(mockInstance.getInstanceTime()).thenReturn(DateTime.now());
        Mockito.when(mockInstance.getCluster()).thenReturn("testCluster");
    }

    @AfterMethod
    public void tearDown() throws StateStoreException {
        AbstractStateStore.get().clear();
    }

    // Tests an entity instance's lifecycle : Trigger -> waiting -> ready -> running
    // -> suspendAll -> resumeAll -> success
    @Test
    public void testLifeCycle() throws FalconException {
        StateService.get().handleStateChange(mockInstance, InstanceState.EVENT.TRIGGER, listener);
        InstanceState instanceFromStore = AbstractStateStore.get()
                .getExecutionInstance(new InstanceID(mockInstance));
        Mockito.verify(listener).onTrigger(mockInstance);
        Assert.assertTrue(instanceFromStore.getCurrentState().equals(InstanceState.STATE.WAITING));
        StateService.get().handleStateChange(mockInstance, InstanceState.EVENT.CONDITIONS_MET, listener);
        Mockito.verify(listener).onConditionsMet(mockInstance);
        instanceFromStore = AbstractStateStore.get()
                .getExecutionInstance(new InstanceID(mockInstance));
        Assert.assertTrue(instanceFromStore.getCurrentState().equals(InstanceState.STATE.READY));
        StateService.get().handleStateChange(mockInstance, InstanceState.EVENT.SCHEDULE, listener);
        Mockito.verify(listener).onSchedule(mockInstance);
        instanceFromStore = AbstractStateStore.get()
                .getExecutionInstance(new InstanceID(mockInstance));
        Assert.assertTrue(instanceFromStore.getCurrentState().equals(InstanceState.STATE.RUNNING));
        StateService.get().handleStateChange(mockInstance, InstanceState.EVENT.SUSPEND, listener);
        Mockito.verify(listener).onSuspend(mockInstance);
        instanceFromStore = AbstractStateStore.get()
                .getExecutionInstance(new InstanceID(mockInstance));
        Assert.assertTrue(instanceFromStore.getCurrentState().equals(InstanceState.STATE.SUSPENDED));
        StateService.get().handleStateChange(mockInstance, InstanceState.EVENT.RESUME_RUNNING, listener);
        Mockito.verify(listener).onResume(mockInstance);
        instanceFromStore = AbstractStateStore.get()
                .getExecutionInstance(new InstanceID(mockInstance));
        Assert.assertTrue(instanceFromStore.getCurrentState().equals(InstanceState.STATE.RUNNING));
        StateService.get().handleStateChange(mockInstance, InstanceState.EVENT.SUCCEED, listener);
        Mockito.verify(listener).onSuccess(mockInstance);
        instanceFromStore = AbstractStateStore.get()
                .getExecutionInstance(new InstanceID(mockInstance));
        Assert.assertTrue(instanceFromStore.getCurrentState().equals(InstanceState.STATE.SUCCEEDED));
        Assert.assertEquals(AbstractStateStore.get().getAllEntities().size(), 0);
    }

    @Test
    public void testInvalidTransitions() throws FalconException {
        StateService.get().handleStateChange(mockInstance, InstanceState.EVENT.TRIGGER, listener);
        try {
            StateService.get().handleStateChange(mockInstance, InstanceState.EVENT.SCHEDULE, listener);
            Assert.fail("Exception expected");
        } catch (InvalidStateTransitionException e) {
            // Do nothing
        }

        // Resume an instance that is not suspended
        StateService.get().handleStateChange(mockInstance, InstanceState.EVENT.CONDITIONS_MET, listener);
        try {
            StateService.get().handleStateChange(mockInstance, InstanceState.EVENT.RESUME_READY, listener);
            Assert.fail("Exception expected");
        } catch (InvalidStateTransitionException e) {
            // Do nothing
        }

        StateService.get().handleStateChange(mockInstance, InstanceState.EVENT.SCHEDULE, listener);
        StateService.get().handleStateChange(mockInstance, InstanceState.EVENT.FAIL, listener);

        // Attempt killing a completed instance
        try {
            StateService.get().handleStateChange(mockInstance, InstanceState.EVENT.KILL, listener);
            Assert.fail("Exception expected");
        } catch (InvalidStateTransitionException e) {
            // Do nothing
        }
    }

    @Test(dataProvider = "state_and_events")
    public void testIdempotency(InstanceState.STATE state, InstanceState.EVENT event)
        throws InvalidStateTransitionException, StateStoreException {
        InstanceState instanceState = new InstanceState(mockInstance).setCurrentState(state);
        instanceState.nextTransition(event);
        Assert.assertEquals(instanceState.getCurrentState(), state);
    }

    @DataProvider(name = "state_and_events")
    public Object[][] stateAndEvents() {
        return new Object[][] {
            {InstanceState.STATE.WAITING, InstanceState.EVENT.TRIGGER},
            {InstanceState.STATE.READY, InstanceState.EVENT.CONDITIONS_MET},
            {InstanceState.STATE.TIMED_OUT, InstanceState.EVENT.TIME_OUT},
            {InstanceState.STATE.RUNNING, InstanceState.EVENT.SCHEDULE},
            {InstanceState.STATE.SUSPENDED, InstanceState.EVENT.SUSPEND},
            {InstanceState.STATE.KILLED, InstanceState.EVENT.KILL},
            {InstanceState.STATE.SUCCEEDED, InstanceState.EVENT.SUCCEED},
            {InstanceState.STATE.FAILED, InstanceState.EVENT.FAIL},
        };
    }
}
