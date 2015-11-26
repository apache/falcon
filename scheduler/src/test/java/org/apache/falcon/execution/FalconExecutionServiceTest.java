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

import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.notification.service.NotificationServicesRegistry;
import org.apache.falcon.notification.service.event.DataEvent;
import org.apache.falcon.notification.service.event.Event;
import org.apache.falcon.notification.service.event.JobCompletedEvent;
import org.apache.falcon.notification.service.event.JobScheduledEvent;
import org.apache.falcon.notification.service.event.TimeElapsedEvent;
import org.apache.falcon.notification.service.impl.AlarmService;
import org.apache.falcon.notification.service.impl.DataAvailabilityService;
import org.apache.falcon.notification.service.impl.JobCompletionService;
import org.apache.falcon.notification.service.impl.SchedulerService;
import org.apache.falcon.service.Services;
import org.apache.falcon.state.AbstractSchedulerTestBase;
import org.apache.falcon.state.EntityClusterID;
import org.apache.falcon.state.EntityID;
import org.apache.falcon.state.EntityState;
import org.apache.falcon.state.ID;
import org.apache.falcon.state.InstanceID;
import org.apache.falcon.state.InstanceState;
import org.apache.falcon.state.store.AbstractStateStore;
import org.apache.falcon.state.store.StateStore;
import org.apache.falcon.state.store.service.FalconJPAService;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.engine.DAGEngine;
import org.apache.falcon.workflow.engine.DAGEngineFactory;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.joda.time.DateTime;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

/**
 * Tests the API of FalconExecution Service and in turn the FalconExecutionService.get()s.
 */
public class FalconExecutionServiceTest extends AbstractSchedulerTestBase {

    private StateStore stateStore = null;
    private AlarmService mockTimeService;
    private DataAvailabilityService mockDataService;
    private SchedulerService mockSchedulerService;
    private JobCompletionService mockCompletionService;
    private DAGEngine dagEngine;
    private int instanceCount = 0;
    private static FalconJPAService falconJPAService = FalconJPAService.get();

    @BeforeClass
    public void init() throws Exception {
        this.dfsCluster = EmbeddedCluster.newCluster("testCluster");
        this.conf = dfsCluster.getConf();
        setupServices();
        super.setup();
        createDB(DB_SQL_FILE);
        falconJPAService.init();
        setupConfigStore();
    }

    @AfterClass
    public void tearDown() throws FalconException, IOException {
        super.cleanup();
        this.dfsCluster.shutdown();
        falconJPAService.destroy();
    }

    // State store is set up to sync with Config Store. That gets tested too.
    public void setupConfigStore() throws Exception {
        stateStore = AbstractStateStore.get();
        getStore().registerListener(stateStore);
        storeEntity(EntityType.CLUSTER, "testCluster");
        storeEntity(EntityType.FEED, "clicksFeed");
        storeEntity(EntityType.FEED, "clicksSummary");
    }

    public void setupServices() throws FalconException, OozieClientException {
        mockTimeService = Mockito.mock(AlarmService.class);
        Mockito.when(mockTimeService.getName()).thenReturn("AlarmService");
        Mockito.when(mockTimeService.createRequestBuilder(Mockito.any(NotificationHandler.class),
                Mockito.any(ID.class))).thenCallRealMethod();

        mockDataService = Mockito.mock(DataAvailabilityService.class);
        Mockito.when(mockDataService.getName()).thenReturn("DataAvailabilityService");
        Mockito.when(mockDataService.createRequestBuilder(Mockito.any(NotificationHandler.class),
                Mockito.any(ID.class))).thenCallRealMethod();
        mockSchedulerService = Mockito.mock(SchedulerService.class);
        Mockito.when(mockSchedulerService.getName()).thenReturn("JobSchedulerService");
        StartupProperties.get().setProperty("dag.engine.impl", MockDAGEngine.class.getName());
        StartupProperties.get().setProperty("execution.service.impl", FalconExecutionService.class.getName());
        dagEngine = Mockito.spy(DAGEngineFactory.getDAGEngine("testCluster"));
        Mockito.when(mockSchedulerService.createRequestBuilder(Mockito.any(NotificationHandler.class),
                Mockito.any(ID.class))).thenCallRealMethod();
        mockCompletionService = Mockito.mock(JobCompletionService.class);
        Mockito.when(mockCompletionService.getName()).thenReturn("JobCompletionService");
        Mockito.when(mockCompletionService.createRequestBuilder(Mockito.any(NotificationHandler.class),
                Mockito.any(ID.class))).thenCallRealMethod();
        Services.get().register(mockTimeService);
        Services.get().register(mockDataService);
        Services.get().register(mockSchedulerService);
        Services.get().register(mockCompletionService);
        Services.get().register(FalconExecutionService.get());
    }

    @BeforeMethod
    private void setupStateStore() throws FalconException {
        stateStore.clear();
    }

    @Test
    public void testBasicFlow() throws Exception {
        storeEntity(EntityType.PROCESS, "summarize1");
        Process process = getStore().get(EntityType.PROCESS, "summarize1");
        Assert.assertNotNull(process);
        EntityID processKey = new EntityID(process);
        String clusterName = dfsCluster.getCluster().getName();

        // Schedule a process
        Assert.assertEquals(stateStore.getEntity(processKey).getCurrentState(), EntityState.STATE.SUBMITTED);
        FalconExecutionService.get().schedule(process);
        Assert.assertEquals(stateStore.getEntity(processKey).getCurrentState(), EntityState.STATE.SCHEDULED);

        // Simulate a time notification
        Event event = createEvent(NotificationServicesRegistry.SERVICE.TIME, process, clusterName);
        FalconExecutionService.get().onEvent(event);

        // Ensure an instance is triggered and registers for data notification
        Assert.assertEquals(stateStore.getAllExecutionInstances(process, clusterName).size(), 1);
        InstanceState instance = stateStore.getAllExecutionInstances(process, clusterName).iterator().next();
        Assert.assertEquals(instance.getCurrentState(), InstanceState.STATE.WAITING);

        // Simulate a data notification
        event = createEvent(NotificationServicesRegistry.SERVICE.DATA, instance.getInstance());
        FalconExecutionService.get().onEvent(event);

        // Ensure the instance is ready for execution
        instance = stateStore.getExecutionInstance(new InstanceID(instance.getInstance()));
        Assert.assertEquals(instance.getCurrentState(), InstanceState.STATE.READY);

        // Simulate a scheduled notification
        event = createEvent(NotificationServicesRegistry.SERVICE.JOB_SCHEDULE, instance.getInstance());
        FalconExecutionService.get().onEvent(event);

        // Ensure the instance is running
        instance = stateStore.getAllExecutionInstances(process, clusterName).iterator().next();
        Assert.assertEquals(instance.getCurrentState(), InstanceState.STATE.RUNNING);

        // Simulate a job complete notification
        event = createEvent(NotificationServicesRegistry.SERVICE.JOB_COMPLETION, instance.getInstance());
        FalconExecutionService.get().onEvent(event);

        // Ensure the instance is in succeeded and is in the state store
        instance = stateStore.getAllExecutionInstances(process, clusterName).iterator().next();
        Assert.assertEquals(instance.getCurrentState(), InstanceState.STATE.SUCCEEDED);
    }

    @Test
    public void testSuspendResume() throws Exception {
        Mockito.doNothing().when(dagEngine).resume(Mockito.any(ExecutionInstance.class));
        storeEntity(EntityType.PROCESS, "summarize2");
        Process process = getStore().get(EntityType.PROCESS, "summarize2");
        Assert.assertNotNull(process);
        String clusterName = dfsCluster.getCluster().getName();
        EntityID processID = new EntityID(process);
        EntityClusterID executorID = new EntityClusterID(process, clusterName);

        // Schedule a process
        Assert.assertEquals(stateStore.getEntity(processID).getCurrentState(),
                EntityState.STATE.SUBMITTED);
        FalconExecutionService.get().schedule(process);

        // Simulate two time notifications
        Event event = createEvent(NotificationServicesRegistry.SERVICE.TIME, process, clusterName);
        FalconExecutionService.get().onEvent(event);
        event = createEvent(NotificationServicesRegistry.SERVICE.TIME, process, clusterName);
        FalconExecutionService.get().onEvent(event);

        // Suspend and resume All waiting - check for notification deregistration

        Iterator i = stateStore.getAllExecutionInstances(process, clusterName).iterator();
        InstanceState instance1 = (InstanceState) i.next();
        InstanceState instance2 = (InstanceState) i.next();

        // Simulate a data notification
        event = createEvent(NotificationServicesRegistry.SERVICE.DATA, instance1.getInstance());
        FalconExecutionService.get().onEvent(event);

        // One in ready and one in waiting. Both should be suspended.
        instance1 = stateStore.getExecutionInstance(new InstanceID(instance1.getInstance()));
        Assert.assertEquals(instance1.getCurrentState(), InstanceState.STATE.READY);
        Assert.assertEquals(instance1.getInstance().getAwaitingPredicates().size(), 0);
        Assert.assertEquals(instance2.getCurrentState(), InstanceState.STATE.WAITING);

        FalconExecutionService.get().suspend(process);

        instance1 = stateStore.getExecutionInstance(new InstanceID(instance1.getInstance()));
        instance2 = stateStore.getExecutionInstance(new InstanceID(instance2.getInstance()));
        Assert.assertEquals(instance1.getCurrentState(), InstanceState.STATE.SUSPENDED);
        Assert.assertEquals(instance2.getCurrentState(), InstanceState.STATE.SUSPENDED);
        Mockito.verify(mockDataService).unregister(FalconExecutionService.get(),
                instance1.getInstance().getId());
        Mockito.verify(mockDataService).unregister(FalconExecutionService.get(),
                instance2.getInstance().getId());
        Mockito.verify(mockTimeService).unregister(FalconExecutionService.get(), executorID);

        Mockito.verify(mockDataService).unregister(FalconExecutionService.get(), executorID);

        FalconExecutionService.get().resume(process);
        instance1 = stateStore.getExecutionInstance(new InstanceID(instance1.getInstance()));
        instance2 = stateStore.getExecutionInstance(new InstanceID(instance2.getInstance()));
        Assert.assertEquals(instance1.getCurrentState(), InstanceState.STATE.READY);
        Assert.assertEquals(instance2.getCurrentState(), InstanceState.STATE.WAITING);

        // Simulate a data notification and a job run notification
        event = createEvent(NotificationServicesRegistry.SERVICE.DATA, instance2.getInstance());
        FalconExecutionService.get().onEvent(event);

        event = createEvent(NotificationServicesRegistry.SERVICE.JOB_SCHEDULE, instance1.getInstance());
        FalconExecutionService.get().onEvent(event);

        // One in running and the other in ready. Both should be suspended
        instance1 = stateStore.getExecutionInstance(new InstanceID(instance1.getInstance()));
        Assert.assertEquals(instance1.getCurrentState(), InstanceState.STATE.RUNNING);
        Mockito.when(dagEngine.isScheduled(instance1.getInstance())).thenReturn(true);
        instance2 = stateStore.getExecutionInstance(new InstanceID(instance2.getInstance()));
        Assert.assertEquals(instance2.getCurrentState(), InstanceState.STATE.READY);

        FalconExecutionService.get().suspend(process);

        instance1 = stateStore.getExecutionInstance(new InstanceID(instance1.getInstance()));
        instance2 = stateStore.getExecutionInstance(new InstanceID(instance2.getInstance()));
        Assert.assertEquals(instance1.getCurrentState(), InstanceState.STATE.SUSPENDED);
        Assert.assertEquals(instance2.getCurrentState(), InstanceState.STATE.SUSPENDED);

        FalconExecutionService.get().resume(process);
        instance1 = stateStore.getExecutionInstance(new InstanceID(instance1.getInstance()));
        instance2 = stateStore.getExecutionInstance(new InstanceID(instance2.getInstance()));
        Assert.assertEquals(instance1.getCurrentState(), InstanceState.STATE.RUNNING);
        Assert.assertEquals(instance2.getCurrentState(), InstanceState.STATE.READY);

        // Running should finish after resume
        event = createEvent(NotificationServicesRegistry.SERVICE.JOB_COMPLETION, instance1.getInstance());
        FalconExecutionService.get().onEvent(event);

        instance1 = stateStore.getExecutionInstance(new InstanceID(instance1.getInstance()));
        Assert.assertEquals(instance1.getCurrentState(), InstanceState.STATE.SUCCEEDED);
    }

    @Test
    // Kill waiting, ready, running - check for notification deregistration
    public void testDelete() throws Exception {
        storeEntity(EntityType.PROCESS, "summarize4");
        Process process = getStore().get(EntityType.PROCESS, "summarize4");
        Assert.assertNotNull(process);
        String clusterName = dfsCluster.getCluster().getName();
        EntityID processID = new EntityID(process);
        EntityClusterID executorID = new EntityClusterID(process, clusterName);
        // Schedule a process
        Assert.assertEquals(stateStore.getEntity(processID).getCurrentState(), EntityState.STATE.SUBMITTED);
        FalconExecutionService.get().schedule(process);

        // Simulate three time notifications
        Event event = createEvent(NotificationServicesRegistry.SERVICE.TIME, process, clusterName);
        FalconExecutionService.get().onEvent(event);
        event = createEvent(NotificationServicesRegistry.SERVICE.TIME, process, clusterName);
        FalconExecutionService.get().onEvent(event);
        event = createEvent(NotificationServicesRegistry.SERVICE.TIME, process, clusterName);
        FalconExecutionService.get().onEvent(event);

        // Suspend and resume All waiting - check for notification deregistration
        Iterator i = stateStore.getAllExecutionInstances(process, clusterName).iterator();
        InstanceState instance1 = (InstanceState) i.next();
        InstanceState instance2 = (InstanceState) i.next();
        InstanceState instance3 = (InstanceState) i.next();

        // Simulate two data notifications and one job run
        event = createEvent(NotificationServicesRegistry.SERVICE.DATA, instance1.getInstance());
        FalconExecutionService.get().onEvent(event);
        event = createEvent(NotificationServicesRegistry.SERVICE.DATA, instance2.getInstance());
        FalconExecutionService.get().onEvent(event);
        event = createEvent(NotificationServicesRegistry.SERVICE.JOB_SCHEDULE, instance1.getInstance());
        FalconExecutionService.get().onEvent(event);

        // One in ready, one in waiting and one running.
        instance1 = stateStore.getExecutionInstance(new InstanceID(instance1.getInstance()));
        instance2 = stateStore.getExecutionInstance(new InstanceID(instance2.getInstance()));
        instance3 = stateStore.getExecutionInstance(new InstanceID(instance3.getInstance()));
        Assert.assertEquals(instance1.getCurrentState(), InstanceState.STATE.RUNNING);
        Assert.assertEquals(instance2.getCurrentState(), InstanceState.STATE.READY);
        Assert.assertEquals(instance3.getCurrentState(), InstanceState.STATE.WAITING);

        FalconExecutionService.get().delete(process);

        // Deregister from notification services
        Mockito.verify(mockTimeService).unregister(FalconExecutionService.get(), executorID);
    }

    @Test
    public void testTimeOut() throws Exception {
        storeEntity(EntityType.PROCESS, "summarize3");
        Process process = getStore().get(EntityType.PROCESS, "summarize3");
        Assert.assertNotNull(process);
        String clusterName = dfsCluster.getCluster().getName();
        EntityID processID = new EntityID(process);

        // Schedule a process
        Assert.assertEquals(stateStore.getEntity(processID).getCurrentState(), EntityState.STATE.SUBMITTED);
        FalconExecutionService.get().schedule(process);

        // Simulate time notification
        Event event = createEvent(NotificationServicesRegistry.SERVICE.TIME, process, clusterName);
        FalconExecutionService.get().onEvent(event);

        // Simulate data unavailable notification and timeout
        InstanceState instanceState = stateStore.getAllExecutionInstances(process, clusterName).iterator().next();
        ((Process) instanceState.getInstance().getEntity()).setTimeout(new Frequency("minutes(0)"));
        DataEvent dataEvent = (DataEvent) createEvent(NotificationServicesRegistry.SERVICE.DATA,
                instanceState.getInstance());
        dataEvent.setStatus(DataEvent.STATUS.UNAVAILABLE);

        FalconExecutionService.get().onEvent(dataEvent);

        instanceState = stateStore.getExecutionInstance(new InstanceID(instanceState.getInstance()));
        Assert.assertEquals(instanceState.getCurrentState(), InstanceState.STATE.TIMED_OUT);
    }

    // Non-triggering event should not create an instance
    @Test
    public void testNonTriggeringEvents() throws Exception {
        storeEntity(EntityType.PROCESS, "summarize6");
        Process process = getStore().get(EntityType.PROCESS, "summarize6");
        Assert.assertNotNull(process);
        String clusterName = dfsCluster.getCluster().getName();
        EntityID processID = new EntityID(process);

        // Schedule a process
        Assert.assertEquals(stateStore.getEntity(processID).getCurrentState(), EntityState.STATE.SUBMITTED);
        FalconExecutionService.get().schedule(process);
        Assert.assertEquals(stateStore.getEntity(processID).getCurrentState(), EntityState.STATE.SCHEDULED);

        // Simulate data notification with a callback of the process.
        Event event = createEvent(NotificationServicesRegistry.SERVICE.DATA, process, clusterName);
        FalconExecutionService.get().onEvent(event);

        // No instances should get triggered
        Assert.assertTrue(stateStore.getAllExecutionInstances(process, clusterName).isEmpty());
    }

    // Individual instance suspend, resume, kill
    @Test
    public void testInstanceOperations() throws Exception {
        storeEntity(EntityType.PROCESS, "summarize5");
        Process process = getStore().get(EntityType.PROCESS, "summarize5");
        Assert.assertNotNull(process);
        String clusterName = dfsCluster.getCluster().getName();
        EntityID processID = new EntityID(process);

        // Schedule a process
        Assert.assertEquals(stateStore.getEntity(processID).getCurrentState(), EntityState.STATE.SUBMITTED);
        FalconExecutionService.get().schedule(process);

        // Simulate three time notifications
        Event event = createEvent(NotificationServicesRegistry.SERVICE.TIME, process, clusterName);
        FalconExecutionService.get().onEvent(event);
        event = createEvent(NotificationServicesRegistry.SERVICE.TIME, process, clusterName);
        FalconExecutionService.get().onEvent(event);
        event = createEvent(NotificationServicesRegistry.SERVICE.TIME, process, clusterName);
        FalconExecutionService.get().onEvent(event);

        // Suspend and resume All waiting - check for notification deregistration
        Iterator i = stateStore.getAllExecutionInstances(process, clusterName).iterator();
        InstanceState instance1 = (InstanceState) i.next();
        InstanceState instance2 = (InstanceState) i.next();
        InstanceState instance3 = (InstanceState) i.next();

        // Simulate two data notifications and one job run
        event = createEvent(NotificationServicesRegistry.SERVICE.DATA, instance1.getInstance());
        FalconExecutionService.get().onEvent(event);
        event = createEvent(NotificationServicesRegistry.SERVICE.DATA, instance2.getInstance());
        FalconExecutionService.get().onEvent(event);
        event = createEvent(NotificationServicesRegistry.SERVICE.JOB_SCHEDULE, instance1.getInstance());
        FalconExecutionService.get().onEvent(event);

        // One in ready, one in waiting and one running.
        instance1 = stateStore.getExecutionInstance(new InstanceID(instance1.getInstance()));
        instance2 = stateStore.getExecutionInstance(new InstanceID(instance2.getInstance()));
        instance3 = stateStore.getExecutionInstance(new InstanceID(instance3.getInstance()));
        Assert.assertEquals(instance1.getCurrentState(), InstanceState.STATE.RUNNING);
        Assert.assertEquals(instance2.getCurrentState(), InstanceState.STATE.READY);
        Assert.assertEquals(instance3.getCurrentState(), InstanceState.STATE.WAITING);

        EntityExecutor entityExecutor = FalconExecutionService.get().getEntityExecutor(process, clusterName);
        Assert.assertNotNull(entityExecutor);
        Collection<ExecutionInstance> instances = new ArrayList<>();
        for (InstanceState instanceState : stateStore.getAllExecutionInstances(process, clusterName)) {
            instances.add(instanceState.getInstance());
        }

        for (ExecutionInstance instance : instances) {
            entityExecutor.suspend(instance);
        }

        // Instances must be suspended, but, entity itself must not be.
        for (InstanceState instanceState : stateStore.getAllExecutionInstances(process, clusterName)) {
            Assert.assertEquals(instanceState.getCurrentState(), InstanceState.STATE.SUSPENDED);
            Mockito.verify(mockDataService).unregister(FalconExecutionService.get(),
                    instanceState.getInstance().getId());
        }
        Assert.assertEquals(stateStore.getEntity(processID).getCurrentState(), EntityState.STATE.SCHEDULED);

        for (ExecutionInstance instance : instances) {
            entityExecutor.resume(instance);
        }
        // Back to one in ready, one in waiting and one running.
        Assert.assertEquals(instance1.getCurrentState(), InstanceState.STATE.RUNNING);
        Assert.assertEquals(instance2.getCurrentState(), InstanceState.STATE.READY);
        Assert.assertEquals(instance3.getCurrentState(), InstanceState.STATE.WAITING);

        for (ExecutionInstance instance : instances) {
            entityExecutor.kill(instance);
        }

        // Instances must be killed, but, entity itself must not be.
        for (InstanceState instanceState : stateStore.getAllExecutionInstances(process, clusterName)) {
            Assert.assertEquals(instanceState.getCurrentState(), InstanceState.STATE.KILLED);
        }
        Assert.assertEquals(stateStore.getEntity(processID).getCurrentState(), EntityState.STATE.SCHEDULED);
    }

    @Test(priority = -1)
    // Add some entities and instance in the store and ensure the executor picks up from where it left
    public void testSystemRestart() throws Exception {
        storeEntity(EntityType.PROCESS, "summarize7");
        Process process = getStore().get(EntityType.PROCESS, "summarize7");
        Assert.assertNotNull(process);
        String clusterName = dfsCluster.getCluster().getName();
        EntityID processID = new EntityID(process);

        // Store couple of instances in store
        EntityState entityState = stateStore.getEntity(processID);
        entityState.setCurrentState(EntityState.STATE.SCHEDULED);
        stateStore.updateEntity(entityState);
        ProcessExecutionInstance instance1 = new ProcessExecutionInstance(process,
                new DateTime(System.currentTimeMillis() - 60 * 60 * 1000), clusterName);
        InstanceState instanceState1 = new InstanceState(instance1);
        instanceState1.setCurrentState(InstanceState.STATE.RUNNING);
        stateStore.putExecutionInstance(instanceState1);
        ProcessExecutionInstance instance2 = new ProcessExecutionInstance(process,
                new DateTime(System.currentTimeMillis() - 30 * 60 * 1000), clusterName);
        InstanceState instanceState2 = new InstanceState(instance2);
        instanceState2.setCurrentState(InstanceState.STATE.READY);
        stateStore.putExecutionInstance(instanceState2);

        FalconExecutionService.get().init();

        // Simulate a scheduled notification. This should cause the reload from state store
        Event event = createEvent(NotificationServicesRegistry.SERVICE.JOB_SCHEDULE, instanceState2.getInstance());
        FalconExecutionService.get().onEvent(event);
        instanceState2 = stateStore.getExecutionInstance(new InstanceID(instanceState2.getInstance()));
        Assert.assertEquals(instanceState2.getCurrentState(), InstanceState.STATE.RUNNING);

        // Simulate a Job completion notification and ensure the instance resumes from where it left
        event = createEvent(NotificationServicesRegistry.SERVICE.JOB_COMPLETION, instanceState1.getInstance());
        FalconExecutionService.get().onEvent(event);
        instanceState1 = stateStore.getExecutionInstance(new InstanceID(instanceState1.getInstance()));
        Assert.assertEquals(instanceState1.getCurrentState(), InstanceState.STATE.SUCCEEDED);
    }

    @Test(dataProvider = "actions")
    public void testPartialFailures(String name, String action, InstanceState.STATE state) throws Exception {
        storeEntity(EntityType.PROCESS, name);
        Process process = getStore().get(EntityType.PROCESS, name);
        Assert.assertNotNull(process);
        String clusterName = dfsCluster.getCluster().getName();
        EntityID processID = new EntityID(process);

        // Schedule the process
        FalconExecutionService.get().schedule(process);

        // Store couple of instances in store
        stateStore.getEntity(processID).setCurrentState(EntityState.STATE.SCHEDULED);
        ProcessExecutionInstance instance1 = new ProcessExecutionInstance(process,
                new DateTime(System.currentTimeMillis() - 60 * 60 * 1000), clusterName);
        instance1.setExternalID("123");
        InstanceState instanceState1 = new InstanceState(instance1);
        instanceState1.setCurrentState(InstanceState.STATE.RUNNING);
        stateStore.putExecutionInstance(instanceState1);
        ProcessExecutionInstance instance2 = new ProcessExecutionInstance(process,
                new DateTime(System.currentTimeMillis() - 30 * 60 * 1000), clusterName);
        InstanceState instanceState2 = new InstanceState(instance2);
        instanceState2.setCurrentState(InstanceState.STATE.READY);
        stateStore.putExecutionInstance(instanceState2);

        // Mock failure
        ((MockDAGEngine)dagEngine).addFailInstance(instance1);
        Method m = FalconExecutionService.get().getClass().getMethod(action, Entity.class);
        try {
            m.invoke(FalconExecutionService.get(), process);
            Assert.fail("Exception expected.");
        } catch (Exception e) {
            // One instance must fail and the other not
            instanceState1 = stateStore.getExecutionInstance(new InstanceID(instanceState1.getInstance()));
            instanceState2 = stateStore.getExecutionInstance(new InstanceID(instanceState2.getInstance()));
            Assert.assertEquals(instanceState2.getCurrentState(), state);
            Assert.assertEquals(instanceState1.getCurrentState(), InstanceState.STATE.RUNNING);
        }

        // throw no exception
        ((MockDAGEngine)dagEngine).removeFailInstance(instance1);
        m.invoke(FalconExecutionService.get(), process);

        instanceState1 = stateStore.getExecutionInstance(new InstanceID(instanceState1.getInstance()));
        instanceState2 = stateStore.getExecutionInstance(new InstanceID(instanceState2.getInstance()));
        // Both instances must be in expected state.
        Assert.assertEquals(instanceState2.getCurrentState(), state);
        Assert.assertEquals(instanceState1.getCurrentState(), state);
    }

    @DataProvider(name = "actions")
    public Object[][] testActions() {
        return new Object[][] {
            {"summarize8", "suspend", InstanceState.STATE.SUSPENDED},
            {"summarize9", "delete", InstanceState.STATE.KILLED},
        };
    }

    private Event createEvent(NotificationServicesRegistry.SERVICE type, Process process, String cluster) {
        EntityClusterID id = new EntityClusterID(process, cluster);
        switch (type) {
        case TIME:
            Date start = process.getClusters().getClusters().get(0).getValidity().getStart();
            long instanceOffset =
                    SchedulerUtil.getFrequencyInMillis(DateTime.now(), process.getFrequency()) * instanceCount++;
            return new TimeElapsedEvent(id, new DateTime(start),
                    new DateTime(process.getClusters().getClusters().get(0).getValidity().getEnd()),
                    new DateTime(start.getTime() + instanceOffset));
        case DATA:
            DataEvent dataEvent = new DataEvent(id, new Path("/projects/falcon/clicks"), LocationType.DATA,
                    DataEvent.STATUS.AVAILABLE);
            return dataEvent;
        default:
            return null;
        }
    }

    private Event createEvent(NotificationServicesRegistry.SERVICE type, ExecutionInstance instance) {
        ID id = new InstanceID(instance);
        switch (type) {
        case DATA:
            DataEvent dataEvent = new DataEvent(id, new Path("/projects/falcon/clicks"), LocationType.DATA,
                    DataEvent.STATUS.AVAILABLE);
            return dataEvent;
        case JOB_SCHEDULE:
            JobScheduledEvent scheduledEvent = new JobScheduledEvent(id, JobScheduledEvent.STATUS.SUCCESSFUL);
            scheduledEvent.setExternalID("234");
            return scheduledEvent;
        case JOB_COMPLETION:
            JobCompletedEvent jobEvent = new JobCompletedEvent(id, WorkflowJob.Status.SUCCEEDED, DateTime.now());
            return jobEvent;
        default:
            return null;
        }
    }

}
