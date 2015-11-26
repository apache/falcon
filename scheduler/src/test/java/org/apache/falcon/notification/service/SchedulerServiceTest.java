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
package org.apache.falcon.notification.service;

import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.AbstractTestBase;
import org.apache.falcon.entity.EntityUtil;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Property;
import org.apache.falcon.execution.ExecutionInstance;
import org.apache.falcon.execution.MockDAGEngine;
import org.apache.falcon.execution.NotificationHandler;
import org.apache.falcon.execution.ProcessExecutionInstance;
import org.apache.falcon.notification.service.event.Event;
import org.apache.falcon.notification.service.event.JobCompletedEvent;
import org.apache.falcon.notification.service.event.JobScheduledEvent;
import org.apache.falcon.notification.service.impl.DataAvailabilityService;
import org.apache.falcon.notification.service.impl.JobCompletionService;
import org.apache.falcon.notification.service.impl.SchedulerService;
import org.apache.falcon.service.Services;
import org.apache.falcon.state.EntityClusterID;
import org.apache.falcon.state.ID;
import org.apache.falcon.state.InstanceID;
import org.apache.falcon.state.InstanceState;
import org.apache.falcon.state.store.AbstractStateStore;
import org.apache.falcon.state.store.StateStore;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.engine.DAGEngine;
import org.apache.falcon.workflow.engine.DAGEngineFactory;
import org.apache.oozie.client.WorkflowJob;
import org.joda.time.DateTime;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Date;

import static org.apache.falcon.state.InstanceState.STATE;

/**
 * Class to test the job scheduler service.
 */
public class SchedulerServiceTest extends AbstractTestBase {

    private SchedulerService scheduler;
    private NotificationHandler handler;
    private static String cluster = "testCluster";
    private static StateStore stateStore;
    private static DAGEngine mockDagEngine;
    private static Process process;
    private volatile boolean failed = false;

    @BeforeMethod
    public void setup() throws FalconException {
        // Initialized before every test in order to track invocations.
        handler = Mockito.spy(new MockNotificationHandler());
    }

    @BeforeClass
    public void init() throws Exception {
        StartupProperties.get().setProperty("falcon.state.store.impl",
                "org.apache.falcon.state.store.InMemoryStateStore");
        stateStore = AbstractStateStore.get();
        scheduler = Mockito.spy(new SchedulerService());
        this.dfsCluster = EmbeddedCluster.newCluster(cluster);
        this.conf = dfsCluster.getConf();
        setupConfigStore();
        DataAvailabilityService mockDataService = Mockito.mock(DataAvailabilityService.class);
        Mockito.when(mockDataService.getName()).thenReturn("DataAvailabilityService");
        Mockito.when(mockDataService.createRequestBuilder(Mockito.any(NotificationHandler.class),
                Mockito.any(ID.class))).thenCallRealMethod();
        Services.get().register(mockDataService);
        JobCompletionService mockCompletionService = Mockito.mock(JobCompletionService.class);
        Mockito.when(mockCompletionService.getName()).thenReturn("JobCompletionService");
        Mockito.when(mockCompletionService.createRequestBuilder(Mockito.any(NotificationHandler.class),
                Mockito.any(ID.class))).thenCallRealMethod();
        Services.get().register(mockCompletionService);

        Services.get().register(scheduler);
        scheduler.init();
        StartupProperties.get().setProperty("dag.engine.impl", MockDAGEngine.class.getName());
        mockDagEngine =  DAGEngineFactory.getDAGEngine("testCluster");

    }

    @AfterClass
    public void tearDown() {
        this.dfsCluster.shutdown();
    }

    // State store is set up to sync with Config Store. That gets tested too.
    public void setupConfigStore() throws Exception {
        getStore().registerListener(stateStore);
        storeEntity(EntityType.CLUSTER, "testCluster");
        storeEntity(EntityType.FEED, "clicksFeed");
        storeEntity(EntityType.FEED, "clicksSummary");
        storeEntity(EntityType.PROCESS, "mockSummary");
        process = getStore().get(EntityType.PROCESS, "mockSummary");
    }

    @Test
    public void testSchedulingWithParallelInstances() throws Exception {
        storeEntity(EntityType.PROCESS, "summarize");
        Process mockProcess = getStore().get(EntityType.PROCESS, "summarize");
        mockProcess.setParallel(1);
        Date startTime = EntityUtil.getStartTime(mockProcess, cluster);
        ExecutionInstance instance1 = new ProcessExecutionInstance(mockProcess, new DateTime(startTime), cluster);
        SchedulerService.JobScheduleRequestBuilder request = (SchedulerService.JobScheduleRequestBuilder)
                scheduler.createRequestBuilder(handler, instance1.getId());
        request.setInstance(instance1);
        // No instances running, ensure DAGEngine.run is invoked.
        scheduler.register(request.build());
        Thread.sleep(100);
        Assert.assertEquals(((MockDAGEngine) mockDagEngine).getTotalRuns(instance1), new Integer(1));
        Mockito.verify(handler).onEvent(Mockito.any(JobScheduledEvent.class));
        // Max. instances running, the new instance should not be run.
        ExecutionInstance instance2 = new ProcessExecutionInstance(mockProcess,
                new DateTime(startTime.getTime() + 60000), cluster);
        SchedulerService.JobScheduleRequestBuilder request2 = (SchedulerService.JobScheduleRequestBuilder)
                scheduler.createRequestBuilder(handler, instance2.getId());
        request2.setInstance(instance2);
        scheduler.register(request2.build());
        Thread.sleep(100);
        Assert.assertEquals(((MockDAGEngine) mockDagEngine).getTotalRuns(instance2), null);
        // Max. instances running, the new instance should not be run.
        ExecutionInstance instance3 = new ProcessExecutionInstance(mockProcess,
                new DateTime(startTime.getTime() + 120000), cluster);
        SchedulerService.JobScheduleRequestBuilder request3 = (SchedulerService.JobScheduleRequestBuilder)
                scheduler.createRequestBuilder(handler, instance3.getId());
        request3.setInstance(instance3);
        scheduler.register(request3.build());
        Thread.sleep(100);
        Assert.assertEquals(((MockDAGEngine) mockDagEngine).getTotalRuns(instance1), new Integer(1));
        // Simulate the completion of previous instance.
        stateStore.getExecutionInstance(instance1.getId()).setCurrentState(STATE.SUCCEEDED);
        scheduler.onEvent(new JobCompletedEvent(new EntityClusterID(mockProcess, cluster), WorkflowJob.Status.SUCCEEDED,
                DateTime.now()));
        // When an instance completes instance2 should get scheduled next iteration
        Thread.sleep(300);
        Assert.assertEquals(((MockDAGEngine) mockDagEngine).getTotalRuns(instance2), new Integer(1));
        // Simulate another completion and ensure instance3 runs.
        stateStore.getExecutionInstance(instance2.getId()).setCurrentState(STATE.SUCCEEDED);
        scheduler.onEvent(new JobCompletedEvent(new EntityClusterID(mockProcess, cluster), WorkflowJob.Status.SUCCEEDED,
                DateTime.now()));
        Thread.sleep(300);
        Assert.assertEquals(((MockDAGEngine) mockDagEngine).getTotalRuns(instance3), new Integer(1));
    }

    @Test
    public void testSchedulingWithDependencies() throws Exception {
        storeEntity(EntityType.PROCESS, "summarize1");
        Process mockProcess = getStore().get(EntityType.PROCESS, "summarize1");
        Date startTime = EntityUtil.getStartTime(mockProcess, cluster);
        // Create two instances, one dependent on the other.
        ExecutionInstance instance1 = new ProcessExecutionInstance(mockProcess, new DateTime(startTime), cluster);
        ExecutionInstance instance2 = new ProcessExecutionInstance(mockProcess,
                new DateTime(startTime.getTime() + 60000), cluster);
        SchedulerService.JobScheduleRequestBuilder request = (SchedulerService.JobScheduleRequestBuilder)
                scheduler.createRequestBuilder(handler, instance1.getId());
        ArrayList<ExecutionInstance> dependencies = new ArrayList<ExecutionInstance>();
        dependencies.add(instance2);
        request.setDependencies(dependencies);
        request.setInstance(instance1);
        // instance2 is not scheduled, dependency not satisfied
        // So, instance1 should not be scheduled either.
        scheduler.register(request.build());
        Thread.sleep(100);
        Assert.assertEquals(((MockDAGEngine) mockDagEngine).getTotalRuns(instance1), null);
        Mockito.verify(handler, Mockito.times(0)).onEvent(Mockito.any(JobScheduledEvent.class));

        // Schedule instance1
        SchedulerService.JobScheduleRequestBuilder request2 = (SchedulerService.JobScheduleRequestBuilder)
                scheduler.createRequestBuilder(handler, instance2.getId());
        request2.setInstance(instance2);
        scheduler.register(request2.build());

        // Simulate completion of the instance.
        Thread.sleep(100);
        Assert.assertEquals(((MockDAGEngine) mockDagEngine).getTotalRuns(instance2), new Integer(1));
        Mockito.verify(handler, Mockito.times(1)).onEvent(Mockito.any(JobScheduledEvent.class));
        stateStore.getExecutionInstance(instance2.getId()).setCurrentState(STATE.SUCCEEDED);
        scheduler.onEvent(new JobCompletedEvent(new InstanceID(mockProcess, cluster, instance2.getInstanceTime()),
                WorkflowJob.Status.SUCCEEDED, DateTime.now()));
        // Dependency now satisfied. Now, the first instance should get scheduled after retry delay.
        Thread.sleep(100);
        Assert.assertEquals(((MockDAGEngine) mockDagEngine).getTotalRuns(instance2), new Integer(1));
    }

    @Test
    public void testSchedulingWithPriorities() throws Exception {
        storeEntity(EntityType.PROCESS, "summarize2");
        Process mockProcess = getStore().get(EntityType.PROCESS, "summarize2");
        storeEntity(EntityType.PROCESS, "summarize3");
        Process mockProcess2 = getStore().get(EntityType.PROCESS, "summarize3");
        // Set higher priority
        Property priorityProp = new Property();
        priorityProp.setName(EntityUtil.MR_JOB_PRIORITY);
        priorityProp.setValue("HIGH");
        mockProcess2.getProperties().getProperties().add(priorityProp);
        Date startTime = EntityUtil.getStartTime(mockProcess, cluster);
        // Create two one dependent on the other.
        ExecutionInstance instance1 = new ProcessExecutionInstance(mockProcess, new DateTime(startTime), cluster);
        ExecutionInstance instance2 = new ProcessExecutionInstance(mockProcess2, new DateTime(startTime), cluster);
        SchedulerService.JobScheduleRequestBuilder request = (SchedulerService.JobScheduleRequestBuilder)
                scheduler.createRequestBuilder(handler, instance1.getId());
        request.setInstance(instance1);
        SchedulerService.JobScheduleRequestBuilder request2 = (SchedulerService.JobScheduleRequestBuilder)
                scheduler.createRequestBuilder(handler, instance2.getId());
        request2.setInstance(instance2);
        scheduler.register(request.build());
        scheduler.register(request2.build());
        Thread.sleep(100);
        // Instance2 should be scheduled first.
        Assert.assertEquals(((MockDAGEngine) mockDagEngine).getTotalRuns(instance2), new Integer(1));
        Assert.assertEquals(((MockDAGEngine) mockDagEngine).getTotalRuns(instance1), new Integer(1));
        Mockito.verify(handler, Mockito.times(2)).onEvent(Mockito.any(JobScheduledEvent.class));
    }

    @Test
    public void testDeRegistration() throws Exception {
        storeEntity(EntityType.PROCESS, "summarize4");
        Process mockProcess = getStore().get(EntityType.PROCESS, "summarize4");
        mockProcess.setParallel(3);
        Date startTime = EntityUtil.getStartTime(mockProcess, cluster);
        ExecutionInstance instance1 = new ProcessExecutionInstance(mockProcess, new DateTime(startTime), cluster);
        // Schedule 3 instances.
        SchedulerService.JobScheduleRequestBuilder request = (SchedulerService.JobScheduleRequestBuilder)
                scheduler.createRequestBuilder(handler, instance1.getId());
        request.setInstance(instance1);
        scheduler.register(request.build());
        ExecutionInstance instance2 = new ProcessExecutionInstance(mockProcess,
                new DateTime(startTime.getTime() + 60000), cluster);
        SchedulerService.JobScheduleRequestBuilder request2 = (SchedulerService.JobScheduleRequestBuilder)
                scheduler.createRequestBuilder(handler, instance2.getId());
        request2.setInstance(instance2);
        scheduler.register(request2.build());
        ExecutionInstance instance3 = new ProcessExecutionInstance(mockProcess,
                new DateTime(startTime.getTime() + 120000), cluster);
        SchedulerService.JobScheduleRequestBuilder request3 = (SchedulerService.JobScheduleRequestBuilder)
                scheduler.createRequestBuilder(handler, instance3.getId());
        request3.setInstance(instance3);
        scheduler.register(request3.build());

        // Abort second instance
        scheduler.unregister(handler, instance2.getId());

        Thread.sleep(100);
        Assert.assertEquals(((MockDAGEngine) mockDagEngine).getTotalRuns(instance1), new Integer(1));
        Assert.assertEquals(((MockDAGEngine) mockDagEngine).getTotalRuns(instance3), new Integer(1));
        // Second instance should not run.
        Assert.assertEquals(((MockDAGEngine) mockDagEngine).getTotalRuns(instance2), null);
    }

    @Test
    public void testScheduleFailure() throws Exception {
        storeEntity(EntityType.PROCESS, "summarize5");
        Process mockProcess = getStore().get(EntityType.PROCESS, "summarize5");
        Date startTime = EntityUtil.getStartTime(mockProcess, cluster);
        ExecutionInstance instance1 = new ProcessExecutionInstance(mockProcess, new DateTime(startTime), cluster);
        // Scheduling an instance should fail.
        NotificationHandler failureHandler = new NotificationHandler() {
            @Override
            public void onEvent(Event event) throws FalconException {
                JobScheduledEvent scheduledEvent = ((JobScheduledEvent) event);
                if (scheduledEvent.getStatus() != JobScheduledEvent.STATUS.FAILED) {
                    failed = true;
                }
            }
        };
        SchedulerService.JobScheduleRequestBuilder request = (SchedulerService.JobScheduleRequestBuilder)
                scheduler.createRequestBuilder(failureHandler, instance1.getId());
        request.setInstance(instance1);
        ((MockDAGEngine)mockDagEngine).addFailInstance(instance1);
        scheduler.register(request.build());
        Thread.sleep(100);
        Assert.assertFalse(failed);
        ((MockDAGEngine)mockDagEngine).removeFailInstance(instance1);
    }

    /**
     * A mock notification Handler that makes appropriate state changes.
     */
    public static class MockNotificationHandler implements NotificationHandler {
        @Override
        public void onEvent(Event event) throws FalconException {
            JobScheduledEvent scheduledEvent = ((JobScheduledEvent) event);
            Process p = (Process) process.copy();
            p.setName(scheduledEvent.getTarget().getEntityName());
            InstanceID instanceID = (InstanceID) scheduledEvent.getTarget();
            DateTime instanceTime = new DateTime(instanceID.getInstanceTime());
            ProcessExecutionInstance instance = new ProcessExecutionInstance(p, instanceTime, cluster);
            InstanceState state = new InstanceState(instance).setCurrentState(STATE.RUNNING);
            if (!stateStore.executionInstanceExists(instance.getId())) {
                stateStore.putExecutionInstance(state);
            } else {
                stateStore.updateExecutionInstance(state);
            }
        }
    }
}

