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
package org.apache.falcon.state.service.store;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.falcon.FalconException;
import org.apache.falcon.cluster.util.EmbeddedCluster;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.exception.StateStoreException;
import org.apache.falcon.execution.ExecutionInstance;
import org.apache.falcon.execution.FalconExecutionService;
import org.apache.falcon.execution.MockDAGEngine;
import org.apache.falcon.execution.NotificationHandler;
import org.apache.falcon.notification.service.impl.AlarmService;
import org.apache.falcon.notification.service.impl.DataAvailabilityService;
import org.apache.falcon.notification.service.impl.JobCompletionService;
import org.apache.falcon.notification.service.impl.SchedulerService;
import org.apache.falcon.predicate.Predicate;
import org.apache.falcon.service.Services;
import org.apache.falcon.state.AbstractSchedulerTestBase;
import org.apache.falcon.state.EntityClusterID;
import org.apache.falcon.state.EntityID;
import org.apache.falcon.state.EntityState;
import org.apache.falcon.state.ID;
import org.apache.falcon.state.InstanceID;
import org.apache.falcon.state.InstanceState;
import org.apache.falcon.state.store.jdbc.BeanMapperUtil;
import org.apache.falcon.state.store.jdbc.JDBCStateStore;
import org.apache.falcon.state.store.StateStore;
import org.apache.falcon.state.store.service.FalconJPAService;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.engine.DAGEngine;
import org.apache.falcon.workflow.engine.DAGEngineFactory;
import org.apache.falcon.workflow.engine.OozieDAGEngine;
import org.joda.time.DateTime;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

/**
 * Test cases for JDBCStateStore.
 */
public class TestJDBCStateStore extends AbstractSchedulerTestBase {
    private static StateStore stateStore = JDBCStateStore.get();
    private static Random randomValGenerator = new Random();
    private static FalconJPAService falconJPAService = FalconJPAService.get();
    private AlarmService mockTimeService;
    private DataAvailabilityService mockDataService;
    private SchedulerService mockSchedulerService;
    private JobCompletionService mockCompletionService;
    private DAGEngine dagEngine;

    @BeforeClass
    public void setup() throws Exception {
        super.setup();
        createDB(DB_SQL_FILE);
        falconJPAService.init();
        this.dfsCluster = EmbeddedCluster.newCluster("testCluster");
        this.conf = dfsCluster.getConf();
        registerServices();
    }

    private void registerServices() throws FalconException {
        mockTimeService = Mockito.mock(AlarmService.class);
        Mockito.when(mockTimeService.getName()).thenReturn("AlarmService");
        Mockito.when(mockTimeService.createRequestBuilder(Mockito.any(NotificationHandler.class),
                Mockito.any(ID.class))).thenCallRealMethod();
        mockDataService = Mockito.mock(DataAvailabilityService.class);
        Mockito.when(mockDataService.getName()).thenReturn("DataAvailabilityService");
        Mockito.when(mockDataService.createRequestBuilder(Mockito.any(NotificationHandler.class),
                Mockito.any(ID.class))).thenCallRealMethod();
        dagEngine = Mockito.mock(OozieDAGEngine.class);
        Mockito.doNothing().when(dagEngine).resume(Mockito.any(ExecutionInstance.class));
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
    }


    @Test
    public void testInsertRetrieveAndUpdate() throws Exception {
        EntityState entityState = getEntityState(EntityType.PROCESS, "process");
        stateStore.putEntity(entityState);
        EntityID entityID = new EntityID(entityState.getEntity());
        EntityState actualEntityState = stateStore.getEntity(entityID);
        Assert.assertEquals(actualEntityState.getEntity(), entityState.getEntity());
        Assert.assertEquals(actualEntityState.getCurrentState(), entityState.getCurrentState());
        try {
            stateStore.putEntity(entityState);
            Assert.fail("Exception must have been thrown");
        } catch (StateStoreException e) {
            //no op
        }

        entityState.setCurrentState(EntityState.STATE.SCHEDULED);
        stateStore.updateEntity(entityState);
        actualEntityState = stateStore.getEntity(entityID);
        Assert.assertEquals(actualEntityState.getEntity(), entityState.getEntity());
        Assert.assertEquals(actualEntityState.getCurrentState(), entityState.getCurrentState());

        stateStore.deleteEntity(entityID);
        boolean entityExists = stateStore.entityExists(entityID);
        Assert.assertEquals(entityExists, false);

        try {
            stateStore.getEntity(entityID);
            Assert.fail("Exception must have been thrown");
        } catch (StateStoreException e){
            // no op
        }

        try {
            stateStore.updateEntity(entityState);
            Assert.fail("Exception must have been thrown");
        } catch (StateStoreException e) {
            // no op
        }

        try {
            stateStore.deleteEntity(entityID);
            Assert.fail("Exception must have been thrown");
        } catch (StateStoreException e){
            // no op
        }
    }


    @Test
    public void testGetEntities() throws Exception {
        EntityState entityState1 = getEntityState(EntityType.PROCESS, "process1");
        EntityState entityState2 = getEntityState(EntityType.PROCESS, "process2");
        EntityState entityState3 = getEntityState(EntityType.FEED, "feed1");

        Collection<EntityState> result = stateStore.getAllEntities();
        Assert.assertEquals(result.size(), 0);

        stateStore.putEntity(entityState1);
        stateStore.putEntity(entityState2);
        stateStore.putEntity(entityState3);

        result = stateStore.getAllEntities();
        Assert.assertEquals(result.size(), 3);

        Collection<Entity> entities = stateStore.getEntities(EntityState.STATE.SUBMITTED);
        Assert.assertEquals(entities.size(), 3);
    }


    @Test
    public void testInstanceInsertionAndUpdate() throws Exception {
        storeEntity(EntityType.CLUSTER, "testCluster");
        storeEntity(EntityType.FEED, "clicksFeed");
        storeEntity(EntityType.FEED, "clicksSummary");
        EntityState entityState = getEntityState(EntityType.PROCESS, "process");
        ExecutionInstance executionInstance = BeanMapperUtil.getExecutionInstance(
                entityState.getEntity().getEntityType(), entityState.getEntity(),
                System.currentTimeMillis(), "cluster", System.currentTimeMillis());
        InstanceState instanceState = new InstanceState(executionInstance);
        initInstanceState(instanceState);
        stateStore.putExecutionInstance(instanceState);
        InstanceID instanceID = new InstanceID(instanceState.getInstance());
        InstanceState actualInstanceState = stateStore.getExecutionInstance(instanceID);
        Assert.assertEquals(actualInstanceState, instanceState);

        instanceState.setCurrentState(InstanceState.STATE.RUNNING);
        Predicate predicate = new Predicate(Predicate.TYPE.DATA);
        instanceState.getInstance().getAwaitingPredicates().add(predicate);

        stateStore.updateExecutionInstance(instanceState);
        actualInstanceState = stateStore.getExecutionInstance(instanceID);
        Assert.assertEquals(actualInstanceState, instanceState);

        try {
            stateStore.putExecutionInstance(instanceState);
            Assert.fail("Exception must have been thrown");
        } catch (StateStoreException e) {
            // no op
        }

        stateStore.deleteExecutionInstance(instanceID);

        try {
            stateStore.getExecutionInstance(instanceID);
            Assert.fail("Exception must have been thrown");
        } catch (StateStoreException e) {
            // no op
        }

        try {
            stateStore.deleteExecutionInstance(instanceID);
            Assert.fail("Exception must have been thrown");
        } catch (StateStoreException e) {
            // no op
        }

        try {
            stateStore.updateExecutionInstance(instanceState);
            Assert.fail("Exception must have been thrown");
        } catch (StateStoreException e) {
            // no op
        }
    }


    @Test
    public void testBulkInstanceOperations() throws Exception {
        storeEntity(EntityType.CLUSTER, "testCluster");
        storeEntity(EntityType.FEED, "clicksFeed");
        storeEntity(EntityType.FEED, "clicksSummary");
        EntityState entityState = getEntityState(EntityType.PROCESS, "process1");
        ExecutionInstance processExecutionInstance1 = BeanMapperUtil.getExecutionInstance(
                entityState.getEntity().getEntityType(), entityState.getEntity(),
                System.currentTimeMillis() - 60000, "cluster1", System.currentTimeMillis() - 60000);
        InstanceState instanceState1 = new InstanceState(processExecutionInstance1);
        instanceState1.setCurrentState(InstanceState.STATE.READY);

        ExecutionInstance processExecutionInstance2 = BeanMapperUtil.getExecutionInstance(
                entityState.getEntity().getEntityType(), entityState.getEntity(),
                System.currentTimeMillis(), "cluster1", System.currentTimeMillis());
        InstanceState instanceState2 = new InstanceState(processExecutionInstance2);
        instanceState2.setCurrentState(InstanceState.STATE.RUNNING);

        ExecutionInstance processExecutionInstance3 = BeanMapperUtil.getExecutionInstance(
                entityState.getEntity().getEntityType(), entityState.getEntity(),
                System.currentTimeMillis(), "cluster2", System.currentTimeMillis());
        InstanceState instanceState3 = new InstanceState(processExecutionInstance3);
        instanceState3.setCurrentState(InstanceState.STATE.READY);

        stateStore.putExecutionInstance(instanceState1);
        stateStore.putExecutionInstance(instanceState2);
        stateStore.putExecutionInstance(instanceState3);

        Collection<InstanceState> actualInstances = stateStore.getAllExecutionInstances(entityState.getEntity(),
                "cluster1");
        Assert.assertEquals(actualInstances.size(), 2);
        Assert.assertEquals(actualInstances.toArray()[0], instanceState1);
        Assert.assertEquals(actualInstances.toArray()[1], instanceState2);

        actualInstances = stateStore.getAllExecutionInstances(entityState.getEntity(),
                "cluster2");
        Assert.assertEquals(actualInstances.size(), 1);
        Assert.assertEquals(actualInstances.toArray()[0], instanceState3);

        List<InstanceState.STATE> states = new ArrayList<>();
        states.add(InstanceState.STATE.READY);

        actualInstances = stateStore.getExecutionInstances(entityState.getEntity(), "cluster1", states);
        Assert.assertEquals(actualInstances.size(), 1);
        Assert.assertEquals(actualInstances.toArray()[0], instanceState1);

        EntityClusterID entityClusterID = new EntityClusterID(entityState.getEntity(), "testCluster");
        actualInstances = stateStore.getExecutionInstances(entityClusterID, states);
        Assert.assertEquals(actualInstances.size(), 2);
        Assert.assertEquals(actualInstances.toArray()[0], instanceState1);
        Assert.assertEquals(actualInstances.toArray()[1], instanceState3);

        states.add(InstanceState.STATE.RUNNING);
        actualInstances = stateStore.getExecutionInstances(entityState.getEntity(), "cluster1", states);
        Assert.assertEquals(actualInstances.size(), 2);
        Assert.assertEquals(actualInstances.toArray()[0], instanceState1);
        Assert.assertEquals(actualInstances.toArray()[1], instanceState2);

        InstanceState lastInstanceState = stateStore.getLastExecutionInstance(entityState.getEntity(), "cluster1");
        Assert.assertEquals(lastInstanceState, instanceState2);


        InstanceID instanceKey = new InstanceID(instanceState3.getInstance());
        stateStore.deleteExecutionInstance(instanceKey);

        actualInstances = stateStore.getAllExecutionInstances(entityState.getEntity(), "cluster2");
        Assert.assertEquals(actualInstances.size(), 0);

        actualInstances = stateStore.getAllExecutionInstances(entityState.getEntity(), "cluster1");
        Assert.assertEquals(actualInstances.size(), 2);

        stateStore.putExecutionInstance(instanceState3);

        actualInstances = stateStore.getAllExecutionInstances(entityState.getEntity(), "cluster2");
        Assert.assertEquals(actualInstances.size(), 1);

        stateStore.deleteExecutionInstances(entityClusterID.getEntityID());
        actualInstances = stateStore.getAllExecutionInstances(entityState.getEntity(), "cluster1");
        Assert.assertEquals(actualInstances.size(), 0);

        actualInstances = stateStore.getAllExecutionInstances(entityState.getEntity(), "cluster2");
        Assert.assertEquals(actualInstances.size(), 0);

    }


    @Test
    public void testGetExecutionInstancesWithRange() throws Exception {
        storeEntity(EntityType.CLUSTER, "testCluster");
        storeEntity(EntityType.FEED, "clicksFeed");
        storeEntity(EntityType.FEED, "clicksSummary");

        long instance1Time = System.currentTimeMillis() - 180000;
        long instance2Time = System.currentTimeMillis();
        EntityState entityState = getEntityState(EntityType.PROCESS, "process1");
        ExecutionInstance processExecutionInstance1 = BeanMapperUtil.getExecutionInstance(
                entityState.getEntity().getEntityType(), entityState.getEntity(),
                instance1Time, "cluster1", instance1Time);
        InstanceState instanceState1 = new InstanceState(processExecutionInstance1);
        instanceState1.setCurrentState(InstanceState.STATE.RUNNING);

        ExecutionInstance processExecutionInstance2 = BeanMapperUtil.getExecutionInstance(
                entityState.getEntity().getEntityType(), entityState.getEntity(),
                instance2Time, "cluster1", instance2Time);
        InstanceState instanceState2 = new InstanceState(processExecutionInstance2);
        instanceState2.setCurrentState(InstanceState.STATE.RUNNING);

        stateStore.putExecutionInstance(instanceState1);
        stateStore.putExecutionInstance(instanceState2);

        List<InstanceState.STATE> states = new ArrayList<>();
        states.add(InstanceState.STATE.RUNNING);

        Collection<InstanceState> actualInstances = stateStore.getExecutionInstances(entityState.getEntity(),
                "cluster1", states, new DateTime(instance1Time), new DateTime(instance1Time + 60000));
        Assert.assertEquals(1, actualInstances.size());
        Assert.assertEquals(instanceState1, actualInstances.toArray()[0]);

        actualInstances = stateStore.getExecutionInstances(entityState.getEntity(),
                "cluster1", states, new DateTime(instance2Time), new DateTime(instance2Time + 60000));
        Assert.assertEquals(1, actualInstances.size());
        Assert.assertEquals(instanceState2, actualInstances.toArray()[0]);

    }


    private void initInstanceState(InstanceState instanceState) {
        instanceState.setCurrentState(InstanceState.STATE.READY);
        instanceState.getInstance().setExternalID(RandomStringUtils.randomNumeric(6));
        instanceState.getInstance().setInstanceSequence(randomValGenerator.nextInt());
        instanceState.getInstance().setActualStart(new DateTime(System.currentTimeMillis()));
        instanceState.getInstance().setActualEnd(new DateTime(System.currentTimeMillis()));
        List<Predicate> predicates = new ArrayList<>();
        Predicate predicate = new Predicate(Predicate.TYPE.JOB_COMPLETION);
        predicates.add(predicate);
        instanceState.getInstance().setAwaitingPredicates(predicates);
    }

    private EntityState getEntityState(EntityType entityType, String name) throws Exception {
        storeEntity(entityType, name);
        Entity entity = getStore().get(entityType, name);
        Assert.assertNotNull(entity);
        return new EntityState(entity);
    }

    @AfterTest
    public void cleanUpTables() throws StateStoreException {
        stateStore.deleteEntities();
        stateStore.deleteExecutionInstances();
    }

    @AfterClass
    public void cleanup() throws IOException {
        super.cleanup();
    }
}
