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
package org.apache.falcon.regression.nativeScheduler;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.response.ServiceResponse;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.XmlUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.NativeInstanceUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.EntityList;
import org.apache.falcon.resource.EntitySummaryResult;
import org.apache.falcon.resource.InstanceDependencyResult;
import org.apache.falcon.resource.InstancesResult;
import org.apache.falcon.resource.TriageResult;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Schedule process via native scheduler and test available APIs.
 */

@Test(groups = {"distributed", "embedded" })
public class NativeSchedulerAPITest extends BaseTestClass {
    private ColoHelper cluster1 = servers.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private static final Logger LOGGER = Logger.getLogger(NativeSchedulerAPITest.class);
    private ProcessMerlin processMerlin;
    private String startTime;
    private String endTime;

    @BeforeClass(alwaysRun = true)
    public void uploadWorkflow() throws Exception {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.concat(OSUtil.RESOURCES, "sleep"));
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        startTime = TimeUtil.getTimeWrtSystemTime(-10);
        endTime = TimeUtil.addMinsToTime(startTime, 50);
        LOGGER.info("Time range between : " + startTime + " and " + endTime);
        Bundle bundle = BundleUtil.readELBundle();

        bundles[0] = new Bundle(bundle, servers.get(0));
        bundles[0].generateUniqueBundle(this);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].submitClusters(prism);
        bundles[0].setProcessConcurrency(2);
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.minutes);

        //Setting input and output feed as null (time based scheduling)
        processMerlin = bundles[0].getProcessObject();
        processMerlin.setInputs(null);
        processMerlin.setOutputs(null);
        LOGGER.info(processMerlin.toString());

        // Submit process
        ServiceResponse response = prism.getProcessHelper().submitEntity(processMerlin.toString());
        AssertUtil.assertSucceeded(response);

        // Schedule with prism
        response = prism.getProcessHelper().schedule(processMerlin.toString(), null,
                "properties=falcon.scheduler:native");
        AssertUtil.assertSucceeded(response);

        // Schedule with server
        response = cluster1.getProcessHelper().schedule(processMerlin.toString(), null,
                "properties=falcon.scheduler:native");
        AssertUtil.assertSucceeded(response);

    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /*Suspend and resume entity and check that it is successful.
     */
    @Test
    public void entitySuspendResumeTest() throws Exception {
        //Suspend entity via prism
        AssertUtil.assertSucceeded(prism.getProcessHelper().suspend(processMerlin.toString()));

        Assert.assertTrue(Util.parseResponse(prism.getProcessHelper()
                .getStatus(processMerlin.toString())).getMessage()
                .contains("SUSPENDED"));

        //Resume entity via prism
        AssertUtil.assertSucceeded(prism.getProcessHelper().resume(processMerlin.toString()));

        Assert.assertTrue(Util.parseResponse(prism.getProcessHelper()
                .getStatus(processMerlin.toString())).getMessage()
                .contains("RUNNING"));

        //Suspend entity via server
        AssertUtil.assertSucceeded(cluster1.getProcessHelper().suspend(processMerlin.toString()));

        Assert.assertTrue(Util.parseResponse(cluster1.getProcessHelper()
                .getStatus(processMerlin.toString())).getMessage()
                .contains("SUSPENDED"));

        //Resume entity via server
        AssertUtil.assertSucceeded(cluster1.getProcessHelper().resume(processMerlin.toString()));

        Assert.assertTrue(Util.parseResponse(cluster1.getProcessHelper()
                .getStatus(processMerlin.toString())).getMessage()
                .contains("RUNNING"));
    }

    /*Test for entity definition, list, dependency, update and summary API and check that it is successful.
     */
    @Test
    public void entityDefinitionListDependencyUpdateSummaryTest() throws Exception {
        // Entity Definition
        String processDef = prism.getProcessHelper().getEntityDefinition(processMerlin.toString()).getMessage();
        Assert.assertTrue(XmlUtil.isIdentical(processMerlin.toString(), processDef), "Definitions are not equal.");

        // Entity List
        EntityList.EntityElement[] entityList = prism.getProcessHelper().listAllEntities().getEntityList().
                getElements();
        Assert.assertTrue(entityList.length==1);
        Assert.assertTrue(entityList[0].type.equals("PROCESS"));
        Assert.assertTrue(entityList[0].name.equals(processMerlin.getName()));

        // Entity Dependency
        EntityList.EntityElement[] entityDependency = prism.getProcessHelper().getDependencies(
                processMerlin.getName()).getEntityList().getElements();
        Assert.assertTrue(entityDependency.length==1);
        Assert.assertTrue(entityDependency[0].type.equals("cluster"));
        Assert.assertTrue(entityDependency[0].name.equals(bundles[0].getClusterElement().getName()));

        // Entity Update
        NativeInstanceUtil.waitTillInstanceReachState(prism, processMerlin, startTime, CoordinatorAction.Status.RUNNING,
                processMerlin.getFrequency());
        processMerlin.setParallel(3);
        LOGGER.info("Updated process xml: " + processMerlin.toString());
        AssertUtil.assertSucceeded(prism.getProcessHelper().update(processMerlin.toString(), processMerlin.toString()));
        processDef = prism.getProcessHelper().getEntityDefinition(processMerlin.toString()).getMessage();
        Assert.assertTrue(XmlUtil.isIdentical(processMerlin.toString(), processDef), "Definitions are not equal.");

        // Entity summary
        EntitySummaryResult.EntitySummary[] summaries = cluster1.getProcessHelper()
                .getEntitySummary(processMerlin.getClusterNames().get(0), "filterBy=STATUS:RUNNING")
                .getEntitySummaryResult().getEntitySummaries();
        Assert.assertEquals(summaries.length, 1, "There should be one RUNNING process filtered.");
        Assert.assertEquals(summaries[0].getName(), processMerlin.getName(),
                "Summary shows invalid suspended process.");
    }

    /*Test for instance dependency, list, params, logs, running and triage API and check that it is successful.
     */
    @Test
    public void instanceAPITest() throws Exception {
        // Instance dependency
        NativeInstanceUtil.waitTillInstanceReachState(prism, processMerlin, startTime, CoordinatorAction.Status.RUNNING,
                processMerlin.getFrequency());
        InstanceDependencyResult dependencyResult = prism.getProcessHelper().getInstanceDependencies(
                processMerlin.getName(),  "?instanceTime=" + startTime, null);
        AssertUtil.assertSucceeded(dependencyResult);
        Assert.assertNull(dependencyResult.getDependencies());

        // Instance List
        NativeInstanceUtil.waitTillInstanceReachState(prism, processMerlin, TimeUtil.addMinsToTime(startTime, 2),
                CoordinatorAction.Status.SUCCEEDED, processMerlin.getFrequency());
        InstancesResult instanceResult = prism.getProcessHelper().listInstances(processMerlin.getName(),
                "start=" + startTime + "&end=" + TimeUtil.addMinsToTime(startTime, 2), null);
        InstanceUtil.validateResponse(instanceResult, 2, 0, 0, 0, 0);

        // Instance Params
        InstancesResult paramsResult = prism.getProcessHelper()
                .getInstanceParams(processMerlin.getName(), "?start=" + startTime);
        AssertUtil.assertSucceeded(paramsResult);
        Assert.assertEquals(paramsResult.getInstances().length, 1);
        Assert.assertEquals(paramsResult.getInstances()[0].getInstance(), startTime);

        // Instance logs
        InstancesResult logsResult = prism.getProcessHelper()
                .getProcessInstanceLogs(processMerlin.getName(),
                        "?start=" + startTime + "&end=" + TimeUtil.addMinsToTime(startTime, 1));
        AssertUtil.assertSucceeded(logsResult);
        Assert.assertEquals(logsResult.getInstances().length, 1);
        Assert.assertEquals(logsResult.getInstances()[0].getInstance(), startTime);

        // Instance running
        InstancesResult runningInstance = prism.getProcessHelper().getRunningInstance(processMerlin.getName());
        InstanceUtil.validateResponse(runningInstance, 2, 2, 0, 0, 0);

        // Instance triage
        TriageResult responseTriage = prism.getProcessHelper().getInstanceTriage(processMerlin.getName(),
                "?start=" + startTime);
        AssertUtil.assertSucceeded(responseTriage);
        Assert.assertEquals(responseTriage.getTriageGraphs().length, 1);
        //There'll be just one process instance vertex and no edges
        Assert.assertEquals(responseTriage.getTriageGraphs()[0].getVertices().length, 1);
    }

    /*Test for instance suspend, status and resume API and check that it is successful.
     */
    @Test
    public void instanceSuspendResumeAPITest() throws Exception {

        String range = "?start=" + startTime + "&end=" + TimeUtil.addMinsToTime(startTime, 5);
        NativeInstanceUtil.waitTillInstanceReachState(prism, processMerlin, startTime, CoordinatorAction.Status.RUNNING,
                processMerlin.getFrequency());
        // Suspend instance
        InstancesResult instancesResult = prism.getProcessHelper().getProcessInstanceSuspend(
                processMerlin.getName(), range);
        InstanceUtil.validateResponse(instancesResult, 5, 0, 5, 0, 0);

        // Suspend again
        instancesResult = prism.getProcessHelper().getProcessInstanceSuspend(
                processMerlin.getName(), range);
        Assert.assertNull(instancesResult.getInstances());

        // Validate instance status
        instancesResult = prism.getProcessHelper().getProcessInstanceStatus(processMerlin.getName(),
                range);
        InstanceUtil.validateResponse(instancesResult, 5, 0, 5, 0, 0);

        // Resume instance
        instancesResult = prism.getProcessHelper().getProcessInstanceResume(processMerlin.getName(),
                range);
        InstanceUtil.validateResponse(instancesResult, 5, 2, 0, 0, 0);

        // Resume again
        instancesResult = prism.getProcessHelper().getProcessInstanceResume(processMerlin.getName(),
                range);
        Assert.assertNull(instancesResult.getInstances());

        // Validate instance status
        instancesResult = prism.getProcessHelper().getProcessInstanceStatus(processMerlin.getName(),
                range);
        InstanceUtil.validateResponse(instancesResult, 5, 2, 0, 0, 0);
    }

    /*Test for instance kill and rerun and check that it is successful.
     */
    @Test
    public void instanceKillRerunAPITest() throws Exception {
        String range = "?start=" + startTime + "&end=" + TimeUtil.addMinsToTime(startTime, 5);
        NativeInstanceUtil.waitTillInstanceReachState(prism, processMerlin, startTime, CoordinatorAction.Status.RUNNING,
                processMerlin.getFrequency());

        //Instance Kill
        InstancesResult instancesResult = prism.getProcessHelper().getProcessInstanceKill(processMerlin.getName(),
                range);
        InstanceUtil.validateResponse(instancesResult, 5, 0, 0, 0, 5);

        // Instance rerun
        prism.getProcessHelper().getProcessInstanceRerun(processMerlin.getName(),
                "?start=" + startTime + "&end=" + TimeUtil.addMinsToTime(startTime, 3));
        NativeInstanceUtil.waitTillInstanceReachState(prism, processMerlin, startTime, CoordinatorAction.Status.RUNNING,
                processMerlin.getFrequency());

        instancesResult = prism.getProcessHelper().getProcessInstanceStatus(processMerlin.getName(), range);
        InstanceUtil.validateResponse(instancesResult, 5, 2, 0, 0, 3);

    }

    /*Test for rerun of succeeded instance and check that it is successful.
     */
    @Test
    public void instanceSucceedRerunAPITest() throws Exception {
        String range = "?start=" + startTime + "&end=" + TimeUtil.addMinsToTime(startTime, 3);
        NativeInstanceUtil.waitTillInstanceReachState(prism, processMerlin, TimeUtil.addMinsToTime(startTime, 3),
                CoordinatorAction.Status.SUCCEEDED, processMerlin.getFrequency());

        // Rerun succeeded instance
        prism.getProcessHelper().getProcessInstanceRerun(processMerlin.getName(), range + "&force=true");
        NativeInstanceUtil.waitTillInstanceReachState(prism, processMerlin, startTime, CoordinatorAction.Status.RUNNING,
                processMerlin.getFrequency());

        InstancesResult instanceResult = prism.getProcessHelper().getProcessInstanceStatus(
                processMerlin.getName(), range);
        InstanceUtil.validateResponse(instanceResult, 3, 2, 0, 0, 0);
    }

}
