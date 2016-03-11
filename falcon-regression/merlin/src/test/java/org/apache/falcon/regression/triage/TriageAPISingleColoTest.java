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

package org.apache.falcon.regression.triage;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.regression.Entities.ProcessMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.enumsAndConstants.ResponseErrors;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.EntityLineageUtil;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.falcon.resource.LineageGraphResult.Edge;
import org.apache.falcon.resource.SchedulableEntityInstance;
import org.apache.falcon.resource.TriageResult;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Test Class for Testing the Triage API on a single colo corresponding to FALCON-1377.
 */
@Test(groups = { "distributed", "embedded", "sanity" })
public class TriageAPISingleColoTest extends BaseTestClass {
    private ColoHelper cluster = servers.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String baseTestHDFSDir = cleanAndGetTestDir();
    private String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    private String outputFeedName, processName, clusterName;
    private String startTime, endTime;
    private String feedInputPath = baseTestHDFSDir + "/input" + MINUTE_DATE_PATTERN;
    private String feedOutputPath = baseTestHDFSDir + "/output-data" + MINUTE_DATE_PATTERN;
    private List<String> expectedVertexList = new ArrayList<>();
    private List<Edge> expectedEdgeList = new ArrayList<>();
    private TriageResult responseTriage;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        startTime = "2015-01-02T00:00Z";
        endTime = "2015-01-02T00:03Z";
        bundles[0] = BundleUtil.readELBundle();
        bundles[0].generateUniqueBundle(this);
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessConcurrency(1);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setOutputFeedLocationData(feedOutputPath);

        processName = bundles[0].getProcessName();
        clusterName = bundles[0].getClusterNames().get(0);
        outputFeedName = bundles[0].getOutputFeedNameFromBundle();

        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);

        expectedEdgeList = new ArrayList<>();
        expectedVertexList = new ArrayList<>();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        removeTestClassEntities();
        cleanTestsDirs();
    }

    /**
     * Creates expected output based on entity type, and then calls the validation function
     * to compare expected and actual graphs.
     * @param entityType type of entity, whether process or feed
     */
    private void createExpectedOutput(EntityType entityType) throws Exception{
        String finalInstanceTag;

        AssertUtil.assertSucceeded(responseTriage);
        Assert.assertEquals(responseTriage.getTriageGraphs().length, 1);
        String inputVertex1 = createVertex(bundles[0].getInputFeedNameFromBundle(),
                TimeUtil.addMinsToTime(startTime, -20), EntityType.FEED, "Input[MISSING]");
        expectedVertexList.add(inputVertex1);
        String inputVertex2 = createVertex(bundles[0].getInputFeedNameFromBundle(), startTime,
                EntityType.FEED, "Input[MISSING]");
        expectedVertexList.add(inputVertex2);
        if (entityType.equals(EntityType.PROCESS)) {
            finalInstanceTag = "[WAITING]";
        } else {
            finalInstanceTag = "Output[WAITING]";
        }
        String processVertex = createVertex(processName, startTime, EntityType.PROCESS, finalInstanceTag);
        expectedVertexList.add(processVertex);
        if (entityType.equals(EntityType.FEED)) {
            String outputVertex1 = createVertex(outputFeedName, startTime,
                    EntityType.FEED, "[MISSING]");
            expectedVertexList.add(outputVertex1);
            expectedEdgeList.add(new Edge(processVertex, outputVertex1, "produces"));
        }
        expectedEdgeList.add(new Edge(inputVertex1, processVertex, "consumed by"));
        expectedEdgeList.add(new Edge(inputVertex2, processVertex, "consumed by"));

        EntityLineageUtil.validateLineageGraphResult(responseTriage.getTriageGraphs()[0],
                expectedVertexList.toArray(new String[expectedVertexList.size()]),
                expectedEdgeList.toArray(new Edge[expectedEdgeList.size()]));
    }

    /**
     * Single process with one input and one output, of which one instance is in waiting, and request for triage
     * on that instance on the server. There should be no output feed and the process instance is the terminal
     * instance. Upon triaging on server on an output instance of the feed, an additional vertex and edge should
     * be seen for this feed instance.
     *
     * @throws Exception
     */
    @Test(dataProvider = "getParameters", groups = "embedded")
    public void triageTestServer(EntityType entityType) throws Exception {
        bundles[0].submitFeedsScheduleProcess();
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        if (entityType.equals(EntityType.FEED)) {
            responseTriage = cluster.getFeedHelper().getInstanceTriage(outputFeedName,
                    "?start=" + startTime);
        } else {
            responseTriage = cluster.getProcessHelper().getInstanceTriage(processName,
                    "?start=" + startTime);
        }
        //Creating expected vertices and graphs
        createExpectedOutput(entityType);
    }

    /**
     * Single process with one input and one output, of which one instance is in waiting, and request for triage
     * on that instance on the server. There should be no output feed and the process instance is the terminal
     * instance. Upon triaging on prism on an output instance of the feed, an additional vertex and edge should
     * be seen for this feed instance.
     *
     * @throws Exception
     */
    @Test(dataProvider = "getParameters", groups = "distributed")
    public void triageTestPrism(EntityType entityType) throws Exception {
        bundles[0].submitFeedsScheduleProcess();
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        if (entityType.equals(EntityType.FEED)) {
            responseTriage = prism.getFeedHelper().getInstanceTriage(outputFeedName,
                    "?start=" + startTime);
        } else {
            responseTriage = prism.getProcessHelper().getInstanceTriage(processName,
                    "?start=" + startTime);
        }
        //Creating expected vertices and graphs
        createExpectedOutput(entityType);
    }

    /**
     * Single process with one input and one output, but we triage on a non-existent feed/process
     * instance on the server. Appropriate error should be thrown.
     *
     * @throws Exception
     */
    @Test(dataProvider = "getParameters", groups = "embedded")
    public void invalidInstanceOnServerTest(EntityType entityType) throws Exception {
        bundles[0].submitFeedsScheduleProcess();
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        if (entityType.equals(EntityType.FEED)) {
            responseTriage = cluster.getFeedHelper().getInstanceTriage(outputFeedName,
                    "?start=" + TimeUtil.addMinsToTime(startTime, 2));
        } else {
            responseTriage = cluster.getProcessHelper().getInstanceTriage(processName,
                    "?start=" + TimeUtil.addMinsToTime(startTime, 2));
        }
        EntityLineageUtil.validateError(responseTriage, ResponseErrors.INVALID_INSTANCE_TIME);
    }

    /**
     * Single process with one input and one output, but we triage on a non-existent feed/process
     * instance on the prism. Appropriate error should be thrown.
     *
     * @throws Exception
     */
    @Test(dataProvider = "getParameters", groups = "distributed")
    public void invalidInstanceOnPrismTest(EntityType entityType) throws Exception {
        bundles[0].submitFeedsScheduleProcess();
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        if (entityType.equals(EntityType.FEED)) {
            responseTriage = prism.getFeedHelper().getInstanceTriage(outputFeedName,
                    "?start=" + TimeUtil.addMinsToTime(startTime, 2));
        } else {
            responseTriage = prism.getProcessHelper().getInstanceTriage(processName,
                    "?start=" + TimeUtil.addMinsToTime(startTime, 2));
        }
        EntityLineageUtil.validateError(responseTriage, ResponseErrors.INVALID_INSTANCE_TIME);
    }

    /**
     * Submit and Schedule a process on one cluster via prism, and triage on an instance of the process on a different
     * cluster. Appropriate error should be thrown.
     *
     * @throws Exception
     */
    @Test(groups = "embedded")
    public void processTriageOnServerWhereProcessDoesNotExistTest() throws Exception {
        responseTriage = servers.get(1).getProcessHelper().getInstanceTriage(processName,
                "?start=" + startTime);
        EntityLineageUtil.validateError(responseTriage, ResponseErrors.PROCESS_NOT_FOUND);
    }

    /**
     * Single process with one input and one output which succeeds. Triage on server, on a succeeded instance,
     * and we should get just one vertex in the graph, without any edges.
     *
     * @throws Exception
     */
    @Test(groups = "embedded")
    public void processInstanceSucceededTriageOnServerTest() throws Exception {
        bundles[0].submitFeedsScheduleProcess();
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS, 5);
        responseTriage = cluster.getProcessHelper().getInstanceTriage(processName,
                "?start=" + startTime);
        AssertUtil.assertSucceeded(responseTriage);
        Assert.assertEquals(responseTriage.getTriageGraphs().length, 1);

        //There'll be just one process instance vertex and no edges
        Assert.assertEquals(responseTriage.getTriageGraphs()[0].getVertices().length, 1);
        String processVertex = createVertex(processName, startTime, EntityType.PROCESS, "[SUCCEEDED]");
        expectedVertexList.add(processVertex);

        EntityLineageUtil.validateLineageGraphResult(responseTriage.getTriageGraphs()[0],
                expectedVertexList.toArray(new String[expectedVertexList.size()]),
                expectedEdgeList.toArray(new Edge[expectedEdgeList.size()]));
    }

    /**
     * Single process with one input and one output which succeeds. Triage on prism, on a succeeded instance,
     * and we should get just one vertex in the graph, without any edges.
     *
     * @throws Exception
     */
    @Test(groups = "distributed")
    public void processInstanceSucceededTriageOnPrismTest() throws Exception {
        bundles[0].submitFeedsScheduleProcess();
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        InstanceUtil.waitTillInstancesAreCreated(clusterOC, bundles[0].getProcessData(), 0);
        OozieUtil.createMissingDependencies(cluster, EntityType.PROCESS, processName, 0);
        InstanceUtil.waitTillInstanceReachState(clusterOC, processName, 1,
                CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS, 5);
        responseTriage = prism.getProcessHelper().getInstanceTriage(processName,
                "?start=" + startTime);
        AssertUtil.assertSucceeded(responseTriage);
        Assert.assertEquals(responseTriage.getTriageGraphs().length, 1);

        //There'll be just one process instance vertex and no edges
        Assert.assertEquals(responseTriage.getTriageGraphs()[0].getVertices().length, 1);
        String processVertex = createVertex(processName, startTime, EntityType.PROCESS,
                "[SUCCEEDED]");
        expectedVertexList.add(processVertex);

        EntityLineageUtil.validateLineageGraphResult(responseTriage.getTriageGraphs()[0],
                expectedVertexList.toArray(new String[expectedVertexList.size()]),
                expectedEdgeList.toArray(new Edge[expectedEdgeList.size()]));
    }

    /**
     * Single process one instance of whose output feed is fed as input to the process. This is to test the closed
     * loop condition, in case we triage on a process instance, or on an output feed instance.
     *
     * @throws Exception
     */
    @Test(groups = "distributed")
    public void cycleTest() throws Exception {
        //Setting an instance of the output feed of a process as input to the process
        bundles[0].addProcessInput("inputData2", outputFeedName);
        ProcessMerlin processObj = new ProcessMerlin(bundles[0].getProcessData());
        processObj.getInputs().getInputs().get(1).setStart("now(-1,0)");
        processObj.getInputs().getInputs().get(1).setEnd("now(-1,0)");
        bundles[0].setProcessData(processObj.toString());
        bundles[0].submitFeedsScheduleProcess();

        responseTriage = prism.getFeedHelper().getInstanceTriage(outputFeedName,
                "?start=" + startTime);
        AssertUtil.assertSucceeded(responseTriage);
        Assert.assertEquals(responseTriage.getTriageGraphs().length, 1);

        //There'll be four feed instance vertices and one process instance vertex
        String inputVertex1 = createVertex(bundles[0].getInputFeedNameFromBundle(),
                TimeUtil.addMinsToTime(startTime, -20), EntityType.FEED, "Input[MISSING]");
        expectedVertexList.add(inputVertex1);
        String inputVertex2 = createVertex(bundles[0].getInputFeedNameFromBundle(), startTime,
                EntityType.FEED, "Input[MISSING]");
        expectedVertexList.add(inputVertex2);
        String inputVertex3 = createVertex(outputFeedName, TimeUtil.addMinsToTime(startTime, -60),
                EntityType.FEED, "Input[MISSING]");
        expectedVertexList.add(inputVertex3);
        String processVertex = createVertex(processName, startTime, EntityType.PROCESS,
                "Output[WAITING]");
        expectedVertexList.add(processVertex);
        String outputVertex1 = createVertex(outputFeedName, startTime,
                EntityType.FEED, "[MISSING]");
        expectedVertexList.add(outputVertex1);
        expectedEdgeList.add(new Edge(inputVertex1, processVertex, "consumed by"));
        expectedEdgeList.add(new Edge(inputVertex2, processVertex, "consumed by"));
        expectedEdgeList.add(new Edge(inputVertex3, processVertex, "consumed by"));
        expectedEdgeList.add(new Edge(processVertex, outputVertex1, "produces"));

        EntityLineageUtil.validateLineageGraphResult(responseTriage.getTriageGraphs()[0],
                expectedVertexList.toArray(new String[expectedVertexList.size()]),
                expectedEdgeList.toArray(new Edge[expectedEdgeList.size()]));

        responseTriage = prism.getProcessHelper().getInstanceTriage(processName, "?start=" + startTime);
        AssertUtil.assertSucceeded(responseTriage);
        Assert.assertEquals(responseTriage.getTriageGraphs().length, 1);

        //There'll be three feed instance vertices and one process instance vertex
        expectedVertexList = new ArrayList<>();
        expectedEdgeList = new ArrayList<>();
        inputVertex1 = createVertex(bundles[0].getInputFeedNameFromBundle(), TimeUtil.addMinsToTime(startTime, -20),
                EntityType.FEED, "Input[MISSING]");
        expectedVertexList.add(inputVertex1);
        inputVertex2 = createVertex(bundles[0].getInputFeedNameFromBundle(), startTime,
                EntityType.FEED, "Input[MISSING]");
        expectedVertexList.add(inputVertex2);
        inputVertex3 = createVertex(outputFeedName, TimeUtil.addMinsToTime(startTime, -60),
                EntityType.FEED, "Input[MISSING]");
        expectedVertexList.add(inputVertex3);
        processVertex = createVertex(processName, startTime, EntityType.PROCESS,
                "[WAITING]");
        expectedVertexList.add(processVertex);
        expectedEdgeList.add(new Edge(inputVertex1, processVertex, "consumed by"));
        expectedEdgeList.add(new Edge(inputVertex2, processVertex, "consumed by"));
        expectedEdgeList.add(new Edge(inputVertex3, processVertex, "consumed by"));

        EntityLineageUtil.validateLineageGraphResult(responseTriage.getTriageGraphs()[0],
                expectedVertexList.toArray(new String[expectedVertexList.size()]),
                expectedEdgeList.toArray(new Edge[expectedEdgeList.size()]));
    }

    /**
     * Two Dependent processes, where one consumes the output of the other. Triage on the output of the
     * second process.
     *
     * @throws Exception
     */
    @Test(groups = "distributed")
    public void twoDependentProcessesTest() throws Exception {
        bundles[0].submitFeedsScheduleProcess(); //this process will stay in waiting

        //There'll be three feed instance vertices (2 input, 1 output) and two process instance vertices
        String inputVertex1 = createVertex(bundles[0].getInputFeedNameFromBundle(),
                TimeUtil.addMinsToTime(startTime, -20), EntityType.FEED, "Input[MISSING]");
        expectedVertexList.add(inputVertex1);
        String inputVertex2 = createVertex(bundles[0].getInputFeedNameFromBundle(), startTime,
                EntityType.FEED, "Input[MISSING]");
        expectedVertexList.add(inputVertex2);
        String processVertex1 = createVertex(processName, startTime, EntityType.PROCESS,
                "Output[WAITING]");
        expectedVertexList.add(processVertex1);
        String outputVertex1 = createVertex(outputFeedName, startTime,
                EntityType.FEED, "Input[MISSING]");
        expectedVertexList.add(outputVertex1);

        //preparing second process
        bundles[0].setProcessValidity(TimeUtil.addMinsToTime(startTime, 60), TimeUtil.addMinsToTime(startTime, 61));
        ProcessMerlin processObj = new ProcessMerlin(bundles[0].getProcessData());
        processObj.getOutputs().getOutputs().get(0).setFeed(processObj.getInputs().getInputs().get(0).getFeed());
        //The input of this process is the output of the previous process
        processObj.getInputs().getInputs().get(0).setFeed(outputFeedName);
        processObj.setName("ConsumerOfFirstProcessOutput");
        bundles[0].setProcessData(processObj.toString());
        bundles[0].setProcessInputStartEnd("now(-1,0)", "now(-1,0)");
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].submitFeedsScheduleProcess();
        responseTriage = prism.getFeedHelper().getInstanceTriage(bundles[0].getOutputFeedNameFromBundle(),
                "?start=" + TimeUtil.addMinsToTime(startTime, 60));
        AssertUtil.assertSucceeded(responseTriage);
        Assert.assertEquals(responseTriage.getTriageGraphs().length, 1);

        //Adding one more process vertex and output feed vertex
        String processVertex2 = createVertex("ConsumerOfFirstProcessOutput", TimeUtil.addMinsToTime(startTime, 60),
                EntityType.PROCESS, "Output[WAITING]");
        expectedVertexList.add(processVertex2);
        String outputVertex2 = createVertex(bundles[0].getOutputFeedNameFromBundle(),
                TimeUtil.addMinsToTime(startTime, 60), EntityType.FEED, "[MISSING]");
        expectedVertexList.add(outputVertex2);

        expectedEdgeList.add(new Edge(inputVertex1, processVertex1, "consumed by"));
        expectedEdgeList.add(new Edge(inputVertex2, processVertex1, "consumed by"));
        expectedEdgeList.add(new Edge(processVertex1, outputVertex1, "produces"));
        expectedEdgeList.add(new Edge(outputVertex1, processVertex2, "consumed by"));
        expectedEdgeList.add(new Edge(processVertex2, outputVertex2, "produces"));

        EntityLineageUtil.validateLineageGraphResult(responseTriage.getTriageGraphs()[0],
                expectedVertexList.toArray(new String[expectedVertexList.size()]),
                expectedEdgeList.toArray(new Edge[expectedEdgeList.size()]));
    }

    /**
     * Data Provider enables the same test to run for triage on entities feed and process.
     */
    @DataProvider
    private Object[][] getParameters() {
        return new Object[][]{{EntityType.FEED}, {EntityType.PROCESS}};
    }

    /**
     * Creates a vertex out of the fields provided.
     * @param name name of process/feed
     * @param instanceTime instance time
     * @param entityType if entity is process or feed
     * @param tags status of the feed
     */
    private String createVertex(String name, String instanceTime, EntityType entityType, String tags) {
        SchedulableEntityInstance vertex = new SchedulableEntityInstance(name, clusterName,
                SchemaHelper.parseDateUTC(instanceTime), entityType);
        vertex.setTags(tags);
        return vertex.toString();
    }
}
