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

package org.apache.falcon.regression.lineage;

import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.LineageHelper;
import org.apache.falcon.regression.core.response.InstancesResult;
import org.apache.falcon.regression.core.response.lineage.Direction;
import org.apache.falcon.regression.core.response.lineage.Vertex;
import org.apache.falcon.regression.core.response.lineage.VerticesResult;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.GraphAssert;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.OozieUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.oozie.client.Job;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

@Test(groups = "lineage-rest")
public class LineageApiProcessInstanceTest extends BaseTestClass {
    private static final Logger logger = Logger.getLogger(LineageApiProcessInstanceTest.class);

    ColoHelper cluster = servers.get(0);
    FileSystem clusterFS = serverFS.get(0);
    LineageHelper lineageHelper;
    String baseTestHDFSDir = baseHDFSDir + "/LineageApiInstanceTest";
    String aggregateWorkflowDir = baseTestHDFSDir + "/aggregator";
    String feedInputPrefix = baseTestHDFSDir + "/input";
    String feedInputPath = feedInputPrefix + "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String feedOutputPath =
        baseTestHDFSDir + "/output-data/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String processName;
    String inputFeedName;
    String outputFeedName;
    final String dataStartDate = "2010-01-02T09:00Z";
    final String processStartDate = "2010-01-02T09:50Z";
    final String endDate = "2010-01-02T10:00Z";


    @BeforeClass(alwaysRun = true)
    public void init() {
        lineageHelper = new LineageHelper(prism);
    }

    @BeforeMethod(alwaysRun = true, firstTimeOnly = true)
    public void setup(Method method) throws Exception {
        HadoopUtil.deleteDirIfExists(baseTestHDFSDir, clusterFS);
        HadoopUtil.uploadDir(clusterFS, aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);

        bundles[0] = new Bundle(BundleUtil.readELBundle(), cluster);
        bundles[0].generateUniqueBundle();

        bundles[0].setInputFeedDataPath(feedInputPath);

        // data set creation
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(dataStartDate, endDate, 5);
        logger.info("dataDates = " + dataDates);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, feedInputPrefix,
            dataDates);

        // running process
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setInputFeedPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setOutputFeedLocationData(feedOutputPath);
        bundles[0].setOutputFeedPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        bundles[0].setProcessValidity(processStartDate, endDate);
        bundles[0].setProcessPeriodicity(5, Frequency.TimeUnit.minutes);
        bundles[0].submitFeedsScheduleProcess(prism);
        processName = bundles[0].getProcessName();
        inputFeedName = bundles[0].getInputFeedNameFromBundle();
        outputFeedName = bundles[0].getOutputFeedNameFromBundle();
        Job.Status status = null;
        for (int i = 0; i < 20; i++) {
            status = InstanceUtil.getDefaultCoordinatorStatus(cluster,
                Util.getProcessName(bundles[0].getProcessData()), 0);
            if (status == Job.Status.SUCCEEDED || status == Job.Status.KILLED)
                break;
            TimeUtil.sleepSeconds(30);
        }
        Assert.assertNotNull(status);
        Assert.assertEquals(status, Job.Status.SUCCEEDED,
            "The job did not succeeded even in long time");
    }

    @AfterMethod(alwaysRun = true, lastTimeOnly = true)
    public void tearDown() {
        removeBundles();
    }

    /**
     * Test navigation from the process vertex to its instances vertices
     * @throws Exception
     */
    @Test
    public void processToProcessInstanceNodes() throws Exception {
        final VerticesResult processResult = lineageHelper.getVerticesByName(processName);
        GraphAssert.assertVertexSanity(processResult);
        Vertex processVertex = processResult.getResults().get(0);
        final VerticesResult processIncoming =
            lineageHelper.getVerticesByDirection(processVertex.get_id(), Direction.inComingVertices);
        GraphAssert.assertVertexSanity(processIncoming);
        final List<Vertex> processInstanceVertices =
            processIncoming.filterByType(Vertex.VERTEX_TYPE.PROCESS_INSTANCE);
        logger.info("process instances = " + processInstanceVertices);
        InstancesResult result = prism.getProcessHelper()
            .getProcessInstanceStatus(processName, "?start=" + processStartDate +
                "&end=" + endDate);
        Assert.assertEquals(processInstanceVertices.size(), result.getInstances().length,
            "Number of process instances should be same weather it is retrieved from lineage api " +
                "or falcon rest api");
    }

    /**
     * Test navigation from the process instance vertex to its input and output feed instances
     * @throws Exception
     */
    @Test
    public void processInstanceToFeedInstanceNodes() throws Exception {
        final VerticesResult processResult = lineageHelper.getVerticesByName(processName);
        GraphAssert.assertVertexSanity(processResult);
        Vertex processVertex = processResult.getResults().get(0);
        final VerticesResult processIncoming =
            lineageHelper.getVerticesByDirection(processVertex.get_id(), Direction.inComingVertices);
        GraphAssert.assertVertexSanity(processIncoming);
        // fetching process instance vertex
        final List<Vertex> piVertices =
            processIncoming.filterByType(Vertex.VERTEX_TYPE.PROCESS_INSTANCE);
        logger.info("process instance vertex = " + piVertices);

        // fetching process instances info
        InstancesResult piResult = prism.getProcessHelper()
            .getProcessInstanceStatus(processName, "?start=" + processStartDate +
                "&end=" + endDate);
        Assert.assertEquals(piVertices.size(), piResult.getInstances().length,
            "Number of process instances should be same weather it is retrieved from lineage api " +
                "or falcon rest api");
        final List<String> allowedPITimes = new ArrayList<String>();
        for (InstancesResult.Instance processInstance : piResult.getInstances()) {
            allowedPITimes.add(processInstance.getInstance());
        }

        for (Vertex piVertex : piVertices) {
            Assert.assertTrue(piVertex.getName().startsWith(processName),
                "Process instance names should start with process name: " + piVertex.getName());
            String processInstanceTime = piVertex.getName().substring(processName.length() + 1);
            logger.info("processInstanceTime = " + processInstanceTime);
            Assert.assertTrue(allowedPITimes.remove(processInstanceTime),
                "Unexpected processInstanceTime: " + processInstanceTime +
                    "it should have been be in the list " + allowedPITimes);

            VerticesResult piIncoming = lineageHelper.getVerticesByDirection(piVertex.get_id(),
                Direction.inComingVertices);
            GraphAssert.assertVertexSanity(piIncoming);
            //process input start="now(0,-20) and end is "now(0,0)"
            //frequency of the feed is 5 min so there are 5 instances
            Assert.assertEquals(piIncoming.getTotalSize(), 5);
            final List<String> allowedInpFeedInstDates =
                TimeUtil.getMinuteDatesOnEitherSide(
                    TimeUtil.oozieDateToDate(processInstanceTime).plusMinutes(-20),
                    TimeUtil.oozieDateToDate(processInstanceTime), 5,
                    OozieUtil.getOozieDateTimeFormatter());
            // checking input feed instances
            for(Vertex inFeedInst : piIncoming.filterByType(Vertex.VERTEX_TYPE.FEED_INSTANCE)) {
                final String inFeedInstName = inFeedInst.getName();
                Assert.assertTrue(inFeedInstName.startsWith(inputFeedName),
                    "input feed instances should start with input feed name: " + inFeedInstName);
                final String inFeedInstanceTime = inFeedInstName.substring(
                    inputFeedName.length() + 1);
                logger.info("inFeedInstanceTime = " + inFeedInstanceTime);
                Assert.assertTrue(allowedInpFeedInstDates.remove(inFeedInstanceTime),
                    "Unexpected inFeedInstanceTime: " + inFeedInstanceTime  + " it should have " +
                        "been present in: " + allowedInpFeedInstDates);
            }

            VerticesResult piOutgoing = lineageHelper.getVerticesByDirection(
                piVertex.get_id(), Direction.outgoingVertices);
            GraphAssert.assertVertexSanity(piOutgoing);
            Assert.assertEquals(piOutgoing.filterByType(Vertex.VERTEX_TYPE.FEED_INSTANCE).size(),
                1, "Expected only one output feed instance.");
            // checking output feed instances
            final Vertex outFeedInst = piOutgoing.filterByType(Vertex.VERTEX_TYPE.FEED_INSTANCE).get(0);
            final String outFeedInstName = outFeedInst.getName();
            Assert.assertTrue(outFeedInstName.startsWith(outputFeedName),
                "Expecting outFeedInstName: " + outFeedInstName +
                    " to start with outputFeedName: " + outputFeedName);
            final String outFeedInstanceTime = outFeedInstName.substring(outputFeedName.length() +
                1);
            Assert.assertEquals(outFeedInstanceTime, processInstanceTime,
                "Expecting output feed instance time and process instance time to be same");
        }

    }

}
