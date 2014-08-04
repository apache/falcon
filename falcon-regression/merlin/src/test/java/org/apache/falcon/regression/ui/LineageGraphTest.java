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

package org.apache.falcon.regression.ui;

import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.LineageHelper;
import org.apache.falcon.regression.core.response.lineage.Direction;
import org.apache.falcon.regression.core.response.lineage.Edge;
import org.apache.falcon.regression.core.response.lineage.Vertex;
import org.apache.falcon.regression.core.response.lineage.VerticesResult;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.HadoopUtil;
import org.apache.falcon.regression.core.util.InstanceUtil;
import org.apache.falcon.regression.core.util.OSUtil;
import org.apache.falcon.regression.core.util.TimeUtil;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseUITestClass;
import org.apache.falcon.regression.ui.pages.ProcessPage;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.log4j.Logger;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.openqa.selenium.Point;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Test(groups = "lineage-ui")
public class LineageGraphTest extends BaseUITestClass {

    private ColoHelper cluster = servers.get(0);
    private String baseTestDir = baseHDFSDir + "/LineageGraphTest";
    private String aggregateWorkflowDir = baseTestDir + "/aggregator";
    private static final Logger logger = Logger.getLogger(LineageGraphTest.class);
    String datePattern = "/${YEAR}/${MONTH}/${DAY}/${HOUR}/${MINUTE}";
    String feedInputPath = baseTestDir + "/input" + datePattern;
    private FileSystem clusterFS = serverFS.get(0);
    private OozieClient clusterOC = serverOC.get(0);
    private String processName = null;
    private String inputFeedName = null;
    private String outputFeedName = null;
    int inputEnd = 4;
    private List<Vertex> piVertices;
    LineageHelper lineageHelper = new LineageHelper(prism);

    /**
     * Adjusts bundle and schedules it. Provides process with data, waits till some instances got
     * succeeded.
     */
    @BeforeClass
    public void setUp()
        throws IOException, JAXBException, URISyntaxException, AuthenticationException,
        OozieClientException {
        uploadDirToClusters(aggregateWorkflowDir, OSUtil.RESOURCES_OOZIE);
        bundles[0] = BundleUtil.readELBundle();
        bundles[0] = new Bundle(bundles[0], cluster);
        bundles[0].generateUniqueBundle();
        bundles[0].setProcessWorkflow(aggregateWorkflowDir);
        String startTime = TimeUtil.getTimeWrtSystemTime(0);
        String endTime = TimeUtil.addMinsToTime(startTime, 5);
        logger.info("Start time: " + startTime + "\tEnd time: " + endTime);

        /**prepare process definition*/
        bundles[0].setProcessValidity(startTime, endTime);
        bundles[0].setProcessPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setProcessConcurrency(5);
        bundles[0].setInputFeedPeriodicity(1, Frequency.TimeUnit.minutes);
        bundles[0].setInputFeedDataPath(feedInputPath);
        bundles[0].setProcessInput("now(0,0)", String.format("now(0,%d)", inputEnd - 1));

        /**provide necessary data for first 3 instances to run*/
        logger.info("Creating necessary data...");
        String prefix = bundles[0].getFeedDataPathPrefix();
        HadoopUtil.deleteDirIfExists(prefix.substring(1), clusterFS);
        List<String> dataDates = TimeUtil.getMinuteDatesOnEitherSide(
            TimeUtil.addMinsToTime(startTime, -2), endTime, 0);
        HadoopUtil.flattenAndPutDataInFolder(clusterFS, OSUtil.NORMAL_INPUT, prefix, dataDates);
        logger.info("Process data: " + Util.prettyPrintXml(bundles[0].getProcessData()));
        bundles[0].submitBundle(prism);

        processName = bundles[0].getProcessName();
        inputFeedName = bundles[0].getInputFeedNameFromBundle();
        outputFeedName = bundles[0].getOutputFeedNameFromBundle();
        /**schedule process, wait for instances to succeed*/
        prism.getProcessHelper().schedule(Util.URLS.SCHEDULE_URL, bundles[0].getProcessData());
        InstanceUtil.waitTillInstanceReachState(clusterOC, bundles[0].getProcessName(), 3,
            CoordinatorAction.Status.SUCCEEDED, EntityType.PROCESS);
        /**get process instances*/
        Vertex processVertex = lineageHelper.getVerticesByName(processName).getResults().get(0);
        piVertices = lineageHelper.getVerticesByDirection(processVertex.get_id(),
            Direction.inComingVertices).filterByType(Vertex.VERTEX_TYPE.PROCESS_INSTANCE);
        openBrowser();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown() throws IOException {
        closeBrowser();
        removeBundles();
    }

    /**
     * Tests the number of vertices on graph and if they match to expected number of instances
     * and their description.
     */
    @Test
    public void testGraphVertices() {

        ProcessPage processPage = new ProcessPage(DRIVER, cluster, processName);
        processPage.navigateTo();
        for (Vertex piVertex : piVertices) {
            String nominalTime = piVertex.getNominalTime();
            /* get expected feed instances */
            /* input feed instances */
            List<Vertex> inpInstancesAPI = lineageHelper.getVerticesByDirection(piVertex.get_id(),
                Direction.inComingVertices).getResults();
            /* output feed instance */
            List<Vertex> outInstancesAPI = lineageHelper.getVerticesByDirection(piVertex.get_id(),
                Direction.outgoingVertices).filterByType(Vertex.VERTEX_TYPE.FEED_INSTANCE);
            /* open lineage for particular process instance */
            processPage.openLineage(nominalTime);
            /* verify if number of vertices and their content is correct */
            HashMap<String, List<String>> map = processPage.getAllVertices();
            Assert.assertTrue(map.containsKey(processName) && map.containsKey(inputFeedName)
                && map.containsKey(outputFeedName));
            /* process validation */
            List<String> processInstancesUI = map.get(processName);
            Assert.assertEquals(processInstancesUI.size(), 1);
            Assert.assertEquals(processInstancesUI.get(0), nominalTime);
            /* input feed validations */
            List<String> inpInstancesUI = map.get(inputFeedName);
            logger.info("InputFeed instances on lineage UI : " + inpInstancesUI);
            logger.info("InputFeed instances from API : " + inpInstancesAPI);
            Assert.assertEquals(inpInstancesUI.size(), inpInstancesAPI.size());
            for (Vertex inpInstanceAPI : inpInstancesAPI) {
                Assert.assertTrue(inpInstancesUI.contains(inpInstanceAPI.getNominalTime()));
            }
            /* output feed validation */
            List<String> outInstancesUI = map.get(outputFeedName);
            logger.info("Expected outputFeed instances : " + outInstancesUI);
            logger.info("Actual instance : " + outInstancesAPI);
            Assert.assertEquals(outInstancesUI.size(), outInstancesAPI.size());
            for (Vertex outInstanceAPI : outInstancesAPI) {
                Assert.assertTrue(outInstancesUI.contains(outInstanceAPI.getNominalTime()));
            }
            processPage.refresh();
        }
    }

    /**
     * Clicks on each vertex and check the content of info panel
     */
    @Test
    public void testVerticesInfo()
        throws JAXBException, URISyntaxException, AuthenticationException, IOException {
        String clusterName = Util.readEntityName(bundles[0].getClusters().get(0));
        ProcessPage processPage = new ProcessPage(DRIVER, cluster, processName);
        processPage.navigateTo();
        for (Vertex piVertex : piVertices) {
            String nominalTime = piVertex.getNominalTime();
            /**open lineage for particular process instance*/
            processPage.openLineage(nominalTime);
            HashMap<String, List<String>> map = processPage.getAllVertices();
            /**click on each vertex and check the bottom info*/
            for (Map.Entry<String, List<String>> entry : map.entrySet()) {
                String entityName = entry.getKey();
                List<String> entityInstances = entry.getValue();
                for (String entityInstance : entityInstances) {
                    processPage.clickOnVertex(entityName, entityInstance);
                    HashMap<String, String> info = processPage.getPanelInfo();
                    if (entityName.equals(processName)) {
                        String message = "Lineage info-panel reflects invalid %s for process %s.";
                        String workflow = processName + "-workflow";
                        Assert.assertEquals(info.get("User workflow"), workflow,
                            String.format(message, "workflow", processName));
                        Assert.assertEquals(info.get("User workflow engine"), "oozie",
                            String.format(message, "engine", processName));
                        Assert.assertEquals(info.get("Runs on"), clusterName,
                            String.format(message, "cluster", processName));
                    }
                    Assert.assertEquals(info.get("Owned by"), System.getProperty("user" +
                        ".name"), "Entity should be owned by current system user.");
                }
            }
            processPage.refresh();
        }
    }

    /**
     * Tests available titles and descriptions of different lineage sections.
     */
    @Test
    public void testTitlesAndDescriptions() {
        HashMap<String, String> expectedDescriptions = new HashMap<String, String>();
        expectedDescriptions.put("lineage-legend-process-inst", "Process instance");
        expectedDescriptions.put("lineage-legend-process-inst lineage-legend-terminal",
            "Process instance (terminal)");
        expectedDescriptions.put("lineage-legend-feed-inst", "Feed instance");
        expectedDescriptions.put("lineage-legend-feed-inst lineage-legend-terminal",
            "Feed instance (terminal)");
        ProcessPage processPage = new ProcessPage(DRIVER, prism, processName);
        processPage.navigateTo();
        for (Vertex piVertex : piVertices) {
            String nominalTime = piVertex.getNominalTime();
            processPage.openLineage(nominalTime);
            /* check the main lineage title */
            Assert.assertEquals(processPage.getLineageTitle(), "Lineage information");
            /* check legends title */
            Assert.assertEquals(processPage.getLegendsTitle(), "Legends");
            /* check that all legends are present and match to expected*/
            HashMap<String, String> legends = processPage.getLegends();
            for (Map.Entry<String, String> entry : legends.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                Assert.assertEquals(expectedDescriptions.get(key), value);
            }
            processPage.refresh();
        }
    }

    /**
     * Tests whether vertices are terminals or not.
     */
    @Test
    public void testTerminals() {
        ProcessPage processPage = new ProcessPage(DRIVER, prism, processName);
        processPage.navigateTo();
        lineageHelper = new LineageHelper(prism);
        VerticesResult processResult = lineageHelper.getVerticesByName(processName);
        Vertex processVertex = processResult.getResults().get(0);
        List<Vertex> piVertices =
            lineageHelper.getVerticesByDirection(processVertex.get_id(),
                Direction.inComingVertices).filterByType(Vertex.VERTEX_TYPE.PROCESS_INSTANCE);
        for (Vertex piVertex : piVertices) {
            String nominalTime = piVertex.getNominalTime();
            processPage.openLineage(nominalTime);
            List<Vertex> inVertices = lineageHelper.getVerticesByDirection(piVertex.get_id(),
                Direction.inComingVertices).getResults();
            List<Vertex> outVertices = lineageHelper.getVerticesByDirection(piVertex.get_id(),
                Direction.outgoingVertices).filterByType(Vertex.VERTEX_TYPE.FEED_INSTANCE);
            for (Vertex inVertex : inVertices) {
                Assert.assertTrue(processPage.isTerminal(inVertex.getName()),
                    String.format("Input feed instance vertex %s should be terminal",
                        inVertex.getName()));
            }
            for (Vertex outVertex : outVertices) {
                Assert.assertTrue(processPage.isTerminal(outVertex.getName()),
                    String.format("Output feed instance vertex %s should be terminal",
                        outVertex.getName()));
            }
            Assert.assertFalse(processPage.isTerminal(piVertex.getName()),
                String.format("Process instance vertex %s should be non-terminal",
                    piVertex.getName()));
            processPage.refresh();
        }
    }

    /**
     * Evaluates expected number of edges and their endpoints and compares them with
     * endpoints of edges which were retrieved from lineage graph.
     */
    @Test
    public void testEdges() {
        ProcessPage processPage = new ProcessPage(DRIVER, prism, processName);
        processPage.navigateTo();
        for (Vertex piVertex : piVertices) {
            String nominalTime = piVertex.getNominalTime();
            processPage.openLineage(nominalTime);
            /**get expected edges */
            List<Edge> expectedEdgesAPI = new ArrayList<Edge>();
            List<Edge> incEdges = lineageHelper.getEdgesByDirection(piVertex.get_id(),
                Direction.inComingEdges).getResults();
            List<Edge> outcEdges = lineageHelper.getEdgesByDirection(piVertex.get_id(),
                Direction.outGoingEdges).filterByType(Edge.LEBEL_TYPE.OUTPUT);
            assert expectedEdgesAPI.addAll(incEdges);
            assert expectedEdgesAPI.addAll(outcEdges);
            /** Check the number of edges and their location*/
            List<Point[]> edgesOnUI = processPage.getEdgesFromGraph();
            /**check the number of edges on UI*/
            Assert.assertEquals(edgesOnUI.size(), expectedEdgesAPI.size());
            /**check if all appropriate edges match each other*/
            List<Point[]> edgesFromGraph = processPage.getEdgesFromGraph();
            boolean isEdgePresent = false;
            int vertexRadius = processPage.getCircleRadius();
            for (Edge expEdgeAPI : expectedEdgesAPI) {
                Vertex startVertexAPI = lineageHelper.getVertexById(expEdgeAPI.get_outV())
                    .getResults();
                Point startPointAPI = processPage.getVertexEndpoint(startVertexAPI.getName());
                Vertex endVertexAPI = lineageHelper.getVertexById(expEdgeAPI.get_inV())
                    .getResults();
                Point endPointAPI = processPage.getVertexEndpoint(endVertexAPI.getName());
                for (Point[] actualEndpoints : edgesFromGraph) {
                    Point startPointUI = actualEndpoints[0];
                    Point endPointUI = actualEndpoints[1];
                    isEdgePresent =
                        isPointNearTheVertex(startPointAPI, vertexRadius, startPointUI, 5)
                            && isPointNearTheVertex(endPointAPI, vertexRadius, endPointUI, 5);
                    if (isEdgePresent) break;
                }
                Assert.assertTrue(
                    isEdgePresent, String.format("Edge %s-->%s isn't present on lineage or " +
                    "painted incorrectly.", startVertexAPI.getName(), endVertexAPI.getName()));
            }
            processPage.refresh();
        }
    }

    /**
     * Test which opens and closes Lineage info and checks content of it
     */
    @Test
    public void testLineageOpenClose() {
        ProcessPage processPage = new ProcessPage(DRIVER, prism, processName);
        processPage.navigateTo();
        List<String> previous = new ArrayList<String>();
        for (Vertex piVertex : piVertices) {
            String nominalTime = piVertex.getNominalTime();
            processPage.openLineage(nominalTime);
            List<String> vertices = processPage.getAllVerticesNames();
            Assert.assertNotEquals(previous, vertices, "Graph of " + nominalTime + " instance is "
                + "equal to previous");
            previous = vertices;
            processPage.closeLineage();
        }
    }

    /**
     * Evaluates if endpoint is in permissible region near the vertex
     *
     * @param center    coordinates of vertex center
     * @param radius    radius of vertex
     * @param deviation permissible deviation
     */
    private boolean isPointNearTheVertex(Point center, int radius, Point point, int deviation) {
        double distance = Math.sqrt(
            Math.pow(point.getX() - center.getX(), 2) +
                Math.pow(point.getY() - center.getY(), 2));
        return distance <= radius + deviation;
    }
}
