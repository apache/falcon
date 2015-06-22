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

import org.apache.commons.httpclient.HttpStatus;
import org.apache.falcon.regression.Entities.ClusterMerlin;
import org.apache.falcon.regression.Entities.FeedMerlin;
import org.apache.falcon.regression.core.bundle.Bundle;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.regression.core.enumsAndConstants.MerlinConstants;
import org.apache.falcon.regression.core.helpers.ColoHelper;
import org.apache.falcon.regression.core.helpers.LineageHelper;
import org.apache.falcon.regression.core.response.lineage.Direction;
import org.apache.falcon.regression.core.response.lineage.Edge;
import org.apache.falcon.regression.core.response.lineage.EdgesResult;
import org.apache.falcon.regression.core.response.lineage.Vertex;
import org.apache.falcon.regression.core.response.lineage.VertexIdsResult;
import org.apache.falcon.regression.core.response.lineage.VertexResult;
import org.apache.falcon.regression.core.response.lineage.VerticesResult;
import org.apache.falcon.regression.core.util.AssertUtil;
import org.apache.falcon.regression.core.util.BundleUtil;
import org.apache.falcon.regression.core.util.Generator;
import org.apache.falcon.regression.core.util.GraphAssert;
import org.apache.falcon.regression.core.util.Util;
import org.apache.falcon.regression.testHelper.BaseTestClass;
import org.apache.http.HttpResponse;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * Tests for Lineage API.
 */
@Test(groups = "lineage-rest")
public class LineageApiTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(LineageApiTest.class);
    private static final String TEST_NAME = "LineageApiTest";
    private static final String TEST_TAG =
        Edge.LabelType.TESTNAME.toString().toLowerCase() + "=" + TEST_NAME;
    private static final String VERTEX_NOT_FOUND_REGEX = ".*Vertex.*%d.*not.*found.*\n?";
    private static final String INVALID_ARGUMENT_STR = "Invalid argument";
    private LineageHelper lineageHelper;
    private final ColoHelper cluster = servers.get(0);
    private final String baseTestHDFSDir = cleanAndGetTestDir();
    private final String feedInputPath = baseTestHDFSDir + "/input";
    private final String feedOutputPath = baseTestHDFSDir + "/output";
    // use 5 <= x < 10 input feeds
    private final int numInputFeeds = 5 + new Random().nextInt(5);
    // use 5 <= x < 10 output feeds
    private final int numOutputFeeds = 5 + new Random().nextInt(5);
    private ClusterMerlin clusterMerlin;
    private FeedMerlin[] inputFeeds;
    private FeedMerlin[] outputFeeds;

    @BeforeClass(alwaysRun = true)
    public void init() {
        lineageHelper = new LineageHelper(prism);
    }

    @BeforeMethod(alwaysRun = true, firstTimeOnly = true)
    public void setUp() throws Exception {
        Bundle bundle = BundleUtil.readELBundle();
        bundle.generateUniqueBundle(this);
        bundles[0] = new Bundle(bundle, cluster);
        final List<String> clusterStrings = bundles[0].getClusters();
        Assert.assertEquals(clusterStrings.size(), 1, "Expecting only 1 clusterMerlin.");
        clusterMerlin = new ClusterMerlin(clusterStrings.get(0));
        clusterMerlin.setTags(TEST_TAG);
        AssertUtil.assertSucceeded(prism.getClusterHelper().submitEntity(clusterMerlin.toString()));
        LOGGER.info("numInputFeeds = " + numInputFeeds);
        LOGGER.info("numOutputFeeds = " + numOutputFeeds);
        final FeedMerlin inputMerlin = new FeedMerlin(bundles[0].getInputFeedFromBundle());
        inputMerlin.setTags(TEST_TAG);
        String namePrefix = Util.getEntityPrefix(this) + '-';
        inputFeeds = generateFeeds(numInputFeeds, inputMerlin,
            Generator.getNameGenerator(namePrefix + "infeed",
                inputMerlin.getName().replace(namePrefix, "")),
            Generator.getHadoopPathGenerator(feedInputPath, MINUTE_DATE_PATTERN));
        for (FeedMerlin feed : inputFeeds) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(feed.toString()));
        }

        FeedMerlin outputMerlin = new FeedMerlin(bundles[0].getOutputFeedFromBundle());
        outputMerlin.setTags(TEST_TAG);
        outputFeeds = generateFeeds(numOutputFeeds, outputMerlin,
            Generator.getNameGenerator(namePrefix + "outfeed",
                outputMerlin.getName().replace(namePrefix, "")),
            Generator.getHadoopPathGenerator(feedOutputPath, MINUTE_DATE_PATTERN));
        for (FeedMerlin feed : outputFeeds) {
            AssertUtil.assertSucceeded(prism.getFeedHelper().submitEntity(feed.toString()));
        }
    }

    public static FeedMerlin[] generateFeeds(final int numInputFeeds,
                                             final FeedMerlin originalFeedMerlin,
                                             final Generator nameGenerator,
                                             final Generator pathGenerator) {
        FeedMerlin[] inputFeeds = new FeedMerlin[numInputFeeds];
        //submit all input feeds
        for(int count = 0; count < numInputFeeds; ++count) {
            final FeedMerlin feed = new FeedMerlin(originalFeedMerlin.toString());
            feed.setName(nameGenerator.generate());
            feed.setLocation(LocationType.DATA, pathGenerator.generate());
            inputFeeds[count] = feed;
        }
        return inputFeeds;
    }

    @AfterMethod(alwaysRun = true, lastTimeOnly = true)
    public void tearDown() {
        removeTestClassEntities();
    }

    /**
     * Get all vertices from falcon and check that they are sane.
     * @throws Exception
     */
    @Test
    public void testAllVertices() throws Exception {
        final VerticesResult verticesResult = lineageHelper.getAllVertices();
        LOGGER.info(verticesResult);
        GraphAssert.assertVertexSanity(verticesResult);
        GraphAssert.assertUserVertexPresence(verticesResult);
        GraphAssert.assertVerticesPresenceMinOccur(verticesResult, Vertex.VERTEX_TYPE.COLO, 1);
        GraphAssert.assertVerticesPresenceMinOccur(verticesResult, Vertex.VERTEX_TYPE.TAGS, 1);
        GraphAssert.assertVerticesPresenceMinOccur(verticesResult, Vertex.VERTEX_TYPE.CLUSTER_ENTITY, 1);
        GraphAssert.assertVerticesPresenceMinOccur(verticesResult,
            Vertex.VERTEX_TYPE.FEED_ENTITY, numInputFeeds + numOutputFeeds);
    }

    /**
     * Get a vertex by id and check results.
     * @throws Exception
     */
    @Test
    public void testVertexId() throws Exception {
        final VerticesResult userResult =
            lineageHelper.getVerticesByName(MerlinConstants.CURRENT_USER_NAME);
        GraphAssert.assertVertexSanity(userResult);
        final int vertexId = userResult.getResults().get(0).getId();
        final VertexResult userVertex =
            lineageHelper.getVertexById(vertexId);
        Assert.assertEquals(userResult.getResults().get(0), userVertex.getResults(),
            "Same vertex should have been returned.");
    }

    /**
     * Negative test - get a vertex without specifying id, we should not get internal server error.
     * @throws Exception
     */
    @Test
    public void testVertexNoId() throws Exception {
        HttpResponse response = lineageHelper.runGetRequest(
            lineageHelper.getUrl(LineageHelper.URL.VERTICES, ""));
        String responseString = lineageHelper.getResponseString(response);
        LOGGER.info("response: " + response);
        LOGGER.info("responseString: " + responseString);
        Assert.assertNotEquals(response.getStatusLine().getStatusCode(),
            HttpStatus.SC_INTERNAL_SERVER_ERROR,
            "We should not get internal server error");
    }

    /**
     * Negative test - get a vertex specifying an invalid id, we should not get http non-found
     * error.
     * @throws Exception
     */
    @Test
    public void testVertexInvalidId() throws Exception {
        final VerticesResult allVerticesResult =
            lineageHelper.getAllVertices();
        GraphAssert.assertVertexSanity(allVerticesResult);
        int invalidVertexId = -1;
        for (Vertex vertex : allVerticesResult.getResults()) {
            if (invalidVertexId <= vertex.getId()) {
                invalidVertexId = vertex.getId() + 1;
            }
        }

        HttpResponse response = lineageHelper.runGetRequest(
            lineageHelper.getUrl(LineageHelper.URL.VERTICES, "" + invalidVertexId));
        String responseString = lineageHelper.getResponseString(response);
        LOGGER.info("response: " + response);
        LOGGER.info("responseString: " + responseString);
        Assert.assertTrue(
            responseString.matches(String.format(VERTEX_NOT_FOUND_REGEX, invalidVertexId)),
            "Unexpected responseString: " + responseString);
        Assert.assertEquals(response.getStatusLine().getStatusCode(),
            HttpStatus.SC_NOT_FOUND,
            "We should get http not found error");
    }

    /**
     * Get properties of one type of vertex and check those properties.
     * @param vertexType type of the vertex that we want to check
     */
    private void checkVertexOneProperty(Vertex.VERTEX_TYPE vertexType) {
        final VerticesResult coloResult = lineageHelper.getVerticesByType(vertexType);
        GraphAssert.assertVertexSanity(coloResult);
        for (Vertex coloVertex : coloResult.getResults()) {
            final int coloVertexId = coloVertex.getId();
            final VertexResult coloProperties = lineageHelper.getVertexProperties(coloVertexId);
            Assert.assertNotNull(coloProperties.getResults().getName(),
                "name should not be null");
            Assert.assertEquals(coloProperties.getResults().getType(), vertexType);
            Assert.assertNotNull(coloProperties.getResults().getTimestamp(),
                "timestamp should not be null");
        }
    }

    /**
     * Test vertex properties for different types of vertices.
     * @throws Exception
     */
    @Test
    public void testVertexProperties() throws Exception {
        //testing properties of a user vertex
        checkVertexOneProperty(Vertex.VERTEX_TYPE.USER);

        //testing properties of colo vertices
        checkVertexOneProperty(Vertex.VERTEX_TYPE.COLO);

        //testing properties of group vertices
        checkVertexOneProperty(Vertex.VERTEX_TYPE.GROUPS);

        //testing properties of group vertices
        //checkVertexOneProperty(Vertex.VERTEX_TYPE.FEED_ENTITY);
    }

    /**
     * Test vertex properties supplying a blank id, expecting http not found error.
     * @throws Exception
     */
    @Test
    public void testVertexPropertiesNoId() throws Exception {
        //testing properties of a user vertex
        HttpResponse response = lineageHelper.runGetRequest(lineageHelper
            .getUrl(LineageHelper.URL.VERTICES_PROPERTIES, lineageHelper.getUrlPath("")));
        String responseString = lineageHelper.getResponseString(response);
        LOGGER.info("response: " + response);
        LOGGER.info("responseString: " + responseString);
        Assert.assertEquals(response.getStatusLine().getStatusCode(),
            HttpStatus.SC_NOT_FOUND, "We should get http not found error");
    }

    /**
     * Test vertex properties supplying an invalid id, expecting http not found error.
     * @throws Exception
     */
    @Test
    public void testVertexPropertiesInvalidId() throws Exception {
        final VerticesResult allVerticesResult =
            lineageHelper.getAllVertices();
        GraphAssert.assertVertexSanity(allVerticesResult);

        int invalidVertexId = -1;
        for (Vertex vertex : allVerticesResult.getResults()) {
            if (invalidVertexId <= vertex.getId()) {
                invalidVertexId = vertex.getId() + 1;
            }
        }

        HttpResponse response = lineageHelper.runGetRequest(
            lineageHelper.getUrl(LineageHelper.URL.VERTICES_PROPERTIES, "" + invalidVertexId));
        String responseString = lineageHelper.getResponseString(response);
        LOGGER.info("response: " + response);
        LOGGER.info("responseString: " + responseString);
        Assert.assertTrue(
            responseString.matches(String.format(VERTEX_NOT_FOUND_REGEX, invalidVertexId)),
            "Unexpected responseString: " + responseString);
        Assert.assertEquals(response.getStatusLine().getStatusCode(),
            HttpStatus.SC_NOT_FOUND,
            "We should get http not found error");
    }

    /**
     * Test filtering vertices by name.
     * @throws Exception
     */
    @Test
    public void testVerticesFilterByName() throws Exception {
        final String clusterName = clusterMerlin.getName();
        final VerticesResult clusterVertices = lineageHelper.getVerticesByName(clusterName);
        GraphAssert.assertVertexSanity(clusterVertices);
        GraphAssert.assertVerticesPresenceMinOccur(clusterVertices,
            Vertex.VERTEX_TYPE.CLUSTER_ENTITY, 1);
        GraphAssert.assertVertexPresence(clusterVertices, clusterName);
        for(int i = 0; i < numInputFeeds; ++i) {
            final String feedName = inputFeeds[i].getName();
            final VerticesResult feedVertices = lineageHelper.getVerticesByName(feedName);
            GraphAssert.assertVertexSanity(feedVertices);
            GraphAssert.assertVerticesPresenceMinOccur(feedVertices,
                Vertex.VERTEX_TYPE.FEED_ENTITY, 1);
            GraphAssert.assertVertexPresence(feedVertices, feedName);
        }
        for(int i = 0; i < numOutputFeeds; ++i) {
            final String feedName = outputFeeds[i].getName();
            final VerticesResult feedVertices = lineageHelper.getVerticesByName(feedName);
            GraphAssert.assertVertexSanity(feedVertices);
            GraphAssert.assertVerticesPresenceMinOccur(feedVertices,
                Vertex.VERTEX_TYPE.FEED_ENTITY, 1);
            GraphAssert.assertVertexPresence(feedVertices, feedName);
        }

    }

    /**
     * Test filtering vertices by type.
     * @throws Exception
     */
    @Test
    public void testVerticesFilterByType() throws Exception {
        final VerticesResult clusterVertices =
            lineageHelper.getVerticesByType(Vertex.VERTEX_TYPE.CLUSTER_ENTITY);
        GraphAssert.assertVertexSanity(clusterVertices);
        GraphAssert.assertVerticesPresenceMinOccur(clusterVertices,
            Vertex.VERTEX_TYPE.CLUSTER_ENTITY, 1);
        GraphAssert.assertVertexPresence(clusterVertices, clusterMerlin.getName());
        final VerticesResult feedVertices =
            lineageHelper.getVerticesByType(Vertex.VERTEX_TYPE.FEED_ENTITY);
        GraphAssert.assertVertexSanity(feedVertices);
        GraphAssert.assertVerticesPresenceMinOccur(feedVertices,
            Vertex.VERTEX_TYPE.FEED_ENTITY, 1);
        for (FeedMerlin oneFeed : inputFeeds) {
            GraphAssert.assertVertexPresence(feedVertices, oneFeed.getName());
        }
        for (FeedMerlin oneFeed : outputFeeds) {
            GraphAssert.assertVertexPresence(feedVertices, oneFeed.getName());
        }
    }

    /**
     * Test filtering vertices when no output is produced.
     * @throws Exception
     */
    @Test
    public void testVerticesFilterNoOutput() throws Exception {
        final String nonExistingName = "this-is-a-non-existing-name";
        final VerticesResult clusterVertices = lineageHelper.getVerticesByName(nonExistingName);
        GraphAssert.assertVertexSanity(clusterVertices);
        Assert.assertEquals(clusterVertices.getTotalSize(), 0,
            "Result should not contain any vertex");
    }

    @Test
    public void testVerticesFilterBlankValue() throws Exception {
        Map<String, String> params = new TreeMap<>();
        params.put("key", Vertex.FilterKey.name.toString());
        params.put("value", "");
        HttpResponse response = lineageHelper
            .runGetRequest(lineageHelper.getUrl(LineageHelper.URL.VERTICES, params));
        String responseString = lineageHelper.getResponseString(response);
        LOGGER.info(responseString);
        Assert.assertEquals(response.getStatusLine().getStatusCode(),
            HttpStatus.SC_BAD_REQUEST,
            "The get request was a bad request");
        Assert.assertTrue(responseString.contains(INVALID_ARGUMENT_STR),
            "Result should contain string Invalid argument");
    }

    @Test
    public void testVerticesFilterBlankKey() throws Exception {
        Map<String, String> params = new TreeMap<>();
        params.put("key", "");
        params.put("value", "someValue");
        HttpResponse response = lineageHelper.runGetRequest(
            lineageHelper.getUrl(LineageHelper.URL.VERTICES, params));
        String responseString = lineageHelper.getResponseString(response);
        LOGGER.info(responseString);
        Assert.assertEquals(response.getStatusLine().getStatusCode(),
            HttpStatus.SC_BAD_REQUEST,
            "The get request was a bad request");
        Assert.assertTrue(responseString.contains(INVALID_ARGUMENT_STR),
            "Result should contain string Invalid argument");
    }

    @Test
    public void testVertexDirectionFetchEdges() throws Exception {
        final int clusterVertexId = lineageHelper.getVertex(clusterMerlin.getName()).getId();

        final EdgesResult bothEdges =
            lineageHelper.getEdgesByDirection(clusterVertexId, Direction.bothEdges);
        GraphAssert.assertEdgeSanity(bothEdges);
        Assert.assertEquals(bothEdges.filterByType(Edge.LabelType.STORED_IN).size(),
            inputFeeds.length + outputFeeds.length,
            "Expecting edge between the cluster and inputFeeds, outputFeeds");
        Assert.assertEquals(bothEdges.filterByType(Edge.LabelType.CLUSTER_COLO).size(),
            1, "Expecting an edge from the cluster to colo");
        Assert.assertEquals(bothEdges.getTotalSize(), inputFeeds.length + outputFeeds.length + 3,
            "Expecting edge from the cluster to inputFeeds & outputFeeds,"
                + "one between cluster and owner, one between cluster and colo and one between cluster and tags");

        final EdgesResult inComingEdges =
            lineageHelper.getEdgesByDirection(clusterVertexId, Direction.inComingEdges);
        GraphAssert.assertEdgeSanity(inComingEdges);
        Assert.assertEquals(inComingEdges.getTotalSize(), inputFeeds.length + outputFeeds.length,
            "Expecting edge from the cluster to inputFeeds & outputFeeds");
        Assert.assertEquals(inComingEdges.filterByType(Edge.LabelType.STORED_IN).size(),
            inputFeeds.length + outputFeeds.length,
            "Expecting edge from the cluster to inputFeeds & outputFeeds");


        final EdgesResult outGoingEdges =
            lineageHelper.getEdgesByDirection(clusterVertexId, Direction.outGoingEdges);
        GraphAssert.assertEdgeSanity(outGoingEdges);
        Assert.assertEquals(outGoingEdges.filterByType(Edge.LabelType.CLUSTER_COLO).size(),
            1, "Expecting an edge from the cluster to colo");
        Assert.assertEquals(outGoingEdges.filterByType(Edge.LabelType.TESTNAME).size(),
            1, "Expecting an edge from the cluster to classification");
        Assert.assertEquals(outGoingEdges.getTotalSize(), 3,
            "Expecting one edge between cluster and owner, one between cluster and colo and "
                + "one between cluster and tags");
    }

    @Test
    public void testVertexCountsFetchVertices() throws Exception {
        final int clusterVertexId = lineageHelper.getVertex(clusterMerlin.getName()).getId();

        final VerticesResult bothVertices =
            lineageHelper.getVerticesByDirection(clusterVertexId, Direction.bothVertices);
        GraphAssert.assertVertexSanity(bothVertices);
        Assert.assertEquals(bothVertices.filterByType(Vertex.VERTEX_TYPE.FEED_ENTITY).size(),
            inputFeeds.length + outputFeeds.length,
            "Expecting edge from the cluster to inputFeeds & outputFeeds");
        Assert.assertEquals(bothVertices.filterByType(Vertex.VERTEX_TYPE.COLO).size(), 1,
            "The should be one edge between cluster and colo");
        Assert.assertEquals(bothVertices.getTotalSize(),
            inputFeeds.length + outputFeeds.length + 3,
            "Expecting edge from the cluster to inputFeeds & outputFeeds, "
                + "one between cluster and owner, one between cluster and colo and one between cluster and tags");

        final VerticesResult inComingVertices =
            lineageHelper.getVerticesByDirection(clusterVertexId, Direction.inComingVertices);
        GraphAssert.assertVertexSanity(inComingVertices);
        Assert.assertEquals(inComingVertices.filterByType(Vertex.VERTEX_TYPE.FEED_ENTITY).size(),
            inputFeeds.length + outputFeeds.length,
            "Expecting edge from the cluster to inputFeeds & outputFeeds");
        Assert.assertEquals(inComingVertices.getTotalSize(),
            inputFeeds.length + outputFeeds.length,
            "Expecting edge from the cluster to inputFeeds & outputFeeds and one "
                + "between cluster and colo");

        final VerticesResult outgoingVertices =
            lineageHelper.getVerticesByDirection(clusterVertexId, Direction.outgoingVertices);
        GraphAssert.assertVertexSanity(outgoingVertices);
        Assert.assertEquals(outgoingVertices.filterByType(Vertex.VERTEX_TYPE.COLO).size(), 1,
            "The should be one edge between cluster and colo");
        Assert.assertEquals(outgoingVertices.filterByName(TEST_NAME).size(),
            1, "Expecting an edge from the cluster to classification");
        Assert.assertEquals(outgoingVertices.getTotalSize(), 3,
            "Expecting one edge between cluster and owner, one between cluster and colo and "
                + "one between cluster and tags");
    }

    @Test
    public void testVertexDirectionFetchCounts() throws Exception {
        final int clusterVertexId = lineageHelper.getVertex(clusterMerlin.getName()).getId();

        final VerticesResult bothCount =
            lineageHelper.getVerticesByDirection(clusterVertexId, Direction.bothCount);
        Assert.assertEquals(bothCount.getTotalSize(),
            inputFeeds.length + outputFeeds.length + 1 + 2,
            "Expecting edge from the cluster to inputFeeds & outputFeeds, "
                + "one between cluster and owner, one between cluster and colo and one between cluster and tags");

        final VerticesResult inCount =
            lineageHelper.getVerticesByDirection(clusterVertexId, Direction.inCount);
        Assert.assertEquals(inCount.getTotalSize(),
            inputFeeds.length + outputFeeds.length,
            "Expecting edge from the cluster to inputFeeds & outputFeeds and one "
                + "between cluster and colo");

        final VerticesResult outCount =
            lineageHelper.getVerticesByDirection(clusterVertexId, Direction.outCount);
        Assert.assertEquals(outCount.getTotalSize(), 3,
            "Expecting one edge between cluster and owner, one between cluster and "
                + "colo and one between cluster and tags");
    }

    @Test
    public void testVertexDirectionFetchVertexIds() throws Exception {
        final int clusterVertexId = lineageHelper.getVertex(clusterMerlin.getName()).getId();

        final VertexIdsResult bothVerticesIds =
            lineageHelper.getVertexIdsByDirection(clusterVertexId, Direction.bothVerticesIds);
        for (Integer vertexId : bothVerticesIds.getResults()) {
            Assert.assertTrue(vertexId > 0, "Vertex id should be valid.");
        }
        Assert.assertEquals(bothVerticesIds.getTotalSize(),
            inputFeeds.length + outputFeeds.length + 3,
            "Expecting edge from the cluster to inputFeeds & outputFeeds,"
                + " one between cluster and owner, one between cluster and colo and one between cluster and tags");

        final VertexIdsResult incomingVerticesIds =
            lineageHelper.getVertexIdsByDirection(clusterVertexId, Direction.incomingVerticesIds);
        for (Integer vertexId : incomingVerticesIds.getResults()) {
            Assert.assertTrue(vertexId > 0, "Vertex id should be valid.");
        }
        Assert.assertEquals(incomingVerticesIds.getTotalSize(),
            inputFeeds.length + outputFeeds.length,
            "Expecting edge from the cluster to inputFeeds & outputFeeds, "
                + "one between cluster and owner, one between cluster and colo and one between cluster and tags");

        final VertexIdsResult outgoingVerticesIds =
            lineageHelper.getVertexIdsByDirection(clusterVertexId, Direction.outgoingVerticesIds);
        for (Integer vertexId : outgoingVerticesIds.getResults()) {
            Assert.assertTrue(vertexId > 0, "Vertex id should be valid.");
        }
        Assert.assertEquals(outgoingVerticesIds.getTotalSize(), 3,
            "Expecting one edge between cluster and owner, one between cluster and colo and "
                + "one between cluster and tags");
    }

    @Test
    public void testVertexBadDirection() throws Exception {
        final int clusterVertexId = lineageHelper.getVertex(clusterMerlin.getName()).getId();

        HttpResponse response = lineageHelper
            .runGetRequest(lineageHelper.getUrl(LineageHelper.URL.VERTICES,
                lineageHelper.getUrlPath(clusterVertexId, "badDirection")));
        final String responseString = lineageHelper.getResponseString(response);
        LOGGER.info("response: " + response);
        LOGGER.info("responseString: " + responseString);
        Assert.assertEquals(response.getStatusLine().getStatusCode(),
            HttpStatus.SC_BAD_REQUEST,
            "We should not get internal server error");
    }

    @Test
    public void testAllEdges() throws Exception {
        final EdgesResult edgesResult = lineageHelper.getAllEdges();
        LOGGER.info(edgesResult);
        Assert.assertTrue(edgesResult.getTotalSize() > 0, "Total number of edges should be"
            + " greater that zero but is: " + edgesResult.getTotalSize());
        GraphAssert.assertEdgeSanity(edgesResult);
        GraphAssert.assertEdgePresenceMinOccur(edgesResult, Edge.LabelType.CLUSTER_COLO, 1);
        GraphAssert.assertEdgePresenceMinOccur(edgesResult, Edge.LabelType.STORED_IN,
            numInputFeeds + numOutputFeeds);
        GraphAssert.assertEdgePresenceMinOccur(edgesResult, Edge.LabelType.OWNED_BY,
            1 + numInputFeeds + numOutputFeeds);
    }

    @Test
    public void testEdge() throws Exception {
        final int clusterVertexId = lineageHelper.getVertex(clusterMerlin.getName()).getId();
        final EdgesResult outGoingEdges =
            lineageHelper.getEdgesByDirection(clusterVertexId, Direction.outGoingEdges);
        GraphAssert.assertEdgeSanity(outGoingEdges);
        Assert.assertEquals(outGoingEdges.filterByType(Edge.LabelType.CLUSTER_COLO).size(),
            1, "Expecting an edge from the cluster to colo");

        final String clusterColoEdgeId =
            outGoingEdges.filterByType(Edge.LabelType.CLUSTER_COLO).get(0).getId();
        final Edge clusterColoEdge =
            lineageHelper.getEdgeById(clusterColoEdgeId).getResults();
        GraphAssert.assertEdgeSanity(clusterColoEdge);
    }

    @Test
    public void testEdgeBlankId() throws Exception {
        final HttpResponse httpResponse = lineageHelper.runGetRequest(
            lineageHelper.getUrl(LineageHelper.URL.EDGES, lineageHelper.getUrlPath("")));
        LOGGER.info(httpResponse.toString());
        LOGGER.info(lineageHelper.getResponseString(httpResponse));
        Assert.assertEquals(httpResponse.getStatusLine().getStatusCode(),
            HttpStatus.SC_NOT_FOUND,
            "Expecting not-found error.");
    }

    @Test
    public void testEdgeInvalidId() throws Exception {
        final HttpResponse response = lineageHelper.runGetRequest(
            lineageHelper.getUrl(LineageHelper.URL.EDGES, lineageHelper.getUrlPath("invalid-id")));
        LOGGER.info(response.toString());
        LOGGER.info(lineageHelper.getResponseString(response));
        Assert.assertEquals(response.getStatusLine().getStatusCode(),
            HttpStatus.SC_NOT_FOUND,
            "Expecting not-found error.");
    }

    @Test
    public void testColoToClusterNode() throws Exception {
        final VerticesResult verticesResult = lineageHelper.getVerticesByType(
            Vertex.VERTEX_TYPE.COLO);
        GraphAssert.assertVertexSanity(verticesResult);
        Assert.assertTrue(verticesResult.getTotalSize() > 0, "Expected at least 1 colo node");
        Assert.assertTrue(verticesResult.getTotalSize() <= 3, "Expected at most 3 colo nodes");
        final List<Vertex> colo1Vertex = verticesResult.filterByName(clusterMerlin.getColo());
        AssertUtil.checkForListSize(colo1Vertex, 1);
        Vertex coloVertex = colo1Vertex.get(0);
        LOGGER.info("coloVertex: " + coloVertex);
        final VerticesResult verticesByDirection =
            lineageHelper.getVerticesByDirection(coloVertex.getId(), Direction.inComingVertices);
        AssertUtil.checkForListSize(
            verticesByDirection.filterByName(clusterMerlin.getName()), 1);
    }

    @Test
    public void testClusterNodeToFeedNode() throws Exception {
        final VerticesResult clusterResult = lineageHelper.getVerticesByName(
            clusterMerlin.getName());
        GraphAssert.assertVertexSanity(clusterResult);
        Vertex clusterVertex = clusterResult.getResults().get(0);
        final VerticesResult clusterIncoming =
            lineageHelper.getVerticesByDirection(clusterVertex.getId(), Direction.inComingVertices);
        GraphAssert.assertVertexSanity(clusterIncoming);
        for(FeedMerlin feed : inputFeeds) {
            AssertUtil.checkForListSize(clusterIncoming.filterByName(feed.getName()), 1);
        }
        for(FeedMerlin feed : outputFeeds) {
            AssertUtil.checkForListSize(clusterIncoming.filterByName(feed.getName()), 1);
        }
    }

    @Test
    public void testUserToEntityNode() throws Exception {
        final VerticesResult userResult = lineageHelper.getVerticesByName(
            MerlinConstants.CURRENT_USER_NAME);
        GraphAssert.assertVertexSanity(userResult);
        Vertex clusterVertex = userResult.getResults().get(0);
        final VerticesResult userIncoming =
            lineageHelper.getVerticesByDirection(clusterVertex.getId(), Direction.inComingVertices);
        GraphAssert.assertVertexSanity(userIncoming);
        for(FeedMerlin feed : inputFeeds) {
            AssertUtil.checkForListSize(userIncoming.filterByName(feed.getName()), 1);
        }
        for(FeedMerlin feed : outputFeeds) {
            AssertUtil.checkForListSize(userIncoming.filterByName(feed.getName()), 1);
        }
    }
}
