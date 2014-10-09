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

package org.apache.falcon.resource.metadata;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.falcon.metadata.RelationshipLabel;
import org.apache.falcon.metadata.RelationshipProperty;
import org.apache.falcon.service.Services;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowJobEndNotificationService;
import org.json.simple.JSONValue;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for org.apache.falcon.resource.metadata.LineageMetadataResource.
 */
public class LineageMetadataResourceTest {

    private MetadataTestContext testContext;

    @BeforeClass
    public void setUp() throws Exception {
        testContext = new MetadataTestContext();
        testContext.setUp();
    }

    @AfterClass
    public void tearDown() throws Exception {
        testContext.tearDown();
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testGetVerticesWithInvalidId() throws Exception {
        LineageMetadataResource resource = new LineageMetadataResource();
        Response response = resource.getVertex("blah");
        Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
        Assert.assertEquals(response.getEntity().toString(), "Vertex with [blah] cannot be found.");
    }

    @Test
    public void testGetVerticesWithId() throws Exception {
        LineageMetadataResource resource = new LineageMetadataResource();
        Vertex vertex = resource.getGraph().getVertices(RelationshipProperty.NAME.getName(),
                MetadataTestContext.PROCESS_ENTITY_NAME + "/2014-01-01T01:00Z").iterator().next();

        Response response = resource.getVertex(String.valueOf(vertex.getId()));
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Map vertexProperties = (Map) results.get(LineageMetadataResource.RESULTS);
        Assert.assertEquals(vertexProperties.get("_id"), vertex.getId());
        assertBasicVertexProperties(vertex, vertexProperties);
    }

    @Test
    public void testGetVertexPropertiesForProcessInstance() throws Exception {
        String processInstance = MetadataTestContext.PROCESS_ENTITY_NAME + "/2014-01-01T01:00Z";
        LineageMetadataResource resource = new LineageMetadataResource();
        Vertex vertex = resource.getGraph().getVertices(
                RelationshipProperty.NAME.getName(), processInstance).iterator().next();

        Response response = resource.getVertexProperties(String.valueOf(vertex.getId()), "true");
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Map vertexProperties = (Map) results.get(LineageMetadataResource.RESULTS);
        assertBasicVertexProperties(vertex, vertexProperties);

        assertWorkflowProperties(vertex, vertexProperties);
        assertProcessInstanceRelationships(vertexProperties);
    }

    @Test
    public void testGetVertexPropertiesForFeedInstance() throws Exception {
        String feedInstance = "impression-feed/2014-01-01T00:00Z";
        LineageMetadataResource resource = new LineageMetadataResource();
        Vertex vertex = resource.getGraph().getVertices(
                RelationshipProperty.NAME.getName(), feedInstance).iterator().next();

        Response response = resource.getVertexProperties(String.valueOf(vertex.getId()), "true");
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Map vertexProperties = (Map) results.get(LineageMetadataResource.RESULTS);
        assertBasicVertexProperties(vertex, vertexProperties);

        assertFeedInstanceRelationships(vertexProperties, false);
    }

    @Test
    public void testGetVertexPropertiesForFeedInstanceNoTags() throws Exception {
        String feedInstance = "clicks-feed/2014-01-01T00:00Z";
        LineageMetadataResource resource = new LineageMetadataResource();
        Vertex vertex = resource.getGraph().getVertices(
                RelationshipProperty.NAME.getName(), feedInstance).iterator().next();

        Response response = resource.getVertexProperties(String.valueOf(vertex.getId()), "true");
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Map vertexProperties = (Map) results.get(LineageMetadataResource.RESULTS);
        assertBasicVertexProperties(vertex, vertexProperties);

        assertFeedInstanceRelationships(vertexProperties, true);
    }

    @Test
    public void testGetVerticesWithKeyValue() throws Exception {
        String processInstance = MetadataTestContext.PROCESS_ENTITY_NAME + "/2014-01-01T01:00Z";
        LineageMetadataResource resource = new LineageMetadataResource();
        Vertex vertex = resource.getGraph().getVertices(
                RelationshipProperty.NAME.getName(), processInstance).iterator().next();

        Response response = resource.getVertices(RelationshipProperty.NAME.getName(), processInstance);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Map vertexProperties = (Map) ((ArrayList) results.get(LineageMetadataResource.RESULTS)).get(0);
        Assert.assertEquals(vertexProperties.get("_id"), vertex.getId());
        assertBasicVertexProperties(vertex, vertexProperties);
    }

    @Test
    public void testGetVerticesWithInvalidKeyValue() throws Exception {
        LineageMetadataResource resource = new LineageMetadataResource();
        try {
            resource.getVertices(null, null);
        } catch(WebApplicationException e) {
            Assert.assertEquals(e.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
            Assert.assertEquals(e.getResponse().getEntity().toString(),
                    "Invalid argument: key or value passed is null or empty.");
        }
    }

    @Test
    public void testVertexEdgesForIdAndDirectionOut() throws Exception {
        String processInstance = MetadataTestContext.PROCESS_ENTITY_NAME + "/2014-01-01T01:00Z";
        LineageMetadataResource resource = new LineageMetadataResource();
        Vertex vertex = resource.getGraph().getVertices(
                RelationshipProperty.NAME.getName(), processInstance).iterator().next();
        String vertexId = String.valueOf(vertex.getId());

        int expectedSize = 7; // 2 output feeds, user, cluster, process entity, tag, pipeline
        List<String> expected = Arrays.asList(MetadataTestContext.FALCON_USER,
                MetadataTestContext.CLUSTER_ENTITY_NAME, "Critical",
                MetadataTestContext.PROCESS_ENTITY_NAME,
                "imp-click-join1/2014-01-01T00:00Z", "imp-click-join2/2014-01-01T00:00Z");

        verifyVertexEdges(vertexId, LineageMetadataResource.OUT, expectedSize, expected);
        verifyVertexEdgesCount(vertexId, LineageMetadataResource.OUT_COUNT, expectedSize);
        verifyVertexEdgesCount(vertexId, LineageMetadataResource.OUT_E, expectedSize);
        verifyVertexEdgesCount(vertexId, LineageMetadataResource.OUT_IDS, expectedSize);
    }

    @Test
    public void testVertexEdgesForIdAndDirectionIn() throws Exception {
        String processInstance = MetadataTestContext.PROCESS_ENTITY_NAME + "/2014-01-01T01:00Z";
        LineageMetadataResource resource = new LineageMetadataResource();
        Vertex vertex = resource.getGraph().getVertices(
                RelationshipProperty.NAME.getName(), processInstance).iterator().next();
        String vertexId = String.valueOf(vertex.getId());

        int expectedSize = 2; // 2 input feeds
        List<String> expected = Arrays.asList("impression-feed/2014-01-01T00:00Z", "clicks-feed/2014-01-01T00:00Z");

        verifyVertexEdges(vertexId, LineageMetadataResource.IN, expectedSize, expected);
        verifyVertexEdgesCount(vertexId, LineageMetadataResource.IN_COUNT, expectedSize);
        verifyVertexEdgesCount(vertexId, LineageMetadataResource.IN_E, expectedSize);
        verifyVertexEdgesCount(vertexId, LineageMetadataResource.IN_IDS, expectedSize);
    }

    @Test
    public void testVertexEdgesForIdAndDirectionBoth() throws Exception {
        String processInstance = MetadataTestContext.PROCESS_ENTITY_NAME + "/2014-01-01T01:00Z";
        LineageMetadataResource resource = new LineageMetadataResource();
        Vertex vertex = resource.getGraph().getVertices(
                RelationshipProperty.NAME.getName(), processInstance).iterator().next();
        String vertexId = String.valueOf(vertex.getId());

        int expectedSize = 9;
        List<String> expected = Arrays.asList(MetadataTestContext.FALCON_USER,
                MetadataTestContext.CLUSTER_ENTITY_NAME, "Critical",
                MetadataTestContext.PROCESS_ENTITY_NAME,
                "imp-click-join1/2014-01-01T00:00Z", "imp-click-join2/2014-01-01T00:00Z",
                "impression-feed/2014-01-01T00:00Z", "clicks-feed/2014-01-01T00:00Z");

        verifyVertexEdges(vertexId, LineageMetadataResource.BOTH, expectedSize, expected);
        verifyVertexEdgesCount(vertexId, LineageMetadataResource.BOTH_COUNT, expectedSize);
        verifyVertexEdgesCount(vertexId, LineageMetadataResource.BOTH_E, expectedSize);
        verifyVertexEdgesCount(vertexId, LineageMetadataResource.BOTH_IDS, expectedSize);
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testVertexEdgesForIdAndInvalidDirection() throws Exception {
        LineageMetadataResource resource = new LineageMetadataResource();
        resource.getVertexEdges("0", "blah");
        Assert.fail("The API call should have thrown an exception");
    }

    private void verifyVertexEdges(String vertexId, String direction,
                                   int expectedSize, List<String> expected) {
        LineageMetadataResource resource = new LineageMetadataResource();
        Response response = resource.getVertexEdges(vertexId, direction);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        long totalSize = Long.valueOf(results.get(LineageMetadataResource.TOTAL_SIZE).toString());
        Assert.assertEquals(totalSize, expectedSize);

        ArrayList propertyList = (ArrayList) results.get(LineageMetadataResource.RESULTS);
        List<String> actual = new ArrayList<String>();
        for (Object property : propertyList) {
            actual.add((String) ((Map) property).get(RelationshipProperty.NAME.getName()));
        }

        Assert.assertTrue(actual.containsAll(expected));
    }

    private void verifyVertexEdgesCount(String vertexId, String direction, int expectedSize) {
        LineageMetadataResource resource = new LineageMetadataResource();
        Response response = resource.getVertexEdges(vertexId, direction);
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        long totalSize = Long.valueOf(results.get(LineageMetadataResource.TOTAL_SIZE).toString());
        Assert.assertEquals(totalSize, expectedSize);
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testEdgesByInvalidId() throws Exception {
        LineageMetadataResource resource = new LineageMetadataResource();
        Response response = resource.getEdge("blah");
        Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
        Assert.assertEquals(response.getEntity().toString(), "Edge with [blah] cannot be found.");
    }

    @Test
    public void testEdgesById() throws Exception {
        String processInstance = MetadataTestContext.PROCESS_ENTITY_NAME + "/2014-01-01T01:00Z";
        LineageMetadataResource resource = new LineageMetadataResource();
        Vertex vertex = resource.getGraph().getVertices(
                RelationshipProperty.NAME.getName(), processInstance).iterator().next();

        Edge edge = vertex.getEdges(Direction.OUT,
                RelationshipLabel.PROCESS_CLUSTER_EDGE.getName()).iterator().next();
        Vertex toVertex = edge.getVertex(Direction.IN);

        Response response = resource.getEdge(String.valueOf(edge.getId()));
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Map vertexProperties = (Map) results.get(LineageMetadataResource.RESULTS);

        Assert.assertEquals(vertexProperties.get("_id").toString(), edge.getId().toString());
        Assert.assertEquals(vertexProperties.get("_outV"), vertex.getId());
        Assert.assertEquals(vertexProperties.get("_inV"), toVertex.getId());
    }

    @Test
    public void testGetAllVertices() throws Exception {
        LineageMetadataResource resource = new LineageMetadataResource();
        Response response = resource.getVertices();
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        long totalSize = Long.valueOf(results.get(LineageMetadataResource.TOTAL_SIZE).toString());
        Assert.assertEquals(totalSize, getVerticesCount(resource.getGraph()));
    }

    @Test
    public void testGetAllEdges() throws Exception {
        LineageMetadataResource resource = new LineageMetadataResource();
        Response response = resource.getEdges();
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        long totalSize = Long.valueOf(results.get(LineageMetadataResource.TOTAL_SIZE).toString());
        Assert.assertEquals(totalSize, getEdgesCount(resource.getGraph()));
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testSerializeGraphBadFile() throws Exception {
        String path = StartupProperties.get().getProperty("falcon.graph.serialize.path");
        StartupProperties.get().setProperty("falcon.graph.serialize.path", "blah");
        try {
            LineageMetadataResource resource = new LineageMetadataResource();
            Response response = resource.serializeGraph();
            Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
        } finally {
            StartupProperties.get().setProperty("falcon.graph.serialize.path", path);
        }
    }

    @Test
    public void testSerializeGraph() throws Exception {
        LineageMetadataResource resource = new LineageMetadataResource();
        Response response = resource.serializeGraph();
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

        // verify file exists
        String path = StartupProperties.get().getProperty("falcon.graph.serialize.path");
        File[] jsonFiles = new File(path).listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isFile() && file.getName().endsWith(".json");
            }
        });
        Assert.assertTrue(jsonFiles.length > 0);
    }

    @Test (expectedExceptions = WebApplicationException.class)
    public void testLineageServiceIsDisabled() throws Exception {
        Services.get().reset();
        try {
            LineageMetadataResource resource = new LineageMetadataResource();
            resource.getVertices();
        } finally {
            Services.get().register(new WorkflowJobEndNotificationService());
            Services.get().register(testContext.getService());
        }
    }

    private void assertBasicVertexProperties(Vertex vertex, Map vertexProperties) {
        RelationshipProperty[] properties = {
            RelationshipProperty.NAME,
            RelationshipProperty.TYPE,
            RelationshipProperty.TIMESTAMP,
            RelationshipProperty.VERSION,
        };

        assertVertexProperties(vertex, vertexProperties, properties);
    }

    private void assertVertexProperties(Vertex vertex, Map vertexProperties,
                                        RelationshipProperty[] properties) {
        for (RelationshipProperty property : properties) {
            Assert.assertEquals(vertexProperties.get(property.getName()),
                    vertex.getProperty(property.getName()));
        }
    }

    private void assertWorkflowProperties(Vertex vertex, Map vertexProperties) {
        RelationshipProperty[] properties = {
            RelationshipProperty.USER_WORKFLOW_NAME,
            RelationshipProperty.USER_WORKFLOW_VERSION,
            RelationshipProperty.USER_WORKFLOW_ENGINE,
            RelationshipProperty.USER_SUBFLOW_ID,
            RelationshipProperty.WF_ENGINE_URL,
            RelationshipProperty.WORKFLOW_ID,
            RelationshipProperty.STATUS,
            RelationshipProperty.RUN_ID,
        };

        assertVertexProperties(vertex, vertexProperties, properties);
    }

    private void assertProcessInstanceRelationships(Map vertexProperties) {
        Assert.assertEquals(vertexProperties.get(RelationshipLabel.USER.getName()), MetadataTestContext.FALCON_USER);
        Assert.assertEquals(vertexProperties.get(RelationshipLabel.PROCESS_CLUSTER_EDGE.getName()),
                MetadataTestContext.CLUSTER_ENTITY_NAME);
        Assert.assertEquals(vertexProperties.get("classified-as"), "Critical");
    }

    private void assertFeedInstanceRelationships(Map vertexProperties, boolean skipRelationships) {
        Assert.assertEquals(vertexProperties.get(RelationshipLabel.USER.getName()), MetadataTestContext.FALCON_USER);
        Assert.assertEquals(vertexProperties.get(RelationshipLabel.FEED_CLUSTER_EDGE.getName()),
                MetadataTestContext.CLUSTER_ENTITY_NAME);

        if (!skipRelationships) {
            Assert.assertEquals(vertexProperties.get(RelationshipLabel.GROUPS.getName()), "analytics");
            Assert.assertEquals(vertexProperties.get("classified-as"), "Secure");
        }
    }

    private long getVerticesCount(final Graph graph) {
        long count = 0;
        for (Vertex ignored : graph.getVertices()) {
            count++;
        }

        return count;
    }

    private long getEdgesCount(final Graph graph) {
        long count = 0;
        for (Edge ignored : graph.getEdges()) {
            count++;
        }

        return count;
    }
}
