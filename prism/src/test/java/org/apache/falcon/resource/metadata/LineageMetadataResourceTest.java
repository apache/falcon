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
import org.apache.falcon.cluster.util.EntityBuilderTestUtil;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.metadata.MetadataMappingService;
import org.apache.falcon.metadata.RelationshipLabel;
import org.apache.falcon.metadata.RelationshipProperty;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.service.Services;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for org.apache.falcon.resource.metadata.LineageMetadataResource.
 */
public class LineageMetadataResourceTest {
    public static final String FALCON_USER = "falcon-user";
    private static final String LOGS_DIR = "target/log";
    private static final String NOMINAL_TIME = "2014-01-01-01-00";
    public static final String OPERATION = "GENERATE";

    public static final String CLUSTER_ENTITY_NAME = "primary-cluster";
    public static final String PROCESS_ENTITY_NAME = "sample-process";
    public static final String COLO_NAME = "west-coast";
    public static final String WORKFLOW_NAME = "imp-click-join-workflow";
    public static final String WORKFLOW_VERSION = "1.0.9";

    public static final String INPUT_FEED_NAMES = "impression-feed#clicks-feed";
    public static final String INPUT_INSTANCE_PATHS =
            "jail://global:00/falcon/impression-feed/20140101#jail://global:00/falcon/clicks-feed/20140101";

    public static final String OUTPUT_FEED_NAMES = "imp-click-join1,imp-click-join2";
    public static final String OUTPUT_INSTANCE_PATHS =
            "jail://global:00/falcon/imp-click-join1/20140101,jail://global:00/falcon/imp-click-join2/20140101";

    private ConfigurationStore configStore;
    private MetadataMappingService service;

    private Cluster clusterEntity;
    private List<Feed> inputFeeds = new ArrayList<Feed>();
    private List<Feed> outputFeeds = new ArrayList<Feed>();


    @BeforeClass
    public void setUp() throws Exception {
        CurrentUser.authenticate(FALCON_USER);

        configStore = ConfigurationStore.get();

        Services.get().register(new WorkflowJobEndNotificationService());
        Assert.assertTrue(Services.get().isRegistered(WorkflowJobEndNotificationService.SERVICE_NAME));

        StartupProperties.get().setProperty("falcon.graph.preserve.history", "true");
        service = new MetadataMappingService();
        service.init();
        Services.get().register(service);
        Assert.assertTrue(Services.get().isRegistered(MetadataMappingService.SERVICE_NAME));

        addClusterEntity();
        addFeedEntity();
        addProcessEntity();
        addInstance();
    }

    @AfterClass
    public void tearDown() throws Exception {
        cleanupGraphStore(service.getGraph());
        cleanupConfigurationStore(configStore);

        service.destroy();

        StartupProperties.get().setProperty("falcon.graph.preserve.history", "false");
        Services.get().reset();
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
        Vertex vertex = service.getGraph().getVertices(RelationshipProperty.NAME.getName(),
                PROCESS_ENTITY_NAME + "/2014-01-01T01:00Z").iterator().next();

        LineageMetadataResource resource = new LineageMetadataResource();
        Response response = resource.getVertex(String.valueOf(vertex.getId()));
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Map vertexProperties = (Map) results.get(LineageMetadataResource.RESULTS);
        Assert.assertEquals(vertexProperties.get("_id"), vertex.getId());
        assertBasicVertexProperties(vertex, vertexProperties);
    }

    @Test
    public void testGetVertexPropertiesForProcessInstance() throws Exception {
        String processInstance = PROCESS_ENTITY_NAME + "/2014-01-01T01:00Z";
        Vertex vertex = service.getGraph().getVertices(
                RelationshipProperty.NAME.getName(), processInstance).iterator().next();

        LineageMetadataResource resource = new LineageMetadataResource();
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
        Vertex vertex = service.getGraph().getVertices(
                RelationshipProperty.NAME.getName(), feedInstance).iterator().next();

        LineageMetadataResource resource = new LineageMetadataResource();
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
        Vertex vertex = service.getGraph().getVertices(
                RelationshipProperty.NAME.getName(), feedInstance).iterator().next();

        LineageMetadataResource resource = new LineageMetadataResource();
        Response response = resource.getVertexProperties(String.valueOf(vertex.getId()), "true");
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        Map vertexProperties = (Map) results.get(LineageMetadataResource.RESULTS);
        assertBasicVertexProperties(vertex, vertexProperties);

        assertFeedInstanceRelationships(vertexProperties, true);
    }

    @Test
    public void testGetVerticesWithKeyValue() throws Exception {
        String processInstance = PROCESS_ENTITY_NAME + "/2014-01-01T01:00Z";
        Vertex vertex = service.getGraph().getVertices(
                RelationshipProperty.NAME.getName(), processInstance).iterator().next();

        LineageMetadataResource resource = new LineageMetadataResource();
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
        String processInstance = PROCESS_ENTITY_NAME + "/2014-01-01T01:00Z";
        Vertex vertex = service.getGraph().getVertices(
                RelationshipProperty.NAME.getName(), processInstance).iterator().next();
        String vertexId = String.valueOf(vertex.getId());

        int expectedSize = 6; // 2 output feeds, user, cluster, process entity, tag
        List<String> expected = Arrays.asList(FALCON_USER, CLUSTER_ENTITY_NAME, "Critical",
                PROCESS_ENTITY_NAME, "imp-click-join1/2014-01-01T00:00Z", "imp-click-join2/2014-01-01T00:00Z");

        verifyVertexEdges(vertexId, LineageMetadataResource.OUT, expectedSize, expected);
        verifyVertexEdgesCount(vertexId, LineageMetadataResource.OUT_COUNT, expectedSize);
        verifyVertexEdgesCount(vertexId, LineageMetadataResource.OUT_E, expectedSize);
        verifyVertexEdgesCount(vertexId, LineageMetadataResource.OUT_IDS, expectedSize);
    }

    @Test
    public void testVertexEdgesForIdAndDirectionIn() throws Exception {
        String processInstance = PROCESS_ENTITY_NAME + "/2014-01-01T01:00Z";
        Vertex vertex = service.getGraph().getVertices(
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
        String processInstance = PROCESS_ENTITY_NAME + "/2014-01-01T01:00Z";
        Vertex vertex = service.getGraph().getVertices(
                RelationshipProperty.NAME.getName(), processInstance).iterator().next();
        String vertexId = String.valueOf(vertex.getId());

        int expectedSize = 8;
        List<String> expected = Arrays.asList(FALCON_USER, CLUSTER_ENTITY_NAME, "Critical",
                PROCESS_ENTITY_NAME, "imp-click-join1/2014-01-01T00:00Z", "imp-click-join2/2014-01-01T00:00Z",
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
        String processInstance = PROCESS_ENTITY_NAME + "/2014-01-01T01:00Z";
        Vertex vertex = service.getGraph().getVertices(
                RelationshipProperty.NAME.getName(), processInstance).iterator().next();

        Edge edge = vertex.getEdges(Direction.OUT,
                RelationshipLabel.PROCESS_CLUSTER_EDGE.getName()).iterator().next();
        Vertex toVertex = edge.getVertex(Direction.IN);

        LineageMetadataResource resource = new LineageMetadataResource();
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
        Assert.assertEquals(totalSize, getVerticesCount(service.getGraph()));
    }

    @Test
    public void testGetAllEdges() throws Exception {
        LineageMetadataResource resource = new LineageMetadataResource();
        Response response = resource.getEdges();
        Assert.assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());

        Map results = (Map) JSONValue.parse(response.getEntity().toString());
        long totalSize = Long.valueOf(results.get(LineageMetadataResource.TOTAL_SIZE).toString());
        Assert.assertEquals(totalSize, getEdgesCount(service.getGraph()));
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
            Response response = resource.getVertices();
            Assert.assertEquals(response.getStatus(), Response.Status.NOT_FOUND.getStatusCode());
            Assert.assertEquals(response.getEntity().toString(), "Lineage Metadata Service is not enabled.");
        } finally {
            Services.get().register(new WorkflowJobEndNotificationService());
            Services.get().register(service);
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
        Assert.assertEquals(vertexProperties.get(RelationshipLabel.USER.getName()), FALCON_USER);
        Assert.assertEquals(vertexProperties.get(RelationshipLabel.PROCESS_CLUSTER_EDGE.getName()),
                CLUSTER_ENTITY_NAME);
        Assert.assertEquals(vertexProperties.get("classified-as"), "Critical");
    }

    private void assertFeedInstanceRelationships(Map vertexProperties, boolean skipRelationships) {
        Assert.assertEquals(vertexProperties.get(RelationshipLabel.USER.getName()), FALCON_USER);
        Assert.assertEquals(vertexProperties.get(RelationshipLabel.FEED_CLUSTER_EDGE.getName()),
                CLUSTER_ENTITY_NAME);

        if (!skipRelationships) {
            Assert.assertEquals(vertexProperties.get(RelationshipLabel.GROUPS.getName()), "analytics");
            Assert.assertEquals(vertexProperties.get("classified-as"), "Secure");
        }
    }

    public void addClusterEntity() throws Exception {
        clusterEntity = EntityBuilderTestUtil.buildCluster(CLUSTER_ENTITY_NAME,
                COLO_NAME, "classification=production");
        configStore.publish(EntityType.CLUSTER, clusterEntity);
    }

    public void addFeedEntity() throws Exception {
        Feed impressionsFeed = EntityBuilderTestUtil.buildFeed("impression-feed", clusterEntity,
                "classified-as=Secure", "analytics");
        addStorage(impressionsFeed, Storage.TYPE.FILESYSTEM, "/falcon/impression-feed/${YEAR}${MONTH}${DAY}");
        configStore.publish(EntityType.FEED, impressionsFeed);
        inputFeeds.add(impressionsFeed);

        Feed clicksFeed = EntityBuilderTestUtil.buildFeed("clicks-feed", clusterEntity, null, null);
        addStorage(clicksFeed, Storage.TYPE.FILESYSTEM, "/falcon/clicks-feed/${YEAR}${MONTH}${DAY}");
        configStore.publish(EntityType.FEED, clicksFeed);
        inputFeeds.add(clicksFeed);

        Feed join1Feed = EntityBuilderTestUtil.buildFeed("imp-click-join1", clusterEntity,
                "classified-as=Financial", "reporting,bi");
        addStorage(join1Feed, Storage.TYPE.FILESYSTEM, "/falcon/imp-click-join1/${YEAR}${MONTH}${DAY}");
        configStore.publish(EntityType.FEED, join1Feed);
        outputFeeds.add(join1Feed);

        Feed join2Feed = EntityBuilderTestUtil.buildFeed("imp-click-join2", clusterEntity,
                "classified-as=Secure,classified-as=Financial", "reporting,bi");
        addStorage(join2Feed, Storage.TYPE.FILESYSTEM, "/falcon/imp-click-join2/${YEAR}${MONTH}${DAY}");
        configStore.publish(EntityType.FEED, join2Feed);
        outputFeeds.add(join2Feed);
    }

    public static void addStorage(Feed feed, Storage.TYPE storageType, String uriTemplate) {
        if (storageType == Storage.TYPE.FILESYSTEM) {
            Locations locations = new Locations();
            feed.setLocations(locations);

            Location location = new Location();
            location.setType(LocationType.DATA);
            location.setPath(uriTemplate);
            feed.getLocations().getLocations().add(location);
        } else {
            CatalogTable table = new CatalogTable();
            table.setUri(uriTemplate);
            feed.setTable(table);
        }
    }

    public void addProcessEntity() throws Exception {
        Process processEntity = EntityBuilderTestUtil.buildProcess(PROCESS_ENTITY_NAME,
                clusterEntity, "classified-as=Critical");
        EntityBuilderTestUtil.addProcessWorkflow(processEntity, WORKFLOW_NAME, WORKFLOW_VERSION);

        for (Feed inputFeed : inputFeeds) {
            EntityBuilderTestUtil.addInput(processEntity, inputFeed);
        }

        for (Feed outputFeed : outputFeeds) {
            EntityBuilderTestUtil.addOutput(processEntity, outputFeed);
        }

        configStore.publish(EntityType.PROCESS, processEntity);
    }

    public void addInstance() throws Exception {
        WorkflowExecutionContext context = WorkflowExecutionContext.create(getTestMessageArgs(),
                WorkflowExecutionContext.Type.POST_PROCESSING);
        service.onSuccess(context);
    }

    private static String[] getTestMessageArgs() {
        return new String[]{
            "-" + WorkflowExecutionArgs.CLUSTER_NAME.getName(), CLUSTER_ENTITY_NAME,
            "-" + WorkflowExecutionArgs.ENTITY_TYPE.getName(), ("process"),
            "-" + WorkflowExecutionArgs.ENTITY_NAME.getName(), PROCESS_ENTITY_NAME,
            "-" + WorkflowExecutionArgs.NOMINAL_TIME.getName(), NOMINAL_TIME,
            "-" + WorkflowExecutionArgs.OPERATION.getName(), OPERATION,

            "-" + WorkflowExecutionArgs.INPUT_FEED_NAMES.getName(), INPUT_FEED_NAMES,
            "-" + WorkflowExecutionArgs.INPUT_FEED_PATHS.getName(), INPUT_INSTANCE_PATHS,

            "-" + WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(), OUTPUT_FEED_NAMES,
            "-" + WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(), OUTPUT_INSTANCE_PATHS,

            "-" + WorkflowExecutionArgs.WORKFLOW_ID.getName(), "workflow-01-00",
            "-" + WorkflowExecutionArgs.WORKFLOW_USER.getName(), FALCON_USER,
            "-" + WorkflowExecutionArgs.RUN_ID.getName(), "1",
            "-" + WorkflowExecutionArgs.STATUS.getName(), "SUCCEEDED",
            "-" + WorkflowExecutionArgs.TIMESTAMP.getName(), NOMINAL_TIME,

            "-" + WorkflowExecutionArgs.WF_ENGINE_URL.getName(), "http://localhost:11000/oozie",
            "-" + WorkflowExecutionArgs.USER_SUBFLOW_ID.getName(), "userflow@wf-id",
            "-" + WorkflowExecutionArgs.USER_WORKFLOW_NAME.getName(), WORKFLOW_NAME,
            "-" + WorkflowExecutionArgs.USER_WORKFLOW_VERSION.getName(), WORKFLOW_VERSION,
            "-" + WorkflowExecutionArgs.USER_WORKFLOW_ENGINE.getName(), EngineType.PIG.name(),


            "-" + WorkflowExecutionArgs.BRKR_IMPL_CLASS.getName(), "blah",
            "-" + WorkflowExecutionArgs.BRKR_URL.getName(), "tcp://localhost:61616?daemon=true",
            "-" + WorkflowExecutionArgs.USER_BRKR_IMPL_CLASS.getName(), "blah",
            "-" + WorkflowExecutionArgs.USER_BRKR_URL.getName(), "tcp://localhost:61616?daemon=true",
            "-" + WorkflowExecutionArgs.BRKR_TTL.getName(), "1000",

            "-" + WorkflowExecutionArgs.LOG_DIR.getName(), LOGS_DIR,
            "-" + WorkflowExecutionArgs.LOG_FILE.getName(), LOGS_DIR + "/log.txt",
        };
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

    private void cleanupGraphStore(Graph graph) {
        for (Edge edge : graph.getEdges()) {
            graph.removeEdge(edge);
        }

        for (Vertex vertex : graph.getVertices()) {
            graph.removeVertex(vertex);
        }

        graph.shutdown();
    }

    private static void cleanupConfigurationStore(ConfigurationStore store) throws Exception {
        for (EntityType type : EntityType.values()) {
            Collection<String> entities = store.getEntities(type);
            for (String entity : entities) {
                store.remove(type, entity);
            }
        }
    }
}
