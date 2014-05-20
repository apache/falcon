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

package org.apache.falcon.metadata;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.Frequency;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.cluster.Interface;
import org.apache.falcon.entity.v0.cluster.Interfaces;
import org.apache.falcon.entity.v0.cluster.Interfacetype;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.Location;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.feed.Locations;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.entity.v0.process.Input;
import org.apache.falcon.entity.v0.process.Inputs;
import org.apache.falcon.entity.v0.process.Output;
import org.apache.falcon.entity.v0.process.Outputs;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.entity.v0.process.Workflow;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.util.StartupProperties;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Test for Metadata relationship mapping service.
 */
public class MetadataMappingServiceTest {

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
        "jail://global:00/falcon/impression-feed/2014/01/01,jail://global:00/falcon/impression-feed/2014/01/02"
                + "#jail://global:00/falcon/clicks-feed/2014-01-01";

    public static final String OUTPUT_FEED_NAMES = "imp-click-join1,imp-click-join2";
    public static final String OUTPUT_INSTANCE_PATHS =
        "jail://global:00/falcon/imp-click-join1/20140101,jail://global:00/falcon/imp-click-join2/20140101";

    private ConfigurationStore configStore;
    private MetadataMappingService service;

    private Cluster clusterEntity;
    private Cluster bcpCluster;
    private List<Feed> inputFeeds = new ArrayList<Feed>();
    private List<Feed> outputFeeds = new ArrayList<Feed>();
    private Process processEntity;


    @BeforeClass
    public void setUp() throws Exception {
        CurrentUser.authenticate(FALCON_USER);

        configStore = ConfigurationStore.get();

        StartupProperties.get().setProperty("falcon.graph.preserve.history", "true");
        service = new MetadataMappingService();
        service.init();

        Set<String> vertexPropertyKeys = service.getVertexIndexedKeys();
        System.out.println("Got vertex property keys: " + vertexPropertyKeys);

        Set<String> edgePropertyKeys = service.getEdgeIndexedKeys();
        System.out.println("Got edge property keys: " + edgePropertyKeys);
    }

    @AfterClass
    public void tearDown() throws Exception {
        GraphUtils.dump(service.getGraph(), System.out);

        cleanupGraphStore(service.getGraph());
        cleanupConfigurationStore(configStore);
        service.destroy();
        StartupProperties.get().setProperty("falcon.graph.preserve.history", "false");
    }

    @AfterMethod
    public void printGraph() throws Exception {
        GraphUtils.dump(service.getGraph());
    }

    private GraphQuery getQuery() {
        return service.getGraph().query();
    }

    @Test
    public void testGetName() throws Exception {
        Assert.assertEquals(service.getName(), MetadataMappingService.SERVICE_NAME);
    }

    @Test
    public void testOnAddClusterEntity() throws Exception {
        clusterEntity = buildCluster(CLUSTER_ENTITY_NAME, COLO_NAME, "classification=production");
        configStore.publish(EntityType.CLUSTER, clusterEntity);

        verifyEntityWasAddedToGraph(CLUSTER_ENTITY_NAME, RelationshipType.CLUSTER_ENTITY);
        verifyClusterEntityEdges();

        Assert.assertEquals(getVerticesCount(service.getGraph()), 3); // +3 = cluster, colo, tag
        Assert.assertEquals(getEdgesCount(service.getGraph()), 2); // +2 = cluster to colo and tag
    }

    @Test (dependsOnMethods = "testOnAddClusterEntity")
    public void testOnAddFeedEntity() throws Exception {
        Feed impressionsFeed = buildFeed("impression-feed", clusterEntity, "classified-as=Secure", "analytics",
                Storage.TYPE.FILESYSTEM, "/falcon/impression-feed/${YEAR}/${MONTH}/${DAY}");
        configStore.publish(EntityType.FEED, impressionsFeed);
        inputFeeds.add(impressionsFeed);
        verifyEntityWasAddedToGraph(impressionsFeed.getName(), RelationshipType.FEED_ENTITY);
        verifyFeedEntityEdges(impressionsFeed.getName());
        Assert.assertEquals(getVerticesCount(service.getGraph()), 7); // +4 = feed, tag, group, user
        Assert.assertEquals(getEdgesCount(service.getGraph()), 6); // +4 = cluster, tag, group, user

        Feed clicksFeed = buildFeed("clicks-feed", clusterEntity, "classified-as=Secure,classified-as=Financial",
                "analytics", Storage.TYPE.FILESYSTEM, "/falcon/clicks-feed/${YEAR}-${MONTH}-${DAY}");
        configStore.publish(EntityType.FEED, clicksFeed);
        inputFeeds.add(clicksFeed);
        verifyEntityWasAddedToGraph(clicksFeed.getName(), RelationshipType.FEED_ENTITY);
        Assert.assertEquals(getVerticesCount(service.getGraph()), 9); // feed and financial vertex
        Assert.assertEquals(getEdgesCount(service.getGraph()), 11); // +5 = cluster + user + 2Group + Tag

        Feed join1Feed = buildFeed("imp-click-join1", clusterEntity, "classified-as=Financial", "reporting,bi",
                Storage.TYPE.FILESYSTEM, "/falcon/imp-click-join1/${YEAR}${MONTH}${DAY}");
        configStore.publish(EntityType.FEED, join1Feed);
        outputFeeds.add(join1Feed);
        verifyEntityWasAddedToGraph(join1Feed.getName(), RelationshipType.FEED_ENTITY);
        Assert.assertEquals(getVerticesCount(service.getGraph()), 12); // + 3 = 1 feed and 2 groups
        Assert.assertEquals(getEdgesCount(service.getGraph()), 16); // +5 = cluster + user +
        // Group + 2Tags

        Feed join2Feed = buildFeed("imp-click-join2", clusterEntity, "classified-as=Secure,classified-as=Financial",
                "reporting,bi", Storage.TYPE.FILESYSTEM, "/falcon/imp-click-join2/${YEAR}${MONTH}${DAY}");
        configStore.publish(EntityType.FEED, join2Feed);
        outputFeeds.add(join2Feed);
        verifyEntityWasAddedToGraph(join2Feed.getName(), RelationshipType.FEED_ENTITY);

        Assert.assertEquals(getVerticesCount(service.getGraph()), 13); // +1 feed
        // +6 = user + 2tags + 2Groups + Cluster
        Assert.assertEquals(getEdgesCount(service.getGraph()), 22);
    }

    @Test (dependsOnMethods = "testOnAddFeedEntity")
    public void testOnAddProcessEntity() throws Exception {
        processEntity = buildProcess(PROCESS_ENTITY_NAME, clusterEntity, "classified-as=Critical");
        addWorkflow(processEntity, WORKFLOW_NAME, WORKFLOW_VERSION);

        for (Feed inputFeed : inputFeeds) {
            addInput(processEntity, inputFeed);
        }

        for (Feed outputFeed : outputFeeds) {
            addOutput(processEntity, outputFeed);
        }

        configStore.publish(EntityType.PROCESS, processEntity);

        verifyEntityWasAddedToGraph(processEntity.getName(), RelationshipType.PROCESS_ENTITY);
        verifyProcessEntityEdges();

        // +2 = 1 process + 1 tag
        Assert.assertEquals(getVerticesCount(service.getGraph()), 15);
        // +7 = user,tag,cluster, 2 inputs,2 outputs
        Assert.assertEquals(getEdgesCount(service.getGraph()), 29);
    }

    @Test (dependsOnMethods = "testOnAddProcessEntity")
    public void testOnAdd() throws Exception {
        verifyEntityGraph(RelationshipType.FEED_ENTITY, "Secure");
    }

    @Test(dependsOnMethods = "testOnAdd")
    public void testMapLineage() throws Exception {
        // shutdown the graph and resurrect for testing
        service.destroy();
        service.init();

        LineageRecorder.main(getTestMessageArgs());

        service.onSuccessfulWorkflowCompletion(PROCESS_ENTITY_NAME, OPERATION, LOGS_DIR);

        debug(service.getGraph());
        GraphUtils.dump(service.getGraph());
        verifyLineageGraph(RelationshipType.FEED_INSTANCE.getName());

        // +6 = 1 process, 2 inputs = 3 instances,2 outputs
        Assert.assertEquals(getVerticesCount(service.getGraph()), 21);
        //+32 = +26 for feed instances + 6 for process instance + 6 for second feed instance
        Assert.assertEquals(getEdgesCount(service.getGraph()), 67);
    }

    @Test (dependsOnMethods = "testMapLineage")
    public void testOnChange() throws Exception {
        // shutdown the graph and resurrect for testing
        service.destroy();
        service.init();

        // cannot modify cluster, adding a new cluster
        bcpCluster = buildCluster("bcp-cluster", "east-coast", "classification=bcp");
        configStore.publish(EntityType.CLUSTER, bcpCluster);
        verifyEntityWasAddedToGraph("bcp-cluster", RelationshipType.CLUSTER_ENTITY);

        Assert.assertEquals(getVerticesCount(service.getGraph()), 24); // +3 = cluster, colo, tag
        // +2 edges to above, no user but only to colo and new tag
        Assert.assertEquals(getEdgesCount(service.getGraph()), 69);
    }

    @Test(dependsOnMethods = "testOnChange")
    public void testOnFeedEntityChange() throws Exception {
        Feed oldFeed = inputFeeds.get(0);
        Feed newFeed = buildFeed(oldFeed.getName(), clusterEntity,
                "classified-as=Secured,source=data-warehouse", "reporting",
                Storage.TYPE.FILESYSTEM, "jail://global:00/falcon/impression-feed/20140101");

        try {
            configStore.initiateUpdate(newFeed);

            // add cluster
            org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                    new org.apache.falcon.entity.v0.feed.Cluster();
            feedCluster.setName(bcpCluster.getName());
            newFeed.getClusters().getClusters().add(feedCluster);

            configStore.update(EntityType.FEED, newFeed);
        } finally {
            configStore.cleanupUpdateInit();
        }

        verifyUpdatedEdges(newFeed);
        Assert.assertEquals(getVerticesCount(service.getGraph()), 26); //+2 = 2 new tags
        Assert.assertEquals(getEdgesCount(service.getGraph()), 71); // +2 = 1 new cluster, 1 new tag
    }

    private void verifyUpdatedEdges(Feed newFeed) {
        Vertex feedVertex = getEntityVertex(newFeed.getName(), RelationshipType.FEED_ENTITY);

        // groups
        Edge edge = feedVertex.getEdges(Direction.OUT, RelationshipLabel.GROUPS.getName()).iterator().next();
        Assert.assertEquals(edge.getVertex(Direction.IN).getProperty("name"), "reporting");

        // tags
        edge = feedVertex.getEdges(Direction.OUT, "classified-as").iterator().next();
        Assert.assertEquals(edge.getVertex(Direction.IN).getProperty("name"), "Secured");
        edge = feedVertex.getEdges(Direction.OUT, "source").iterator().next();
        Assert.assertEquals(edge.getVertex(Direction.IN).getProperty("name"), "data-warehouse");

        // new cluster
        List<String> actual = new ArrayList<String>();
        for (Edge clusterEdge : feedVertex.getEdges(Direction.OUT, RelationshipLabel.FEED_CLUSTER_EDGE.getName())) {
            actual.add(clusterEdge.getVertex(Direction.IN).<String>getProperty("name"));
        }
        Assert.assertTrue(actual.containsAll(Arrays.asList("primary-cluster", "bcp-cluster")),
                "Actual does not contain expected: " + actual);
    }

    @Test(dependsOnMethods = "testOnFeedEntityChange")
    public void testOnProcessEntityChange() throws Exception {
        Process oldProcess = processEntity;
        Process newProcess = buildProcess(oldProcess.getName(), bcpCluster, null);
        addWorkflow(newProcess, WORKFLOW_NAME, "2.0.0");
        addInput(newProcess, inputFeeds.get(0));

        try {
            configStore.initiateUpdate(newProcess);
            configStore.update(EntityType.PROCESS, newProcess);
        } finally {
            configStore.cleanupUpdateInit();
        }

        verifyUpdatedEdges(newProcess);
        Assert.assertEquals(getVerticesCount(service.getGraph()), 26); // +0, no net new
        Assert.assertEquals(getEdgesCount(service.getGraph()), 67); // -4 = -2 outputs, -1 tag, -1 cluster
    }

    private void verifyUpdatedEdges(Process newProcess) {
        Vertex processVertex = getEntityVertex(newProcess.getName(), RelationshipType.PROCESS_ENTITY);

        // cluster
        Edge edge = processVertex.getEdges(Direction.OUT,
                RelationshipLabel.PROCESS_CLUSTER_EDGE.getName()).iterator().next();
        Assert.assertEquals(edge.getVertex(Direction.IN).getProperty("name"), bcpCluster.getName());

        // inputs
        edge = processVertex.getEdges(Direction.IN, RelationshipLabel.FEED_PROCESS_EDGE.getName()).iterator().next();
        Assert.assertEquals(edge.getVertex(Direction.OUT).getProperty("name"),
                newProcess.getInputs().getInputs().get(0).getFeed());

        // outputs
        for (Edge e : processVertex.getEdges(Direction.OUT, RelationshipLabel.PROCESS_FEED_EDGE.getName())) {
            Assert.fail("there should not be any edges to output feeds" + e);
        }
    }

    public static void debug(final Graph graph) {
        System.out.println("*****Vertices of " + graph);
        for (Vertex vertex : graph.getVertices()) {
            System.out.println(GraphUtils.vertexString(vertex));
        }

        System.out.println("*****Edges of " + graph);
        for (Edge edge : graph.getEdges()) {
            System.out.println(GraphUtils.edgeString(edge));
        }
    }

    private static Cluster buildCluster(String name, String colo, String tags) {
        Cluster cluster = new Cluster();
        cluster.setName(name);
        cluster.setColo(colo);
        cluster.setTags(tags);

        Interfaces interfaces = new Interfaces();
        cluster.setInterfaces(interfaces);

        Interface storage = new Interface();
        storage.setEndpoint("jail://global:00");
        storage.setType(Interfacetype.WRITE);
        cluster.getInterfaces().getInterfaces().add(storage);

        return cluster;
    }

    private static Feed buildFeed(String feedName, Cluster cluster, String tags, String groups,
                                  Storage.TYPE storageType, String uriTemplate) {
        Feed feed = new Feed();
        feed.setName(feedName);
        feed.setTags(tags);
        feed.setGroups(groups);
        feed.setFrequency(Frequency.fromString("hours(1)"));

        org.apache.falcon.entity.v0.feed.Clusters
                clusters = new org.apache.falcon.entity.v0.feed.Clusters();
        feed.setClusters(clusters);
        org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                new org.apache.falcon.entity.v0.feed.Cluster();
        feedCluster.setName(cluster.getName());
        clusters.getClusters().add(feedCluster);

        addStorage(feed, storageType, uriTemplate);

        return feed;
    }

    private static void addStorage(Feed feed, Storage.TYPE storageType, String uriTemplate) {
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

    private static Process buildProcess(String processName, Cluster cluster,
                                        String tags) throws Exception {
        Process processEntity = new Process();
        processEntity.setName(processName);
        processEntity.setTags(tags);

        org.apache.falcon.entity.v0.process.Cluster processCluster =
                new org.apache.falcon.entity.v0.process.Cluster();
        processCluster.setName(cluster.getName());
        processEntity.setClusters(new org.apache.falcon.entity.v0.process.Clusters());
        processEntity.getClusters().getClusters().add(processCluster);

        return processEntity;
    }

    private static void addWorkflow(Process process, String workflowName, String version) {
        Workflow workflow = new Workflow();
        workflow.setName(workflowName);
        workflow.setVersion(version);
        workflow.setEngine(EngineType.PIG);
        workflow.setPath("/falcon/test/workflow");

        process.setWorkflow(workflow);
    }

    private static void addInput(Process process, Feed feed) {
        if (process.getInputs() == null) {
            process.setInputs(new Inputs());
        }

        Inputs inputs = process.getInputs();
        Input input = new Input();
        input.setFeed(feed.getName());
        inputs.getInputs().add(input);
    }

    private static void addOutput(Process process, Feed feed) {
        if (process.getOutputs() == null) {
            process.setOutputs(new Outputs());
        }

        Outputs outputs = process.getOutputs();
        Output output = new Output();
        output.setFeed(feed.getName());
        outputs.getOutputs().add(output);
    }

    private void verifyEntityWasAddedToGraph(String entityName, RelationshipType entityType) {
        Vertex entityVertex = getEntityVertex(entityName, entityType);
        Assert.assertNotNull(entityVertex);
        verifyEntityProperties(entityVertex, entityName, entityType);
    }

    private void verifyEntityProperties(Vertex entityVertex, String entityName, RelationshipType entityType) {
        Assert.assertEquals(entityName, entityVertex.getProperty(RelationshipProperty.NAME.getName()));
        Assert.assertEquals(entityType.getName(), entityVertex.getProperty(RelationshipProperty.TYPE.getName()));
        Assert.assertNotNull(entityVertex.getProperty(RelationshipProperty.TIMESTAMP.getName()));
    }

    private void verifyClusterEntityEdges() {
        Vertex clusterVertex = getEntityVertex(CLUSTER_ENTITY_NAME,
                RelationshipType.CLUSTER_ENTITY);

        // verify edge to user vertex
        verifyVertexForEdge(clusterVertex, Direction.OUT, RelationshipLabel.USER.getName(),
                FALCON_USER, RelationshipType.USER.getName());
        // verify edge to colo vertex
        verifyVertexForEdge(clusterVertex, Direction.OUT, RelationshipLabel.CLUSTER_COLO.getName(),
                COLO_NAME, RelationshipType.COLO.getName());
        // verify edge to tags vertex
        verifyVertexForEdge(clusterVertex, Direction.OUT, "classification",
                "production", RelationshipType.TAGS.getName());
    }

    private void verifyFeedEntityEdges(String feedName) {
        Vertex feedVertex = getEntityVertex(feedName, RelationshipType.FEED_ENTITY);

        // verify edge to cluster vertex
        verifyVertexForEdge(feedVertex, Direction.OUT, RelationshipLabel.FEED_CLUSTER_EDGE.getName(),
                CLUSTER_ENTITY_NAME, RelationshipType.CLUSTER_ENTITY.getName());
        // verify edge to user vertex
        verifyVertexForEdge(feedVertex, Direction.OUT, RelationshipLabel.USER.getName(),
                FALCON_USER, RelationshipType.USER.getName());
        // verify edge to tags vertex
        verifyVertexForEdge(feedVertex, Direction.OUT, "classified-as",
                "Secure", RelationshipType.TAGS.getName());
        // verify edge to group vertex
        verifyVertexForEdge(feedVertex, Direction.OUT, RelationshipLabel.GROUPS.getName(),
                "analytics", RelationshipType.GROUPS.getName());
    }

    private void verifyProcessEntityEdges() {
        Vertex processVertex = getEntityVertex(PROCESS_ENTITY_NAME,
                RelationshipType.PROCESS_ENTITY);

        // verify edge to cluster vertex
        verifyVertexForEdge(processVertex, Direction.OUT, RelationshipLabel.FEED_CLUSTER_EDGE.getName(),
                CLUSTER_ENTITY_NAME, RelationshipType.CLUSTER_ENTITY.getName());
        // verify edge to user vertex
        verifyVertexForEdge(processVertex, Direction.OUT, RelationshipLabel.USER.getName(),
                FALCON_USER, RelationshipType.USER.getName());
        // verify edge to tags vertex
        verifyVertexForEdge(processVertex, Direction.OUT, "classified-as",
                "Critical", RelationshipType.TAGS.getName());

        // verify edges to inputs
        List<String> actual = new ArrayList<String>();
        for (Edge edge : processVertex.getEdges(Direction.IN,
                RelationshipLabel.FEED_PROCESS_EDGE.getName())) {
            Vertex outVertex = edge.getVertex(Direction.OUT);
            Assert.assertEquals(RelationshipType.FEED_ENTITY.getName(),
                    outVertex.getProperty(RelationshipProperty.TYPE.getName()));
            actual.add(outVertex.<String>getProperty(RelationshipProperty.NAME.getName()));
        }

        Assert.assertTrue(actual.containsAll(Arrays.asList("impression-feed", "clicks-feed")),
                "Actual does not contain expected: " + actual);

        actual.clear();
        // verify edges to outputs
        for (Edge edge : processVertex.getEdges(Direction.OUT,
                RelationshipLabel.PROCESS_FEED_EDGE.getName())) {
            Vertex outVertex = edge.getVertex(Direction.IN);
            Assert.assertEquals(RelationshipType.FEED_ENTITY.getName(),
                    outVertex.getProperty(RelationshipProperty.TYPE.getName()));
            actual.add(outVertex.<String>getProperty(RelationshipProperty.NAME.getName()));
        }
        Assert.assertTrue(actual.containsAll(Arrays.asList("imp-click-join1", "imp-click-join2")),
                "Actual does not contain expected: " + actual);
    }

    private Vertex getEntityVertex(String entityName, RelationshipType entityType) {
        GraphQuery entityQuery = getQuery()
                .has(RelationshipProperty.NAME.getName(), entityName)
                .has(RelationshipProperty.TYPE.getName(), entityType.getName());
        Iterator<Vertex> iterator = entityQuery.vertices().iterator();
        Assert.assertTrue(iterator.hasNext());

        Vertex entityVertex = iterator.next();
        Assert.assertNotNull(entityVertex);

        return entityVertex;
    }

    private void verifyVertexForEdge(Vertex fromVertex, Direction direction, String label,
                                     String expectedName, String expectedType) {
        for (Edge edge : fromVertex.getEdges(direction, label)) {
            Vertex outVertex = edge.getVertex(Direction.IN);
            Assert.assertEquals(
                    outVertex.getProperty(RelationshipProperty.NAME.getName()), expectedName);
            Assert.assertEquals(
                    outVertex.getProperty(RelationshipProperty.TYPE.getName()), expectedType);
        }
    }

    private void verifyEntityGraph(RelationshipType feedType, String classification) {
        // feeds owned by a user
        List<String> feedNamesOwnedByUser = getFeedsOwnedByAUser(feedType.getName());
        Assert.assertEquals(feedNamesOwnedByUser,
                Arrays.asList("impression-feed", "clicks-feed", "imp-click-join1", "imp-click-join2"));

        // feeds classified as secure
        verifyFeedsClassifiedAsSecure(feedType.getName(),
                Arrays.asList("impression-feed", "clicks-feed", "imp-click-join2"));

        // feeds owned by a user and classified as secure
        verifyFeedsOwnedByUserAndClassification(feedType.getName(), classification,
                Arrays.asList("impression-feed", "clicks-feed", "imp-click-join2"));
    }

    private List<String> getFeedsOwnedByAUser(String feedType) {
        GraphQuery userQuery = getQuery()
                .has(RelationshipProperty.NAME.getName(), FALCON_USER)
                .has(RelationshipProperty.TYPE.getName(), RelationshipType.USER.getName());

        List<String> feedNames = new ArrayList<String>();
        for (Vertex userVertex : userQuery.vertices()) {
            for (Vertex feed : userVertex.getVertices(Direction.IN, RelationshipLabel.USER.getName())) {
                if (feed.getProperty(RelationshipProperty.TYPE.getName()).equals(feedType)) {
                    System.out.println(FALCON_USER + " owns -> " + GraphUtils.vertexString(feed));
                    feedNames.add(feed.<String>getProperty(RelationshipProperty.NAME.getName()));
                }
            }
        }

        return feedNames;
    }

    private void verifyFeedsClassifiedAsSecure(String feedType, List<String> expected) {
        GraphQuery classQuery = getQuery()
                .has(RelationshipProperty.NAME.getName(), "Secure")
                .has(RelationshipProperty.TYPE.getName(), RelationshipType.TAGS.getName());

        List<String> actual = new ArrayList<String>();
        for (Vertex feedVertex : classQuery.vertices()) {
            for (Vertex feed : feedVertex.getVertices(Direction.BOTH, "classified-as")) {
                if (feed.getProperty(RelationshipProperty.TYPE.getName()).equals(feedType)) {
                    System.out.println(" Secure classification -> " + GraphUtils.vertexString(feed));
                    actual.add(feed.<String>getProperty(RelationshipProperty.NAME.getName()));
                }
            }
        }

        Assert.assertTrue(actual.containsAll(expected), "Actual does not contain expected: " + actual);
    }

    private void verifyFeedsOwnedByUserAndClassification(String feedType, String classification,
                                                         List<String> expected) {
        List<String> actual = new ArrayList<String>();
        Vertex userVertex = getEntityVertex(FALCON_USER, RelationshipType.USER);
        for (Vertex feed : userVertex.getVertices(Direction.IN, RelationshipLabel.USER.getName())) {
            if (feed.getProperty(RelationshipProperty.TYPE.getName()).equals(feedType)) {
                for (Vertex classVertex : feed.getVertices(Direction.OUT, "classified-as")) {
                    if (classVertex.getProperty(RelationshipProperty.NAME.getName())
                            .equals(classification)) {
                        actual.add(feed.<String>getProperty(RelationshipProperty.NAME.getName()));
                        System.out.println(classification + " feed owned by falcon-user -> "
                                + GraphUtils.vertexString(feed));
                    }
                }
            }
        }
        Assert.assertTrue(actual.containsAll(expected), "Actual does not contain expected: " + actual);
    }

    public long getVerticesCount(final Graph graph) {
        long count = 0;
        for (Vertex ignored : graph.getVertices()) {
            count++;
        }

        return count;
    }

    public long getEdgesCount(final Graph graph) {
        long count = 0;
        for (Edge ignored : graph.getEdges()) {
            count++;
        }

        return count;
    }

    private void verifyLineageGraph(String feedType) {
        // feeds owned by a user
        List<String> feedNamesOwnedByUser = getFeedsOwnedByAUser(feedType);
        List<String> expected = Arrays.asList("impression-feed/2014-01-01T00:00Z", "clicks-feed/2014-01-01T00:00Z",
                "imp-click-join1/2014-01-01T00:00Z", "imp-click-join2/2014-01-01T00:00Z");
        Assert.assertTrue(feedNamesOwnedByUser.containsAll(expected));

        Graph graph = service.getGraph();

        Iterator<Vertex> vertices = graph.getVertices("name", "impression-feed/2014-01-01T00:00Z").iterator();
        Assert.assertTrue(vertices.hasNext());
        Vertex feedInstanceVertex = vertices.next();
        Assert.assertEquals(feedInstanceVertex.getProperty(RelationshipProperty.TYPE.getName()),
                RelationshipType.FEED_INSTANCE.getName());

        Object vertexId = feedInstanceVertex.getId();
        Vertex vertexById = graph.getVertex(vertexId);
        Assert.assertEquals(vertexById, feedInstanceVertex);

        // feeds classified as secure
        verifyFeedsClassifiedAsSecure(feedType,
                Arrays.asList("impression-feed/2014-01-01T00:00Z",
                        "clicks-feed/2014-01-01T00:00Z", "imp-click-join2/2014-01-01T00:00Z"));

        // feeds owned by a user and classified as secure
        verifyFeedsOwnedByUserAndClassification(feedType, "Financial",
                Arrays.asList("clicks-feed/2014-01-01T00:00Z",
                        "imp-click-join1/2014-01-01T00:00Z", "imp-click-join2/2014-01-01T00:00Z"));
    }

    private static String[] getTestMessageArgs() {
        return new String[]{
            "-" + LineageArgs.NOMINAL_TIME.getOptionName(), NOMINAL_TIME,
            "-" + LineageArgs.TIMESTAMP.getOptionName(), NOMINAL_TIME,

            "-" + LineageArgs.ENTITY_NAME.getOptionName(), PROCESS_ENTITY_NAME,
            "-" + LineageArgs.ENTITY_TYPE.getOptionName(), ("process"),
            "-" + LineageArgs.CLUSTER.getOptionName(), CLUSTER_ENTITY_NAME,
            "-" + LineageArgs.OPERATION.getOptionName(), OPERATION,

            "-" + LineageArgs.INPUT_FEED_NAMES.getOptionName(), INPUT_FEED_NAMES,
            "-" + LineageArgs.INPUT_FEED_PATHS.getOptionName(), INPUT_INSTANCE_PATHS,

            "-" + LineageArgs.FEED_NAMES.getOptionName(), OUTPUT_FEED_NAMES,
            "-" + LineageArgs.FEED_INSTANCE_PATHS.getOptionName(), OUTPUT_INSTANCE_PATHS,

            "-" + LineageArgs.WORKFLOW_ID.getOptionName(), "workflow-01-00",
            "-" + LineageArgs.WORKFLOW_USER.getOptionName(), FALCON_USER,
            "-" + LineageArgs.RUN_ID.getOptionName(), "1",
            "-" + LineageArgs.STATUS.getOptionName(), "SUCCEEDED",
            "-" + LineageArgs.WF_ENGINE_URL.getOptionName(), "http://localhost:11000/oozie",
            "-" + LineageArgs.USER_SUBFLOW_ID.getOptionName(), "userflow@wf-id",
            "-" + LineageArgs.USER_WORKFLOW_NAME.getOptionName(), WORKFLOW_NAME,
            "-" + LineageArgs.USER_WORKFLOW_VERSION.getOptionName(), WORKFLOW_VERSION,
            "-" + LineageArgs.USER_WORKFLOW_ENGINE.getOptionName(), EngineType.PIG.name(),

            "-" + LineageArgs.LOG_DIR.getOptionName(), LOGS_DIR,
        };
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
