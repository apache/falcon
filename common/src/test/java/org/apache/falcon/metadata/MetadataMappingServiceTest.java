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
import org.apache.falcon.cluster.util.EntityBuilderTestUtil;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.CatalogTable;
import org.apache.falcon.entity.v0.feed.ClusterType;
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
import org.apache.falcon.retention.EvictedInstanceSerDe;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.service.Services;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import static org.apache.falcon.workflow.WorkflowExecutionContext.EntityOperations;
import org.apache.falcon.workflow.WorkflowJobEndNotificationService;
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
    private static final String LOGS_DIR = "/falcon/staging/feed/logs";
    private static final String NOMINAL_TIME = "2014-01-01-01-00";

    public static final String CLUSTER_ENTITY_NAME = "primary-cluster";
    public static final String BCP_CLUSTER_ENTITY_NAME = "bcp-cluster";
    public static final String PROCESS_ENTITY_NAME = "sample-process";
    public static final String COLO_NAME = "west-coast";
    public static final String GENERATE_WORKFLOW_NAME = "imp-click-join-workflow";
    public static final String REPLICATION_WORKFLOW_NAME = "replication-policy-workflow";
    private static final String EVICTION_WORKFLOW_NAME = "eviction-policy-workflow";
    public static final String WORKFLOW_VERSION = "1.0.9";

    public static final String INPUT_FEED_NAMES = "impression-feed#clicks-feed";
    public static final String INPUT_INSTANCE_PATHS =
        "jail://global:00/falcon/impression-feed/2014/01/01,jail://global:00/falcon/impression-feed/2014/01/02"
                + "#jail://global:00/falcon/clicks-feed/2014-01-01";
    public static final String INPUT_INSTANCE_PATHS_NO_DATE =
            "jail://global:00/falcon/impression-feed,jail://global:00/falcon/impression-feed"
                    + "#jail://global:00/falcon/clicks-feed";

    public static final String OUTPUT_FEED_NAMES = "imp-click-join1,imp-click-join2";
    public static final String OUTPUT_INSTANCE_PATHS =
        "jail://global:00/falcon/imp-click-join1/20140101,jail://global:00/falcon/imp-click-join2/20140101";
    private static final String REPLICATED_FEED = "raw-click";
    private static final String EVICTED_FEED = "imp-click-join1";
    private static final String EVICTED_INSTANCE_PATHS =
            "jail://global:00/falcon/imp-click-join1/20140101,jail://global:00/falcon/imp-click-join1/20140102";
    public static final String OUTPUT_INSTANCE_PATHS_NO_DATE =
            "jail://global:00/falcon/imp-click-join1,jail://global:00/falcon/imp-click-join2";

    public static final String BROKER = "org.apache.activemq.ActiveMQConnectionFactory";

    private ConfigurationStore configStore;
    private MetadataMappingService service;

    private Cluster clusterEntity;
    private Cluster anotherCluster;
    private List<Feed> inputFeeds = new ArrayList<Feed>();
    private List<Feed> outputFeeds = new ArrayList<Feed>();
    private Process processEntity;

    @BeforeClass
    public void setUp() throws Exception {
        CurrentUser.authenticate(FALCON_USER);

        configStore = ConfigurationStore.get();

        Services.get().register(new WorkflowJobEndNotificationService());
        StartupProperties.get().setProperty("falcon.graph.storage.directory",
                "target/graphdb-" + System.currentTimeMillis());
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

        cleanUp();
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
        clusterEntity = addClusterEntity(CLUSTER_ENTITY_NAME, COLO_NAME,
                "classification=production");

        verifyEntityWasAddedToGraph(CLUSTER_ENTITY_NAME, RelationshipType.CLUSTER_ENTITY);
        verifyClusterEntityEdges();

        Assert.assertEquals(getVerticesCount(service.getGraph()), 3); // +3 = cluster, colo, tag
        Assert.assertEquals(getEdgesCount(service.getGraph()), 2); // +2 = cluster to colo and tag
    }

    @Test (dependsOnMethods = "testOnAddClusterEntity")
    public void testOnAddFeedEntity() throws Exception {
        Feed impressionsFeed = addFeedEntity("impression-feed", clusterEntity,
                "classified-as=Secure", "analytics", Storage.TYPE.FILESYSTEM,
                "/falcon/impression-feed/${YEAR}/${MONTH}/${DAY}");
        inputFeeds.add(impressionsFeed);
        verifyEntityWasAddedToGraph(impressionsFeed.getName(), RelationshipType.FEED_ENTITY);
        verifyFeedEntityEdges(impressionsFeed.getName(), "Secure", "analytics");
        Assert.assertEquals(getVerticesCount(service.getGraph()), 7); // +4 = feed, tag, group, user
        Assert.assertEquals(getEdgesCount(service.getGraph()), 6); // +4 = cluster, tag, group, user

        Feed clicksFeed = addFeedEntity("clicks-feed", clusterEntity,
                "classified-as=Secure,classified-as=Financial", "analytics", Storage.TYPE.FILESYSTEM,
                "/falcon/clicks-feed/${YEAR}-${MONTH}-${DAY}");
        inputFeeds.add(clicksFeed);
        verifyEntityWasAddedToGraph(clicksFeed.getName(), RelationshipType.FEED_ENTITY);
        Assert.assertEquals(getVerticesCount(service.getGraph()), 9); // feed and financial vertex
        Assert.assertEquals(getEdgesCount(service.getGraph()), 11); // +5 = cluster + user + 2Group + Tag

        Feed join1Feed = addFeedEntity("imp-click-join1", clusterEntity,
                "classified-as=Financial", "reporting,bi", Storage.TYPE.FILESYSTEM,
                "/falcon/imp-click-join1/${YEAR}${MONTH}${DAY}");
        outputFeeds.add(join1Feed);
        verifyEntityWasAddedToGraph(join1Feed.getName(), RelationshipType.FEED_ENTITY);
        Assert.assertEquals(getVerticesCount(service.getGraph()), 12); // + 3 = 1 feed and 2 groups
        Assert.assertEquals(getEdgesCount(service.getGraph()), 16); // +5 = cluster + user +
        // Group + 2Tags

        Feed join2Feed = addFeedEntity("imp-click-join2", clusterEntity,
                "classified-as=Secure,classified-as=Financial", "reporting,bi", Storage.TYPE.FILESYSTEM,
                "/falcon/imp-click-join2/${YEAR}${MONTH}${DAY}");
        outputFeeds.add(join2Feed);
        verifyEntityWasAddedToGraph(join2Feed.getName(), RelationshipType.FEED_ENTITY);

        Assert.assertEquals(getVerticesCount(service.getGraph()), 13); // +1 feed
        // +6 = user + 2tags + 2Groups + Cluster
        Assert.assertEquals(getEdgesCount(service.getGraph()), 22);
    }

    @Test (dependsOnMethods = "testOnAddFeedEntity")
    public void testOnAddProcessEntity() throws Exception {
        processEntity = addProcessEntity(PROCESS_ENTITY_NAME, clusterEntity,
                "classified-as=Critical", "testPipeline,dataReplication_Pipeline", GENERATE_WORKFLOW_NAME,
                WORKFLOW_VERSION);

        verifyEntityWasAddedToGraph(processEntity.getName(), RelationshipType.PROCESS_ENTITY);
        verifyProcessEntityEdges();

        // +4 = 1 process + 1 tag + 2 pipeline
        Assert.assertEquals(getVerticesCount(service.getGraph()), 17);
        // +9 = user,tag,cluster, 2 inputs,2 outputs, 2 pipelines
        Assert.assertEquals(getEdgesCount(service.getGraph()), 31);
    }

    @Test (dependsOnMethods = "testOnAddProcessEntity")
    public void testOnAdd() throws Exception {
        verifyEntityGraph(RelationshipType.FEED_ENTITY, "Secure");
    }

    @Test
    public void testMapLineage() throws Exception {
        setup();

        WorkflowExecutionContext context = WorkflowExecutionContext.create(getTestMessageArgs(
                EntityOperations.GENERATE, GENERATE_WORKFLOW_NAME, null, null, null, null)
                , WorkflowExecutionContext.Type.POST_PROCESSING);
        service.onSuccess(context);

        debug(service.getGraph());
        GraphUtils.dump(service.getGraph());
        verifyLineageGraph(RelationshipType.FEED_INSTANCE.getName());

        // +6 = 1 process, 2 inputs = 3 instances,2 outputs
        Assert.assertEquals(getVerticesCount(service.getGraph()), 23);
        //+40 = +26 for feed instances + 8 for process instance + 6 for second feed instance
        Assert.assertEquals(getEdgesCount(service.getGraph()), 71);
    }

    @Test
    public void testLineageForNoDateInFeedPath() throws Exception {
        setupForNoDateInFeedPath();

        WorkflowExecutionContext context = WorkflowExecutionContext.create(getTestMessageArgs(
                        EntityOperations.GENERATE, GENERATE_WORKFLOW_NAME, null,
                        OUTPUT_INSTANCE_PATHS_NO_DATE, INPUT_INSTANCE_PATHS_NO_DATE, null),
                WorkflowExecutionContext.Type.POST_PROCESSING);
        service.onSuccess(context);

        debug(service.getGraph());
        GraphUtils.dump(service.getGraph());

        // Verify if instance name has nominal time
        List<String> feedNamesOwnedByUser = getFeedsOwnedByAUser(
                RelationshipType.FEED_INSTANCE.getName());
        List<String> expected = Arrays.asList("impression-feed/2014-01-01T01:00Z", "clicks-feed/2014-01-01T01:00Z",
                "imp-click-join1/2014-01-01T01:00Z", "imp-click-join2/2014-01-01T01:00Z");
        Assert.assertTrue(feedNamesOwnedByUser.containsAll(expected));

        // +5 = 1 process, 2 inputs, 2 outputs
        Assert.assertEquals(getVerticesCount(service.getGraph()), 22);
        //+34 = +26 for feed instances + 8 for process instance
        Assert.assertEquals(getEdgesCount(service.getGraph()), 65);
    }

    @Test
    public void testLineageForReplication() throws Exception {
        setupForLineageReplication();

        WorkflowExecutionContext context = WorkflowExecutionContext.create(getTestMessageArgs(
                        EntityOperations.REPLICATE, REPLICATION_WORKFLOW_NAME, REPLICATED_FEED,
                        "jail://global:00/falcon/raw-click/bcp/20140101",
                        "jail://global:00/falcon/raw-click/primary/20140101", REPLICATED_FEED),
                WorkflowExecutionContext.Type.POST_PROCESSING);
        service.onSuccess(context);

        debug(service.getGraph());
        GraphUtils.dump(service.getGraph());

        verifyLineageGraphForReplicationOrEviction(REPLICATED_FEED,
                "jail://global:00/falcon/raw-click/bcp/20140101", context,
                RelationshipLabel.FEED_CLUSTER_REPLICATED_EDGE);

        // +6 [primary, bcp cluster] = cluster, colo, tag,
        // +4 [input feed] = feed, tag, group, user
        // +4 [output feed] = 1 feed + 1 tag + 2 groups
        // +4 [process] = 1 process + 1 tag + 2 pipeline
        // +3 = 1 process, 1 input, 1 output
        Assert.assertEquals(getVerticesCount(service.getGraph()), 21);
        // +4 [cluster] = cluster to colo and tag [primary and bcp],
        // +4 [input feed] = cluster, tag, group, user
        // +5 [output feed] = cluster + user + Group + 2Tags
        // +7 = user,tag,cluster, 1 input,1 output, 2 pipelines
        // +19 = +6 for output feed instances + 7 for process instance + 6 for input feed instance
        // +1 for replicated-to edge to target cluster for each output feed instance
        Assert.assertEquals(getEdgesCount(service.getGraph()), 40);
    }

    @Test
    public void testLineageForReplicationForNonGeneratedInstances() throws Exception {
        cleanUp();
        service.init();

        addClusterAndFeedForReplication();
        // Get the vertices before running replication WF
        long beforeVerticesCount = getVerticesCount(service.getGraph());
        long beforeEdgesCount = getEdgesCount(service.getGraph());

        WorkflowExecutionContext context = WorkflowExecutionContext.create(getTestMessageArgs(
                        EntityOperations.REPLICATE, REPLICATION_WORKFLOW_NAME, REPLICATED_FEED,
                        "jail://global:00/falcon/raw-click/bcp/20140101",
                        "jail://global:00/falcon/raw-click/primary/20140101", REPLICATED_FEED),
                WorkflowExecutionContext.Type.POST_PROCESSING);
        service.onSuccess(context);

        debug(service.getGraph());
        GraphUtils.dump(service.getGraph());

        verifyFeedEntityEdges(REPLICATED_FEED, "Secure", "analytics");
        verifyLineageGraphForReplicationOrEviction(REPLICATED_FEED,
                "jail://global:00/falcon/raw-click/bcp/20140101", context,
                RelationshipLabel.FEED_CLUSTER_REPLICATED_EDGE);

        // +1 for the new instance vertex added
        Assert.assertEquals(getVerticesCount(service.getGraph()), beforeVerticesCount + 1);
        // +6 = instance-of, stored-in, owned-by, classification, group, replicated-to
        Assert.assertEquals(getEdgesCount(service.getGraph()), beforeEdgesCount + 6);
    }

    @Test
    public void testLineageForRetention() throws Exception {
        setupForLineageEviction();
        WorkflowExecutionContext context = WorkflowExecutionContext.create(getTestMessageArgs(
                        EntityOperations.DELETE, EVICTION_WORKFLOW_NAME,
                        EVICTED_FEED, EVICTED_INSTANCE_PATHS, "IGNORE", EVICTED_FEED),
                WorkflowExecutionContext.Type.POST_PROCESSING);

        service.onSuccess(context);

        debug(service.getGraph());
        GraphUtils.dump(service.getGraph());
        List<String> expectedFeeds = Arrays.asList("impression-feed/2014-01-01T00:00Z", "clicks-feed/2014-01-01T00:00Z",
                "imp-click-join1/2014-01-01T00:00Z", "imp-click-join1/2014-01-02T00:00Z");
        List<String> secureFeeds = Arrays.asList("impression-feed/2014-01-01T00:00Z",
                "clicks-feed/2014-01-01T00:00Z");
        List<String> ownedAndSecureFeeds = Arrays.asList("clicks-feed/2014-01-01T00:00Z",
                "imp-click-join1/2014-01-01T00:00Z", "imp-click-join1/2014-01-02T00:00Z");
        verifyLineageGraph(RelationshipType.FEED_INSTANCE.getName(), expectedFeeds, secureFeeds, ownedAndSecureFeeds);
        String[] paths = EVICTED_INSTANCE_PATHS.split(EvictedInstanceSerDe.INSTANCEPATH_SEPARATOR);
        for (String feedInstanceDataPath : paths) {
            verifyLineageGraphForReplicationOrEviction(EVICTED_FEED, feedInstanceDataPath, context,
                    RelationshipLabel.FEED_CLUSTER_EVICTED_EDGE);
        }

        // No new vertices added
        Assert.assertEquals(getVerticesCount(service.getGraph()), 23);
        // +1 =  +2 for evicted-from edge from Feed Instance vertex to cluster.
        // -1 imp-click-join1 is added twice instead of imp-click-join2 so there is one less edge as there is no
        // classified-as -> Secure edge.
        Assert.assertEquals(getEdgesCount(service.getGraph()), 72);
    }

    @Test
    public void testLineageForRetentionWithNoFeedsEvicted() throws Exception {
        cleanUp();
        service.init();
        long beforeVerticesCount = getVerticesCount(service.getGraph());
        long beforeEdgesCount = getEdgesCount(service.getGraph());
        WorkflowExecutionContext context = WorkflowExecutionContext.create(getTestMessageArgs(
                        EntityOperations.DELETE, EVICTION_WORKFLOW_NAME,
                        EVICTED_FEED, "IGNORE", "IGNORE", EVICTED_FEED),
                WorkflowExecutionContext.Type.POST_PROCESSING);

        service.onSuccess(context);

        debug(service.getGraph());
        GraphUtils.dump(service.getGraph());
        // No new vertices added
        Assert.assertEquals(getVerticesCount(service.getGraph()), beforeVerticesCount);
        // No new edges added
        Assert.assertEquals(getEdgesCount(service.getGraph()), beforeEdgesCount);
    }

    @Test (dependsOnMethods = "testOnAdd")
    public void testOnChange() throws Exception {
        // shutdown the graph and resurrect for testing
        service.destroy();
        service.init();

        // cannot modify cluster, adding a new cluster
        anotherCluster = addClusterEntity("another-cluster", "east-coast",
                "classification=another");
        verifyEntityWasAddedToGraph("another-cluster", RelationshipType.CLUSTER_ENTITY);

        Assert.assertEquals(getVerticesCount(service.getGraph()), 20); // +3 = cluster, colo, tag
        // +2 edges to above, no user but only to colo and new tag
        Assert.assertEquals(getEdgesCount(service.getGraph()), 33);
    }

    @Test(dependsOnMethods = "testOnChange")
    public void testOnFeedEntityChange() throws Exception {
        Feed oldFeed = inputFeeds.get(0);
        Feed newFeed = EntityBuilderTestUtil.buildFeed(oldFeed.getName(), clusterEntity,
                "classified-as=Secured,source=data-warehouse", "reporting");
        addStorage(newFeed, Storage.TYPE.FILESYSTEM,
                "jail://global:00/falcon/impression-feed/20140101");

        try {
            configStore.initiateUpdate(newFeed);

            // add cluster
            org.apache.falcon.entity.v0.feed.Cluster feedCluster =
                    new org.apache.falcon.entity.v0.feed.Cluster();
            feedCluster.setName(anotherCluster.getName());
            newFeed.getClusters().getClusters().add(feedCluster);

            configStore.update(EntityType.FEED, newFeed);
        } finally {
            configStore.cleanupUpdateInit();
        }

        verifyUpdatedEdges(newFeed);
        Assert.assertEquals(getVerticesCount(service.getGraph()), 22); //+2 = 2 new tags
        Assert.assertEquals(getEdgesCount(service.getGraph()), 35); // +2 = 1 new cluster, 1 new tag
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
        Assert.assertTrue(actual.containsAll(Arrays.asList("primary-cluster", "another-cluster")),
                "Actual does not contain expected: " + actual);
    }

    @Test(dependsOnMethods = "testOnFeedEntityChange")
    public void testOnProcessEntityChange() throws Exception {
        Process oldProcess = processEntity;
        Process newProcess = EntityBuilderTestUtil.buildProcess(oldProcess.getName(), anotherCluster,
                null, null);
        EntityBuilderTestUtil.addProcessWorkflow(newProcess, GENERATE_WORKFLOW_NAME, "2.0.0");
        EntityBuilderTestUtil.addInput(newProcess, inputFeeds.get(0));

        try {
            configStore.initiateUpdate(newProcess);
            configStore.update(EntityType.PROCESS, newProcess);
        } finally {
            configStore.cleanupUpdateInit();
        }

        verifyUpdatedEdges(newProcess);
        Assert.assertEquals(getVerticesCount(service.getGraph()), 22); // +0, no net new
        Assert.assertEquals(getEdgesCount(service.getGraph()), 29); // -6 = -2 outputs, -1 tag, -1 cluster, -2 pipelines
    }

    @Test(dependsOnMethods = "testOnProcessEntityChange")
    public void testAreSame() throws Exception {

        Inputs inputs1 = new Inputs();
        Inputs inputs2 = new Inputs();
        Outputs outputs1 = new Outputs();
        Outputs outputs2 = new Outputs();
        // return true when both are null
        Assert.assertTrue(EntityRelationshipGraphBuilder.areSame(inputs1, inputs2));
        Assert.assertTrue(EntityRelationshipGraphBuilder.areSame(outputs1, outputs2));

        Input i1 = new Input();
        i1.setName("input1");
        Input i2 = new Input();
        i2.setName("input2");
        Output o1 = new Output();
        o1.setName("output1");
        Output o2 = new Output();
        o2.setName("output2");

        inputs1.getInputs().add(i1);
        Assert.assertFalse(EntityRelationshipGraphBuilder.areSame(inputs1, inputs2));
        outputs1.getOutputs().add(o1);
        Assert.assertFalse(EntityRelationshipGraphBuilder.areSame(outputs1, outputs2));

        inputs2.getInputs().add(i1);
        Assert.assertTrue(EntityRelationshipGraphBuilder.areSame(inputs1, inputs2));
        outputs2.getOutputs().add(o1);
        Assert.assertTrue(EntityRelationshipGraphBuilder.areSame(outputs1, outputs2));
    }

    private void verifyUpdatedEdges(Process newProcess) {
        Vertex processVertex = getEntityVertex(newProcess.getName(), RelationshipType.PROCESS_ENTITY);

        // cluster
        Edge edge = processVertex.getEdges(Direction.OUT,
                RelationshipLabel.PROCESS_CLUSTER_EDGE.getName()).iterator().next();
        Assert.assertEquals(edge.getVertex(Direction.IN).getProperty("name"), anotherCluster.getName());

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

    private Cluster addClusterEntity(String name, String colo, String tags) throws Exception {
        Cluster cluster = EntityBuilderTestUtil.buildCluster(name, colo, tags);
        configStore.publish(EntityType.CLUSTER, cluster);
        return cluster;
    }

    private Feed addFeedEntity(String feedName, Cluster cluster, String tags, String groups,
                               Storage.TYPE storageType, String uriTemplate) throws Exception {
        return addFeedEntity(feedName, new Cluster[]{cluster}, tags, groups, storageType, uriTemplate);
    }

    private Feed addFeedEntity(String feedName, Cluster[] clusters, String tags, String groups,
                               Storage.TYPE storageType, String uriTemplate) throws Exception {
        Feed feed = EntityBuilderTestUtil.buildFeed(feedName, clusters,
                tags, groups);
        addStorage(feed, storageType, uriTemplate);
        for (org.apache.falcon.entity.v0.feed.Cluster feedCluster : feed.getClusters().getClusters()) {
            if (feedCluster.getName().equals(BCP_CLUSTER_ENTITY_NAME)) {
                feedCluster.setType(ClusterType.TARGET);
            }
        }
        configStore.publish(EntityType.FEED, feed);
        return feed;
    }

    public Process addProcessEntity(String processName, Cluster cluster,
                                    String tags, String pipelineTags, String workflowName,
                                    String version) throws Exception {
        Process process = EntityBuilderTestUtil.buildProcess(processName, cluster,
                tags, pipelineTags);
        EntityBuilderTestUtil.addProcessWorkflow(process, workflowName, version);

        for (Feed inputFeed : inputFeeds) {
            EntityBuilderTestUtil.addInput(process, inputFeed);
        }

        for (Feed outputFeed : outputFeeds) {
            EntityBuilderTestUtil.addOutput(process, outputFeed);
        }

        configStore.publish(EntityType.PROCESS, process);
        return process;
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

    private static void addStorage(org.apache.falcon.entity.v0.feed.Cluster cluster, Feed feed,
                                   Storage.TYPE storageType, String uriTemplate) {
        if (storageType == Storage.TYPE.FILESYSTEM) {
            Locations locations = new Locations();
            feed.setLocations(locations);

            Location location = new Location();
            location.setType(LocationType.DATA);
            location.setPath(uriTemplate);
            cluster.setLocations(new Locations());
            cluster.getLocations().getLocations().add(location);
        } else {
            CatalogTable table = new CatalogTable();
            table.setUri(uriTemplate);
            cluster.setTable(table);
        }
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

    private void verifyFeedEntityEdges(String feedName, String tag, String group) {
        Vertex feedVertex = getEntityVertex(feedName, RelationshipType.FEED_ENTITY);

        // verify edge to cluster vertex
        verifyVertexForEdge(feedVertex, Direction.OUT, RelationshipLabel.FEED_CLUSTER_EDGE.getName(),
                CLUSTER_ENTITY_NAME, RelationshipType.CLUSTER_ENTITY.getName());
        // verify edge to user vertex
        verifyVertexForEdge(feedVertex, Direction.OUT, RelationshipLabel.USER.getName(),
                FALCON_USER, RelationshipType.USER.getName());

        // verify edge to tags vertex
        verifyVertexForEdge(feedVertex, Direction.OUT, "classified-as",
                tag, RelationshipType.TAGS.getName());
        // verify edge to group vertex
        verifyVertexForEdge(feedVertex, Direction.OUT, RelationshipLabel.GROUPS.getName(),
                group, RelationshipType.GROUPS.getName());
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
                Arrays.asList("impression-feed", "clicks-feed", "imp-click-join1",
                        "imp-click-join2")
        );

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
        Assert.assertTrue(actual.containsAll(expected),
                "Actual does not contain expected: " + actual);
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
        List<String> expectedFeeds = Arrays.asList("impression-feed/2014-01-01T00:00Z", "clicks-feed/2014-01-01T00:00Z",
                "imp-click-join1/2014-01-01T00:00Z", "imp-click-join2/2014-01-01T00:00Z");
        List<String> secureFeeds = Arrays.asList("impression-feed/2014-01-01T00:00Z",
                "clicks-feed/2014-01-01T00:00Z", "imp-click-join2/2014-01-01T00:00Z");
        List<String> ownedAndSecureFeeds = Arrays.asList("clicks-feed/2014-01-01T00:00Z",
                "imp-click-join1/2014-01-01T00:00Z", "imp-click-join2/2014-01-01T00:00Z");
        verifyLineageGraph(feedType, expectedFeeds, secureFeeds, ownedAndSecureFeeds);
    }

    private void verifyLineageGraph(String feedType, List<String> expectedFeeds,
                                    List<String> secureFeeds, List<String> ownedAndSecureFeeds) {
        // feeds owned by a user
        List<String> feedNamesOwnedByUser = getFeedsOwnedByAUser(feedType);
        Assert.assertTrue(feedNamesOwnedByUser.containsAll(expectedFeeds));

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
        verifyFeedsClassifiedAsSecure(feedType, secureFeeds);

        // feeds owned by a user and classified as secure
        verifyFeedsOwnedByUserAndClassification(feedType, "Financial", ownedAndSecureFeeds);
    }

    private void verifyLineageGraphForReplicationOrEviction(String feedName, String feedInstanceDataPath,
                                                            WorkflowExecutionContext context,
                                                            RelationshipLabel edgeLabel) throws Exception {
        String feedInstanceName = InstanceRelationshipGraphBuilder.getFeedInstanceName(feedName
                , context.getClusterName(), feedInstanceDataPath, context.getNominalTimeAsISO8601());
        Vertex feedVertex = getEntityVertex(feedInstanceName, RelationshipType.FEED_INSTANCE);

        Edge edge = feedVertex.getEdges(Direction.OUT, edgeLabel.getName())
                .iterator().next();
        Assert.assertNotNull(edge);
        Assert.assertEquals(edge.getProperty(RelationshipProperty.TIMESTAMP.getName())
                , context.getTimeStampAsISO8601());

        Vertex clusterVertex = edge.getVertex(Direction.IN);
        Assert.assertEquals(clusterVertex.getProperty(RelationshipProperty.NAME.getName()), context.getClusterName());
    }

    private static String[] getTestMessageArgs(EntityOperations operation, String wfName, String outputFeedNames,
                                               String feedInstancePaths, String falconInputPaths,
                                               String falconInputFeeds) {
        String cluster;
        if (EntityOperations.REPLICATE == operation) {
            cluster = BCP_CLUSTER_ENTITY_NAME + WorkflowExecutionContext.CLUSTER_NAME_SEPARATOR + CLUSTER_ENTITY_NAME;
        } else {
            cluster = CLUSTER_ENTITY_NAME;
        }

        return new String[]{
            "-" + WorkflowExecutionArgs.CLUSTER_NAME.getName(), cluster,
            "-" + WorkflowExecutionArgs.ENTITY_TYPE.getName(), ("process"),
            "-" + WorkflowExecutionArgs.ENTITY_NAME.getName(), PROCESS_ENTITY_NAME,
            "-" + WorkflowExecutionArgs.NOMINAL_TIME.getName(), NOMINAL_TIME,
            "-" + WorkflowExecutionArgs.OPERATION.getName(), operation.toString(),

            "-" + WorkflowExecutionArgs.INPUT_FEED_NAMES.getName(),
            (falconInputFeeds != null ? falconInputFeeds : INPUT_FEED_NAMES),
            "-" + WorkflowExecutionArgs.INPUT_FEED_PATHS.getName(),
            (falconInputPaths != null ? falconInputPaths : INPUT_INSTANCE_PATHS),

            "-" + WorkflowExecutionArgs.OUTPUT_FEED_NAMES.getName(),
            (outputFeedNames != null ? outputFeedNames : OUTPUT_FEED_NAMES),
            "-" + WorkflowExecutionArgs.OUTPUT_FEED_PATHS.getName(),
            (feedInstancePaths != null ? feedInstancePaths : OUTPUT_INSTANCE_PATHS),

            "-" + WorkflowExecutionArgs.WORKFLOW_ID.getName(), "workflow-01-00",
            "-" + WorkflowExecutionArgs.WORKFLOW_USER.getName(), FALCON_USER,
            "-" + WorkflowExecutionArgs.RUN_ID.getName(), "1",
            "-" + WorkflowExecutionArgs.STATUS.getName(), "SUCCEEDED",
            "-" + WorkflowExecutionArgs.TIMESTAMP.getName(), NOMINAL_TIME,

            "-" + WorkflowExecutionArgs.WF_ENGINE_URL.getName(), "http://localhost:11000/oozie",
            "-" + WorkflowExecutionArgs.USER_SUBFLOW_ID.getName(), "userflow@wf-id",
            "-" + WorkflowExecutionArgs.USER_WORKFLOW_NAME.getName(), wfName,
            "-" + WorkflowExecutionArgs.USER_WORKFLOW_VERSION.getName(), WORKFLOW_VERSION,
            "-" + WorkflowExecutionArgs.USER_WORKFLOW_ENGINE.getName(), EngineType.PIG.name(),

            "-" + WorkflowExecutionArgs.BRKR_IMPL_CLASS.getName(), BROKER,
            "-" + WorkflowExecutionArgs.BRKR_URL.getName(), "tcp://localhost:61616?daemon=true",
            "-" + WorkflowExecutionArgs.USER_BRKR_IMPL_CLASS.getName(), BROKER,
            "-" + WorkflowExecutionArgs.USER_BRKR_URL.getName(), "tcp://localhost:61616?daemon=true",
            "-" + WorkflowExecutionArgs.BRKR_TTL.getName(), "1000",

            "-" + WorkflowExecutionArgs.LOG_DIR.getName(), LOGS_DIR,
        };
    }

    private void setup() throws Exception {
        cleanUp();
        service.init();

        // Add cluster
        clusterEntity = addClusterEntity(CLUSTER_ENTITY_NAME, COLO_NAME,
                "classification=production");

        addFeedsAndProcess(clusterEntity);
    }

    private void addFeedsAndProcess(Cluster cluster) throws Exception  {
        // Add input and output feeds
        Feed impressionsFeed = addFeedEntity("impression-feed", cluster,
                "classified-as=Secure", "analytics", Storage.TYPE.FILESYSTEM,
                "/falcon/impression-feed/${YEAR}/${MONTH}/${DAY}");
        inputFeeds.add(impressionsFeed);
        Feed clicksFeed = addFeedEntity("clicks-feed", cluster,
                "classified-as=Secure,classified-as=Financial", "analytics", Storage.TYPE.FILESYSTEM,
                "/falcon/clicks-feed/${YEAR}-${MONTH}-${DAY}");
        inputFeeds.add(clicksFeed);
        Feed join1Feed = addFeedEntity("imp-click-join1", cluster,
                "classified-as=Financial", "reporting,bi", Storage.TYPE.FILESYSTEM,
                "/falcon/imp-click-join1/${YEAR}${MONTH}${DAY}");
        outputFeeds.add(join1Feed);
        Feed join2Feed = addFeedEntity("imp-click-join2", cluster,
                "classified-as=Secure,classified-as=Financial", "reporting,bi", Storage.TYPE.FILESYSTEM,
                "/falcon/imp-click-join2/${YEAR}${MONTH}${DAY}");
        outputFeeds.add(join2Feed);
        processEntity = addProcessEntity(PROCESS_ENTITY_NAME, clusterEntity,
                "classified-as=Critical", "testPipeline,dataReplication_Pipeline", GENERATE_WORKFLOW_NAME,
                WORKFLOW_VERSION);
    }

    private void setupForLineageReplication() throws Exception {
        cleanUp();
        service.init();

        addClusterAndFeedForReplication();

        // Add output feed
        Feed join1Feed = addFeedEntity("imp-click-join1", clusterEntity,
                "classified-as=Financial", "reporting,bi", Storage.TYPE.FILESYSTEM,
                "/falcon/imp-click-join1/${YEAR}${MONTH}${DAY}");
        outputFeeds.add(join1Feed);

        processEntity = addProcessEntity(PROCESS_ENTITY_NAME, clusterEntity,
                "classified-as=Critical", "testPipeline,dataReplication_Pipeline", GENERATE_WORKFLOW_NAME,
                WORKFLOW_VERSION);

        // GENERATE WF should have run before this to create all instance related vertices
        WorkflowExecutionContext context = WorkflowExecutionContext.create(getTestMessageArgs(
                EntityOperations.GENERATE, GENERATE_WORKFLOW_NAME, "imp-click-join1",
                "jail://global:00/falcon/imp-click-join1/20140101",
                "jail://global:00/falcon/raw-click/primary/20140101",
                REPLICATED_FEED), WorkflowExecutionContext.Type.POST_PROCESSING);
        service.onSuccess(context);
    }

    private void addClusterAndFeedForReplication() throws Exception {
        // Add cluster
        clusterEntity = addClusterEntity(CLUSTER_ENTITY_NAME, COLO_NAME,
                "classification=production");
        // Add backup cluster
        Cluster bcpCluster = addClusterEntity(BCP_CLUSTER_ENTITY_NAME, "east-coast", "classification=bcp");

        Cluster[] clusters = {clusterEntity, bcpCluster};

        // Add feed
        Feed rawFeed = addFeedEntity(REPLICATED_FEED, clusters,
                "classified-as=Secure", "analytics", Storage.TYPE.FILESYSTEM,
                "/falcon/raw-click/${YEAR}/${MONTH}/${DAY}");
        // Add uri template for each cluster
        for (org.apache.falcon.entity.v0.feed.Cluster feedCluster : rawFeed.getClusters().getClusters()) {
            if (feedCluster.getName().equals(CLUSTER_ENTITY_NAME)) {
                addStorage(feedCluster, rawFeed, Storage.TYPE.FILESYSTEM,
                        "/falcon/raw-click/primary/${YEAR}/${MONTH}/${DAY}");
            } else {
                addStorage(feedCluster, rawFeed, Storage.TYPE.FILESYSTEM,
                        "/falcon/raw-click/bcp/${YEAR}/${MONTH}/${DAY}");
            }
        }

        // update config store
        try {
            configStore.initiateUpdate(rawFeed);
            configStore.update(EntityType.FEED, rawFeed);
        } finally {
            configStore.cleanupUpdateInit();
        }
        inputFeeds.add(rawFeed);
    }

    private void setupForLineageEviction() throws Exception {
        setup();

        // GENERATE WF should have run before this to create all instance related vertices
        WorkflowExecutionContext context = WorkflowExecutionContext.create(getTestMessageArgs(
                        EntityOperations.GENERATE, GENERATE_WORKFLOW_NAME,
                        "imp-click-join1,imp-click-join1", EVICTED_INSTANCE_PATHS, null, null),
                WorkflowExecutionContext.Type.POST_PROCESSING);
        service.onSuccess(context);
    }

    private void setupForNoDateInFeedPath() throws Exception {
        cleanUp();
        service.init();

        // Add cluster
        clusterEntity = addClusterEntity(CLUSTER_ENTITY_NAME, COLO_NAME,
                "classification=production");

        // Add input and output feeds
        Feed impressionsFeed = addFeedEntity("impression-feed", clusterEntity,
                "classified-as=Secure", "analytics", Storage.TYPE.FILESYSTEM,
                "/falcon/impression-feed");
        inputFeeds.add(impressionsFeed);
        Feed clicksFeed = addFeedEntity("clicks-feed", clusterEntity,
                "classified-as=Secure,classified-as=Financial", "analytics", Storage.TYPE.FILESYSTEM,
                "/falcon/clicks-feed");
        inputFeeds.add(clicksFeed);
        Feed join1Feed = addFeedEntity("imp-click-join1", clusterEntity,
                "classified-as=Financial", "reporting,bi", Storage.TYPE.FILESYSTEM,
                "/falcon/imp-click-join1");
        outputFeeds.add(join1Feed);
        Feed join2Feed = addFeedEntity("imp-click-join2", clusterEntity,
                "classified-as=Secure,classified-as=Financial", "reporting,bi", Storage.TYPE.FILESYSTEM,
                "/falcon/imp-click-join2");
        outputFeeds.add(join2Feed);
        processEntity = addProcessEntity(PROCESS_ENTITY_NAME, clusterEntity,
                "classified-as=Critical", "testPipeline,dataReplication_Pipeline", GENERATE_WORKFLOW_NAME,
                WORKFLOW_VERSION);

    }

    private void cleanUp() throws Exception {
        cleanupGraphStore(service.getGraph());
        cleanupConfigurationStore(configStore);
        service.destroy();
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
