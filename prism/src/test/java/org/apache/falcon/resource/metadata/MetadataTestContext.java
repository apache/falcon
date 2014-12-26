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

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.falcon.cluster.util.EntityBuilderTestUtil;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.*;
import org.apache.falcon.entity.v0.process.EngineType;
import org.apache.falcon.metadata.MetadataMappingService;
import org.apache.falcon.security.CurrentUser;
import org.apache.falcon.service.Services;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.WorkflowJobEndNotificationService;
import org.testng.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Base test class for Metadata Rest API.
 */
public class MetadataTestContext {
    public static final String FALCON_USER = "falcon-user";
    private static final String LOGS_DIR = "target/log";
    private static final String NOMINAL_TIME = "2014-01-01-01-00";
    public static final String OPERATION = "GENERATE";

    public static final String CLUSTER_ENTITY_NAME = "primary-cluster";
    public static final String CHILD_PROCESS_ENTITY_NAME = "sample-child-process";
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

    public MetadataTestContext() {}

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

    public MetadataMappingService getService() {
        return this.service;
    }

    public void tearDown() throws Exception {
        cleanupGraphStore(service.getGraph());
        cleanupConfigurationStore(configStore);

        service.destroy();

        StartupProperties.get().setProperty("falcon.graph.preserve.history", "false");
        Services.get().reset();
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
        org.apache.falcon.entity.v0.process.Process processEntity =
                EntityBuilderTestUtil.buildProcess(PROCESS_ENTITY_NAME,
                clusterEntity, "classified-as=Critical", "testPipeline");
        EntityBuilderTestUtil.addProcessWorkflow(processEntity, WORKFLOW_NAME, WORKFLOW_VERSION);

        for (Feed inputFeed : inputFeeds) {
            EntityBuilderTestUtil.addInput(processEntity, inputFeed);
        }

        for (Feed outputFeed : outputFeeds) {
            EntityBuilderTestUtil.addOutput(processEntity, outputFeed);
        }

        configStore.publish(EntityType.PROCESS, processEntity);
    }

    public void addConsumerProcess() throws Exception {
        org.apache.falcon.entity.v0.process.Process processEntity =
                EntityBuilderTestUtil.buildProcess(CHILD_PROCESS_ENTITY_NAME,
                        clusterEntity, "classified-as=Critical", "testPipeline");
        EntityBuilderTestUtil.addProcessWorkflow(processEntity, WORKFLOW_NAME, WORKFLOW_VERSION);

        for (Feed inputFeed : inputFeeds) {
            EntityBuilderTestUtil.addOutput(processEntity, inputFeed);
        }

        for (Feed outputFeed : outputFeeds) {
            EntityBuilderTestUtil.addInput(processEntity, outputFeed);
        }

        configStore.publish(EntityType.PROCESS, processEntity);
    }

    public void addInstance() throws Exception {
        WorkflowExecutionContext context = WorkflowExecutionContext.create(getTestMessageArgs(),
                WorkflowExecutionContext.Type.POST_PROCESSING);
        service.onSuccess(context);
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
}
