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

import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.CatalogStorage;
import org.apache.falcon.entity.FeedHelper;
import org.apache.falcon.entity.Storage;
import org.apache.falcon.entity.common.FeedDataPath;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.SchemaHelper;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.feed.LocationType;
import org.apache.falcon.entity.v0.process.Process;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Map;

/**
 * Instance Metadata relationship mapping helper.
 */
public class InstanceRelationshipGraphBuilder extends RelationshipGraphBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(InstanceRelationshipGraphBuilder.class);

    private static final String PROCESS_INSTANCE_FORMAT = "yyyy-MM-dd-HH-mm"; // nominal time
    private static final String FEED_INSTANCE_FORMAT = "yyyyMMddHHmm"; // computed

    // process workflow properties from message
    private static final String[] INSTANCE_WORKFLOW_PROPERTIES = {
        LineageArgs.USER_WORKFLOW_NAME.getOptionName(),
        LineageArgs.USER_WORKFLOW_ENGINE.getOptionName(),
        LineageArgs.WORKFLOW_ID.getOptionName(),
        LineageArgs.RUN_ID.getOptionName(),
        LineageArgs.STATUS.getOptionName(),
        LineageArgs.WF_ENGINE_URL.getOptionName(),
        LineageArgs.USER_SUBFLOW_ID.getOptionName(),
    };


    public InstanceRelationshipGraphBuilder(Graph graph, boolean preserveHistory) {
        super(graph, preserveHistory);
    }

    public Vertex addProcessInstance(Map<String, String> lineageMetadata) throws FalconException {
        String entityName = lineageMetadata.get(LineageArgs.ENTITY_NAME.getOptionName());
        String processInstanceName = getProcessInstanceName(entityName,
                lineageMetadata.get(LineageArgs.NOMINAL_TIME.getOptionName()));
        LOG.info("Adding process instance: {}", processInstanceName);

        String timestamp = getTimestamp(lineageMetadata);
        Vertex processInstance = addVertex(processInstanceName, RelationshipType.PROCESS_INSTANCE, timestamp);
        addWorkflowInstanceProperties(processInstance, lineageMetadata);

        addInstanceToEntity(processInstance, entityName,
                RelationshipType.PROCESS_ENTITY, RelationshipLabel.INSTANCE_ENTITY_EDGE);
        addInstanceToEntity(processInstance, lineageMetadata.get(LineageArgs.CLUSTER.getOptionName()),
                RelationshipType.CLUSTER_ENTITY, RelationshipLabel.PROCESS_CLUSTER_EDGE);
        addInstanceToEntity(processInstance, lineageMetadata.get(LineageArgs.WORKFLOW_USER.getOptionName()),
                RelationshipType.USER, RelationshipLabel.USER);

        if (isPreserveHistory()) {
            Process process = ConfigurationStore.get().get(EntityType.PROCESS, entityName);
            addDataClassification(process.getTags(), processInstance);
        }

        return processInstance;
    }

    private String getTimestamp(Map<String, String> lineageMetadata) {
        String timestamp = lineageMetadata.get(LineageArgs.TIMESTAMP.getOptionName());
        return SchemaHelper.formatDateUTCToISO8601(timestamp, PROCESS_INSTANCE_FORMAT);
    }

    public void addWorkflowInstanceProperties(Vertex processInstance,
                                              Map<String, String> lineageMetadata) {
        for (String instanceWorkflowProperty : INSTANCE_WORKFLOW_PROPERTIES) {
            addProperty(processInstance, lineageMetadata, instanceWorkflowProperty);
        }
        processInstance.setProperty(RelationshipProperty.VERSION.getName(),
                lineageMetadata.get(LineageArgs.USER_WORKFLOW_VERSION.getOptionName()));
    }

    public String getProcessInstanceName(String entityName,
                                         String nominalTime) {
        return entityName + "/"
                + SchemaHelper.formatDateUTCToISO8601(nominalTime, PROCESS_INSTANCE_FORMAT);
    }

    public void addInstanceToEntity(Vertex instanceVertex, String entityName,
                                    RelationshipType entityType, RelationshipLabel edgeLabel) {
        Vertex entityVertex = findVertex(entityName, entityType);
        LOG.info("Vertex exists? name={}, type={}, v={}", entityName, entityType, entityVertex);
        if (entityVertex == null) {
            // todo - throw new IllegalStateException(entityType + " entity vertex must exist " + entityName);
            LOG.error("Illegal State: {} vertex must exist for {}", entityType, entityName);
            return;
        }

        addEdge(instanceVertex, entityVertex, edgeLabel.getName());
    }

    public void addOutputFeedInstances(Map<String, String> lineageMetadata,
                                       Vertex processInstance) throws FalconException {
        String outputFeedNamesArg = lineageMetadata.get(LineageArgs.FEED_NAMES.getOptionName());
        if ("NONE".equals(outputFeedNamesArg)) {
            return; // there are no output feeds for this process
        }

        String[] outputFeedNames = outputFeedNamesArg.split(",");
        String[] outputFeedInstancePaths =
                lineageMetadata.get(LineageArgs.FEED_INSTANCE_PATHS.getOptionName()).split(",");

        addFeedInstances(outputFeedNames, outputFeedInstancePaths,
                processInstance, RelationshipLabel.PROCESS_FEED_EDGE, lineageMetadata);
    }

    public void addInputFeedInstances(Map<String, String> lineageMetadata,
                                      Vertex processInstance) throws FalconException {
        String inputFeedNamesArg = lineageMetadata.get(LineageArgs.INPUT_FEED_NAMES.getOptionName());
        if ("NONE".equals(inputFeedNamesArg)) {
            return; // there are no input feeds for this process
        }

        String[] inputFeedNames =
                lineageMetadata.get(LineageArgs.INPUT_FEED_NAMES.getOptionName()).split("#");
        String[] inputFeedInstancePaths =
                lineageMetadata.get(LineageArgs.INPUT_FEED_PATHS.getOptionName()).split("#");

        addFeedInstances(inputFeedNames, inputFeedInstancePaths,
                processInstance, RelationshipLabel.FEED_PROCESS_EDGE, lineageMetadata);
    }

    public void addFeedInstances(String[] feedNames, String[] feedInstancePaths,
                                  Vertex processInstance, RelationshipLabel edgeLabel,
                                  Map<String, String> lineageMetadata) throws FalconException {
        String clusterName = lineageMetadata.get(LineageArgs.CLUSTER.getOptionName());

        for (int index = 0; index < feedNames.length; index++) {
            String feedName = feedNames[index];
            String feedInstancePath = feedInstancePaths[index];

            LOG.info("Computing feed instance for : name={}, path={}, in cluster: {}",
                    feedName, feedInstancePath, clusterName);
            String feedInstanceName = getFeedInstanceName(feedName, clusterName, feedInstancePath);
            LOG.info("Adding feed instance: {}", feedInstanceName);
            Vertex feedInstance = addVertex(feedInstanceName, RelationshipType.FEED_INSTANCE,
                    getTimestamp(lineageMetadata));

            addProcessFeedEdge(processInstance, feedInstance, edgeLabel);

            addInstanceToEntity(feedInstance, feedName,
                    RelationshipType.FEED_ENTITY, RelationshipLabel.INSTANCE_ENTITY_EDGE);
            addInstanceToEntity(feedInstance, lineageMetadata.get(LineageArgs.CLUSTER.getOptionName()),
                    RelationshipType.CLUSTER_ENTITY, RelationshipLabel.FEED_CLUSTER_EDGE);
            addInstanceToEntity(feedInstance, lineageMetadata.get(LineageArgs.WORKFLOW_USER.getOptionName()),
                    RelationshipType.USER, RelationshipLabel.USER);

            if (isPreserveHistory()) {
                Feed feed = ConfigurationStore.get().get(EntityType.FEED, feedName);
                addDataClassification(feed.getTags(), feedInstance);
                addGroups(feed.getGroups(), feedInstance);
            }
        }
    }

    public String getFeedInstanceName(String feedName, String clusterName,
                                      String feedInstancePath) throws FalconException {
        try {
            Feed feed = ConfigurationStore.get().get(EntityType.FEED, feedName);
            Cluster cluster = ConfigurationStore.get().get(EntityType.CLUSTER, clusterName);

            Storage.TYPE storageType = FeedHelper.getStorageType(feed, cluster);
            return storageType == Storage.TYPE.TABLE
                    ? getTableFeedInstanceName(feed, feedInstancePath, storageType)
                    : getFileSystemFeedInstanceName(feedInstancePath, feed, cluster);

        } catch (URISyntaxException e) {
            throw new FalconException(e);
        }
    }

    private String getTableFeedInstanceName(Feed feed, String feedInstancePath,
                                            Storage.TYPE storageType) throws URISyntaxException {
        CatalogStorage instanceStorage = (CatalogStorage) FeedHelper.createStorage(
                storageType.name(), feedInstancePath);
        return feed.getName() + "/" + instanceStorage.toPartitionAsPath();
    }

    private String getFileSystemFeedInstanceName(String feedInstancePath, Feed feed,
                                                 Cluster cluster) throws FalconException {
        Storage rawStorage = FeedHelper.createStorage(cluster, feed);
        String feedPathTemplate = rawStorage.getUriTemplate(LocationType.DATA);
        String instance = feedInstancePath;

        String[] elements = FeedDataPath.PATTERN.split(feedPathTemplate);
        for (String element : elements) {
            instance = instance.replaceFirst(element, "");
        }

        return feed.getName() + "/"
                + SchemaHelper.formatDateUTCToISO8601(instance, FEED_INSTANCE_FORMAT);
    }
}
