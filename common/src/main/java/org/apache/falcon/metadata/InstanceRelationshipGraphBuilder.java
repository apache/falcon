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
import org.apache.commons.lang3.StringUtils;
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
import org.apache.falcon.workflow.WorkflowExecutionArgs;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

/**
 * Instance Metadata relationship mapping helper.
 */
public class InstanceRelationshipGraphBuilder extends RelationshipGraphBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(InstanceRelationshipGraphBuilder.class);

    private static final String FEED_INSTANCE_FORMAT = "yyyyMMddHHmm"; // computed
    private static final String NONE = "NONE";
    private static final String IGNORE = "IGNORE";

    // process workflow properties from message
    private static final WorkflowExecutionArgs[] INSTANCE_WORKFLOW_PROPERTIES = {
        WorkflowExecutionArgs.USER_WORKFLOW_NAME,
        WorkflowExecutionArgs.USER_WORKFLOW_ENGINE,
        WorkflowExecutionArgs.WORKFLOW_ID,
        WorkflowExecutionArgs.RUN_ID,
        WorkflowExecutionArgs.STATUS,
        WorkflowExecutionArgs.WF_ENGINE_URL,
        WorkflowExecutionArgs.USER_SUBFLOW_ID,
    };


    public InstanceRelationshipGraphBuilder(Graph graph, boolean preserveHistory) {
        super(graph, preserveHistory);
    }

    public Vertex addProcessInstance(WorkflowExecutionContext context) throws FalconException {
        String processInstanceName = getProcessInstanceName(context);
        LOG.info("Adding process instance: {}", processInstanceName);

        Vertex processInstance = addVertex(processInstanceName,
                RelationshipType.PROCESS_INSTANCE, context.getTimeStampAsLong());
        addWorkflowInstanceProperties(processInstance, context);

        Map<RelationshipProperty, String> properties = edgePropertiesForIndexing(context);
        addInstanceToEntity(processInstance, context.getEntityName(),
                RelationshipType.PROCESS_ENTITY, RelationshipLabel.INSTANCE_ENTITY_EDGE, properties);
        addInstanceToEntity(processInstance, context.getClusterName(),
                RelationshipType.CLUSTER_ENTITY, RelationshipLabel.PROCESS_CLUSTER_EDGE);
        addInstanceToEntity(processInstance, context.getWorkflowUser(),
                RelationshipType.USER, RelationshipLabel.USER);

        if (isPreserveHistory()) {
            Process process = ConfigurationStore.get().get(EntityType.PROCESS, context.getEntityName());
            addDataClassification(process.getTags(), processInstance);
            addPipelines(process.getPipelines(), processInstance);
        }

        addCounters(processInstance, context);

        return processInstance;
    }

    private void addCounters(Vertex processInstance, WorkflowExecutionContext context) throws FalconException {
        String counterString = getCounterString(context);
        if (!StringUtils.isBlank(counterString)) {
            addCountersToInstance(counterString, processInstance);
        }
    }

    private String getCounterString(WorkflowExecutionContext context) {
        if (!StringUtils.isBlank(context.getCounters())) {
            return context.getCounters();
        }
        return null;
    }

    public String getProcessInstanceName(WorkflowExecutionContext context) {
        return context.getEntityName() + "/" + context.getNominalTimeAsISO8601();
    }

    public void addWorkflowInstanceProperties(Vertex processInstance,
                                              WorkflowExecutionContext context) {
        for (WorkflowExecutionArgs instanceWorkflowProperty : INSTANCE_WORKFLOW_PROPERTIES) {
            addProperty(processInstance, context, instanceWorkflowProperty);
        }

        processInstance.setProperty(RelationshipProperty.VERSION.getName(),
                context.getUserWorkflowVersion());
    }

    private void addProperty(Vertex vertex, WorkflowExecutionContext context,
                             WorkflowExecutionArgs optionName) {
        String value = context.getValue(optionName);
        if (value == null || value.length() == 0) {
            return;
        }

        vertex.setProperty(optionName.getName(), value);
    }

    private void addCountersToInstance(String counterString, Vertex vertex) throws FalconException {
        String[] counterKeyValues = counterString.split(",");
        try {
            for (String counter : counterKeyValues) {
                String[] keyVals = counter.split(":", 2);
                vertex.setProperty(keyVals[0], Long.parseLong(keyVals[1]));
            }
        } catch (NumberFormatException e) {
            throw new FalconException("Invalid values for counter:" + e);
        }
    }

    public void addInstanceToEntity(Vertex instanceVertex, String entityName,
                                    RelationshipType entityType, RelationshipLabel edgeLabel) {
        addInstanceToEntity(instanceVertex, entityName, entityType, edgeLabel, null);
    }

    public void addInstanceToEntity(Vertex instanceVertex, String entityName,
                                    RelationshipType entityType, RelationshipLabel edgeLabel,
                                    Map<RelationshipProperty, String> properties) {
        Vertex entityVertex = findVertex(entityName, entityType);
        LOG.info("Vertex exists? name={}, type={}, v={}", entityName, entityType, entityVertex);
        if (entityVertex == null) {
            LOG.error("Illegal State: {} vertex must exist for {}", entityType, entityName);
            throw new IllegalStateException(entityType + " entity vertex must exist " + entityName);
        }
        addEdge(instanceVertex, entityVertex, edgeLabel.getName(), properties);
    }

    public void addOutputFeedInstances(WorkflowExecutionContext context,
                                       Vertex processInstance) throws FalconException {
        String outputFeedNamesArg = context.getOutputFeedNames();
        if (NONE.equals(outputFeedNamesArg) || IGNORE.equals(outputFeedNamesArg)) {
            return; // there are no output feeds for this process
        }

        String[] outputFeedNames = context.getOutputFeedNamesList();
        String[] outputFeedInstancePaths = context.getOutputFeedInstancePathsList();

        for (int index = 0; index < outputFeedNames.length; index++) {
            String feedName = outputFeedNames[index];
            String feedInstanceDataPath = outputFeedInstancePaths[index];
            addFeedInstance(processInstance, RelationshipLabel.PROCESS_FEED_EDGE,
                    context, feedName, feedInstanceDataPath);
        }
    }

    public void addInputFeedInstances(WorkflowExecutionContext context,
                                      Vertex processInstance) throws FalconException {
        String inputFeedNamesArg = context.getInputFeedNames();
        if (NONE.equals(inputFeedNamesArg) || IGNORE.equals(inputFeedNamesArg)) {
            return; // there are no input feeds for this process
        }

        String[] inputFeedNames = context.getInputFeedNamesList();
        String[] inputFeedInstancePaths = context.getInputFeedInstancePathsList();

        for (int index = 0; index < inputFeedNames.length; index++) {
            String inputFeedName = inputFeedNames[index];
            String inputFeedInstancePath = inputFeedInstancePaths[index];
            // Multiple instance paths for a given feed is separated by ","
            String[] feedInstancePaths = inputFeedInstancePath.split(",");

            for (String feedInstanceDataPath : feedInstancePaths) {
                addFeedInstance(processInstance, RelationshipLabel.FEED_PROCESS_EDGE,
                        context, inputFeedName, feedInstanceDataPath);
            }
        }
    }

    public void addReplicatedInstance(WorkflowExecutionContext context) throws FalconException {
        // For replication there will be only one output feed name and path
        String feedName = context.getOutputFeedNames();
        String feedInstanceDataPath = context.getOutputFeedInstancePaths();
        String targetClusterName = context.getClusterName();

        LOG.info("Computing feed instance for : name= {} path= {}, in cluster: {}", feedName,
                feedInstanceDataPath, targetClusterName);
        String feedInstanceName = getFeedInstanceName(feedName, targetClusterName,
                feedInstanceDataPath, context.getNominalTimeAsISO8601());
        Vertex feedInstanceVertex = addFeedInstance(
                feedInstanceName, context, feedName, context.getSrcClusterName());

        Map<RelationshipProperty, String> properties = edgePropertiesForIndexing(context);
        properties.put(RelationshipProperty.TIMESTAMP, context.getTimeStampAsISO8601());
        addInstanceToEntity(feedInstanceVertex, targetClusterName, RelationshipType.CLUSTER_ENTITY,
                RelationshipLabel.FEED_CLUSTER_REPLICATED_EDGE, properties);

        addCounters(feedInstanceVertex, context);
    }

    public void addEvictedInstance(WorkflowExecutionContext context) throws FalconException {
        final String outputFeedPaths = context.getOutputFeedInstancePaths();
        if (IGNORE.equals(outputFeedPaths)) {
            LOG.info("There were no evicted instances, nothing to record");
            return;
        }

        LOG.info("Recording lineage for evicted instances {}", outputFeedPaths);
        // For retention there will be only one output feed name
        String feedName = context.getOutputFeedNames();
        String[] evictedFeedInstancePathList = context.getOutputFeedInstancePathsList();
        String clusterName = context.getClusterName();

        for (String evictedFeedInstancePath : evictedFeedInstancePathList) {
            LOG.info("Computing feed instance for : name= {}, path={}, in cluster: {}",
                    feedName, evictedFeedInstancePath, clusterName);
            String feedInstanceName = getFeedInstanceName(feedName, clusterName,
                    evictedFeedInstancePath, context.getNominalTimeAsISO8601());
            Vertex feedInstanceVertex = addFeedInstance(
                    feedInstanceName, context, feedName, clusterName);

            Map<RelationshipProperty, String> properties = edgePropertiesForIndexing(context);
            properties.put(RelationshipProperty.TIMESTAMP, context.getTimeStampAsISO8601());
            addInstanceToEntity(feedInstanceVertex, clusterName, RelationshipType.CLUSTER_ENTITY,
                    RelationshipLabel.FEED_CLUSTER_EVICTED_EDGE, properties);
        }
    }

    public void addImportedInstance(WorkflowExecutionContext context) throws FalconException {
        addImportExportInstanceHelper(context, RelationshipLabel.DATASOURCE_IMPORT_EDGE);
    }

    public void addExportedInstance(WorkflowExecutionContext context) throws FalconException {
        addImportExportInstanceHelper(context, RelationshipLabel.DATASOURCE_EXPORT_EDGE);
    }

    private void addImportExportInstanceHelper(WorkflowExecutionContext context,
                                               RelationshipLabel label) throws FalconException {
        String feedName = (label == RelationshipLabel.DATASOURCE_IMPORT_EDGE)
                ? context.getOutputFeedNames() : context.getInputFeedNames();
        String feedInstanceDataPath = (label == RelationshipLabel.DATASOURCE_IMPORT_EDGE)
                ? context.getOutputFeedInstancePaths() : context.getInputFeedInstancePaths();
        String datasourceName = context.getDatasourceName();
        String sourceClusterName = context.getSrcClusterName();

        LOG.info("Computing {} feed instance for : name= {} path= {}, in cluster: {} "
                        +  "from datasource: {}", label.getName(), feedName,
                feedInstanceDataPath, sourceClusterName, datasourceName);

        String feedInstanceName = getFeedInstanceName(feedName, sourceClusterName,
                feedInstanceDataPath, context.getNominalTimeAsISO8601());
        Vertex feedInstanceVertex = addFeedInstance(
                feedInstanceName, context, feedName, context.getSrcClusterName());

        Map<RelationshipProperty, String> properties = edgePropertiesForIndexing(context);
        properties.put(RelationshipProperty.TIMESTAMP, context.getTimeStampAsISO8601());
        addInstanceToEntity(feedInstanceVertex, datasourceName, RelationshipType.DATASOURCE_ENTITY,
                label, properties);
        addInstanceToEntity(feedInstanceVertex, sourceClusterName, RelationshipType.CLUSTER_ENTITY,
                RelationshipLabel.FEED_CLUSTER_EDGE, properties);
    }

    private void addFeedInstance(Vertex processInstance, RelationshipLabel edgeLabel,
                                 WorkflowExecutionContext context, String feedName,
                                 String feedInstanceDataPath) throws FalconException {
        String clusterName = context.getClusterName();
        LOG.info("Computing feed instance for : name= {} path= {}, in cluster: {}", feedName,
                feedInstanceDataPath, clusterName);
        String feedInstanceName = getFeedInstanceName(feedName, clusterName,
                feedInstanceDataPath, context.getNominalTimeAsISO8601());
        Vertex feedInstance = addFeedInstance(feedInstanceName, context, feedName, clusterName, false);
        addProcessFeedEdge(processInstance, feedInstance, edgeLabel);
    }

    private Vertex addFeedInstance(String feedInstanceName, WorkflowExecutionContext context,
                                   String feedName, String clusterName) throws FalconException {
        return addFeedInstance(feedInstanceName, context, feedName, clusterName, true);
    }

    private Vertex addFeedInstance(String feedInstanceName, WorkflowExecutionContext context, String feedName,
                                   String clusterName, boolean hasEdgeProperties) throws FalconException {
        LOG.info("Adding feed instance {}", feedInstanceName);
        Vertex feedInstance = addVertex(feedInstanceName, RelationshipType.FEED_INSTANCE,
                context.getTimeStampAsLong());
        feedInstance.setProperty(RelationshipProperty.STATUS.getName(), context.getValue(WorkflowExecutionArgs.STATUS));

        if (hasEdgeProperties) {
            Map<RelationshipProperty, String> properties = edgePropertiesForIndexing(context);
            addInstanceToEntity(feedInstance, feedName,
                    RelationshipType.FEED_ENTITY, RelationshipLabel.INSTANCE_ENTITY_EDGE, properties);
        } else {
            addInstanceToEntity(feedInstance, feedName,
                    RelationshipType.FEED_ENTITY, RelationshipLabel.INSTANCE_ENTITY_EDGE);
        }
        addInstanceToEntity(feedInstance, clusterName,
                RelationshipType.CLUSTER_ENTITY, RelationshipLabel.FEED_CLUSTER_EDGE);
        addInstanceToEntity(feedInstance, context.getWorkflowUser(),
                RelationshipType.USER, RelationshipLabel.USER);

        if (isPreserveHistory()) {
            Feed feed = ConfigurationStore.get().get(EntityType.FEED, feedName);
            addDataClassification(feed.getTags(), feedInstance);
            addGroups(feed.getGroups(), feedInstance);
        }

        return feedInstance;
    }

    private Map<RelationshipProperty, String> edgePropertiesForIndexing(WorkflowExecutionContext context) {
        Map<RelationshipProperty, String> properties = new HashMap<RelationshipProperty, String>();
        properties.put(RelationshipProperty.NOMINAL_TIME, context.getNominalTimeAsISO8601());
        properties.put(RelationshipProperty.STATUS, context.getValue(WorkflowExecutionArgs.STATUS));
        return properties;
    }

    public static String getFeedInstanceName(String feedName, String clusterName,
                                             String feedInstancePath,
                                             String nominalTime) throws FalconException {
        try {
            Feed feed = ConfigurationStore.get().get(EntityType.FEED, feedName);
            Cluster cluster = ConfigurationStore.get().get(EntityType.CLUSTER, clusterName);

            Storage.TYPE storageType = FeedHelper.getStorageType(feed, cluster);
            return storageType == Storage.TYPE.TABLE
                    ? getTableFeedInstanceName(feed, feedInstancePath, storageType)
                    : getFileSystemFeedInstanceName(feedInstancePath, feed, cluster, nominalTime);

        } catch (URISyntaxException e) {
            throw new FalconException(e);
        }
    }

    private static String getTableFeedInstanceName(Feed feed, String feedInstancePath,
                                            Storage.TYPE storageType) throws URISyntaxException {
        CatalogStorage instanceStorage = (CatalogStorage) FeedHelper.createStorage(
                storageType.name(), feedInstancePath);
        return feed.getName() + "/" + instanceStorage.toPartitionAsPath();
    }

    private static String getFileSystemFeedInstanceName(String feedInstancePath, Feed feed,
                                                        Cluster cluster,
                                                        String nominalTime) throws FalconException {
        Storage rawStorage = FeedHelper.createStorage(cluster, feed);
        String feedPathTemplate = rawStorage.getUriTemplate(LocationType.DATA);
        String instance = feedInstancePath;

        String[] elements = FeedDataPath.PATTERN.split(feedPathTemplate);
        for (String element : elements) {
            instance = instance.replaceFirst(element, "");
        }

        Date instanceTime = FeedHelper.getDate(feedPathTemplate,
                new Path(feedInstancePath), TimeZone.getTimeZone("UTC"));

        return StringUtils.isEmpty(instance)
                ? feed.getName() + "/" + nominalTime
                : feed.getName() + "/"
                        + SchemaHelper.formatDateUTC(instanceTime);
    }
}
