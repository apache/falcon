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

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.falcon.FalconException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.entity.v0.cluster.Cluster;
import org.apache.falcon.entity.v0.feed.Feed;
import org.apache.falcon.entity.v0.process.Process;
import org.apache.falcon.service.ConfigurationChangeListener;
import org.apache.falcon.service.FalconService;
import org.apache.falcon.util.StartupProperties;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Metadata relationship mapping service. Maps relationships into a graph database.
 */
public class MetadataMappingService implements FalconService, ConfigurationChangeListener {

    private static final Logger LOG = Logger.getLogger(MetadataMappingService.class);

    /**
     * Entity operations.
     */
    public enum EntityOperations {
        GENERATE, REPLICATE, DELETE
    }

    /**
     * Constance for the service name.
     */
    public static final String SERVICE_NAME = MetadataMappingService.class.getSimpleName();

    /**
     * Constant for the configuration property that indicates the prefix.
     */
    private static final String FALCON_PREFIX = "falcon.graph.";

    private KeyIndexableGraph graph;
    private Set<String> vertexIndexedKeys;
    private Set<String> edgeIndexedKeys;
    private EntityRelationshipGraphBuilder entityGraphBuilder;
    private InstanceRelationshipGraphBuilder instanceGraphBuilder;

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws FalconException {
        graph = initializeGraphDB();
        LOG.info("Initialized graph db: " + graph);

        vertexIndexedKeys = graph.getIndexedKeys(Vertex.class);
        LOG.info("Init vertex property keys: " + vertexIndexedKeys);

        edgeIndexedKeys = graph.getIndexedKeys(Edge.class);
        LOG.info("Init edge property keys: " + edgeIndexedKeys);

        boolean preserveHistory = Boolean.valueOf(StartupProperties.get().getProperty(
                "falcon.graph.preserve.history", "false"));
        entityGraphBuilder = new EntityRelationshipGraphBuilder(graph, preserveHistory);
        instanceGraphBuilder = new InstanceRelationshipGraphBuilder(graph, preserveHistory);

        ConfigurationStore.get().registerListener(this);
    }

    protected KeyIndexableGraph initializeGraphDB() {
        LOG.info("Initializing graph db");

        Configuration graphConfig = getConfiguration();
        KeyIndexableGraph graphDB = (KeyIndexableGraph) GraphFactory.open(graphConfig);
        createIndicesForVertexKeys(graphDB);
        return graphDB;
    }

    public Configuration getConfiguration() {
        Configuration graphConfig = new BaseConfiguration();

        Properties configProperties = StartupProperties.get();
        for (Map.Entry entry : configProperties.entrySet()) {
            String name = (String) entry.getKey();
            if (name.startsWith(FALCON_PREFIX)) {
                String value = (String) entry.getValue();
                name = name.substring(FALCON_PREFIX.length());
                graphConfig.setProperty(name, value);
            }
        }

        return graphConfig;
    }

    protected void createIndicesForVertexKeys(KeyIndexableGraph graphDB) {
        LOG.info("Creating indexes for graph");
        // todo - externalize this
        graphDB.createKeyIndex(RelationshipGraphBuilder.NAME_PROPERTY_KEY, Vertex.class);
        graphDB.createKeyIndex(RelationshipGraphBuilder.TYPE_PROPERTY_KEY, Vertex.class);
        graphDB.createKeyIndex(RelationshipGraphBuilder.VERSION_PROPERTY_KEY, Vertex.class);
        graphDB.createKeyIndex(RelationshipGraphBuilder.TIMESTAMP_PROPERTY_KEY, Vertex.class);
    }

    public KeyIndexableGraph getGraph() {
        return graph;
    }

    public Set<String> getVertexIndexedKeys() {
        return vertexIndexedKeys;
    }

    public Set<String> getEdgeIndexedKeys() {
        return edgeIndexedKeys;
    }

    @Override
    public void destroy() throws FalconException {
        LOG.info("Shutting down graph db");
        graph.shutdown();
    }

    @Override
    public void onAdd(Entity entity) throws FalconException {
        EntityType entityType = entity.getEntityType();
        LOG.info("Adding lineage for entity: " + entity.getName() + ", type: " + entityType);

        switch (entityType) {
        case CLUSTER:
            entityGraphBuilder.addClusterEntity((Cluster) entity);
            break;

        case FEED:
            entityGraphBuilder.addFeedEntity((Feed) entity);
            break;

        case PROCESS:
            entityGraphBuilder.addProcessEntity((Process) entity);
            break;

        default:
        }
    }

    @Override
    public void onRemove(Entity entity) throws FalconException {
        // do nothing, we'd leave the deleted entities as-is for historical purposes
        // should we mark 'em as deleted?
    }

    @Override
    public void onChange(Entity oldEntity, Entity newEntity) throws FalconException {
        EntityType entityType = newEntity.getEntityType();
        LOG.info("Updating lineage for entity: " + newEntity.getName() + ", type: " + entityType);

        switch (entityType) {
        case CLUSTER:
            // a cluster cannot be updated
            break;

        case FEED:
            entityGraphBuilder.updateFeedEntity((Feed) oldEntity, (Feed) newEntity);
            break;

        case PROCESS:
            entityGraphBuilder.updateProcessEntity((Process) oldEntity, (Process) newEntity);
            break;

        default:
        }
    }

    @Override
    public void onReload(Entity entity) throws FalconException {
        // do nothing since entities are being loaded from store into memory and
        // are already added to the graph
    }

    public void mapLineage(String entityName, String operation,
                           String logDir) throws FalconException {
        String lineageFile = LineageRecorder.getFilePath(logDir, entityName);

        LOG.info("Parsing lineage metadata from: " + lineageFile);
        Map<String, String> lineageMetadata = LineageRecorder.parseLineageMetadata(lineageFile);

        EntityOperations entityOperation = EntityOperations.valueOf(operation);

        LOG.info("Adding lineage for entity: " + entityName + ", operation: " + operation);
        switch (entityOperation) {
        case GENERATE:
            onProcessInstanceAdded(lineageMetadata);
            break;

        case REPLICATE:
            onFeedInstanceReplicated(lineageMetadata);
            break;

        case DELETE:
            onFeedInstanceEvicted(lineageMetadata);
            break;

        default:
        }
    }

    private void onProcessInstanceAdded(Map<String, String> lineageMetadata) throws FalconException {
        Vertex processInstance = instanceGraphBuilder.addProcessInstance(lineageMetadata);
        instanceGraphBuilder.addOutputFeedInstances(lineageMetadata, processInstance);
        instanceGraphBuilder.addInputFeedInstances(lineageMetadata, processInstance);
    }

    private void onFeedInstanceReplicated(Map<String, String> lineageMetadata) {
        LOG.info("Adding replicated feed instance: " + lineageMetadata.get(LineageArgs.NOMINAL_TIME.getOptionName()));
        // todo - tbd
    }

    private void onFeedInstanceEvicted(Map<String, String> lineageMetadata) {
        LOG.info("Adding evicted feed instance: " + lineageMetadata.get(LineageArgs.NOMINAL_TIME.getOptionName()));
        // todo - tbd
    }
}
