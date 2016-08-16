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

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphFactory;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.TransactionRetryHelper;
import com.tinkerpop.blueprints.util.TransactionWork;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.falcon.FalconException;
import org.apache.falcon.FalconRuntimException;
import org.apache.falcon.entity.store.ConfigurationStore;
import org.apache.falcon.entity.v0.Entity;
import org.apache.falcon.entity.v0.EntityType;
import org.apache.falcon.service.ConfigurationChangeListener;
import org.apache.falcon.service.FalconService;
import org.apache.falcon.service.Services;
import org.apache.falcon.util.StartupProperties;
import org.apache.falcon.workflow.WorkflowJobEndNotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.falcon.workflow.WorkflowExecutionContext;
import org.apache.falcon.workflow.WorkflowExecutionListener;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Metadata relationship mapping service. Maps relationships into a graph database.
 */
public class MetadataMappingService
        implements FalconService, ConfigurationChangeListener, WorkflowExecutionListener {

    private static final Logger LOG = LoggerFactory.getLogger(MetadataMappingService.class);

    /**
     * Constance for the service name.
     */
    public static final String SERVICE_NAME = MetadataMappingService.class.getSimpleName();

    /**
     * Constant for the configuration property that indicates the prefix.
     */
    private static final String FALCON_PREFIX = "falcon.graph.";
    /**
     * Constant for the configuration property that indicates the storage backend.
     */
    public static final String PROPERTY_KEY_STORAGE_BACKEND = "storage.backend";
    public static final String STORAGE_BACKEND_HBASE = "hbase";
    public static final String STORAGE_BACKEND_BDB = "berkeleyje";
    /**
     * HBase configuration properties.
     */
    public static final String PROPERTY_KEY_STORAGE_HOSTNAME = "storage.hostname";
    public static final String PROPERTY_KEY_STORAGE_TABLE = "storage.hbase.table";
    public static final Set<String> PROPERTY_KEYS_HBASE = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            PROPERTY_KEY_STORAGE_HOSTNAME, PROPERTY_KEY_STORAGE_TABLE)));
    /**
     * Berkeley DB configuration properties.
     */
    public static final String PROPERTY_KEY_STORAGE_DIRECTORY = "storage.directory";
    public static final String PROPERTY_KEY_SERIALIZE_PATH = "serialize.path";
    public static final Set<String> PROPERTY_KEYS_BDB = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
            PROPERTY_KEY_STORAGE_DIRECTORY, PROPERTY_KEY_SERIALIZE_PATH)));

    private Graph graph;
    private Set<String> vertexIndexedKeys;
    private Set<String> edgeIndexedKeys;
    private EntityRelationshipGraphBuilder entityGraphBuilder;
    private InstanceRelationshipGraphBuilder instanceGraphBuilder;

    private int transactionRetries;
    private long transactionRetryDelayInMillis;

    @Override
    public String getName() {
        return SERVICE_NAME;
    }

    @Override
    public void init() throws FalconException {
        graph = initializeGraphDB();
        createIndicesForVertexKeys();
        // todo - create Edge Cardinality Constraints
        LOG.info("Initialized graph db: {}", graph);

        vertexIndexedKeys = getIndexableGraph().getIndexedKeys(Vertex.class);
        LOG.info("Init vertex property keys: {}", vertexIndexedKeys);

        edgeIndexedKeys = getIndexableGraph().getIndexedKeys(Edge.class);
        LOG.info("Init edge property keys: {}", edgeIndexedKeys);

        boolean preserveHistory = Boolean.valueOf(StartupProperties.get().getProperty(
                "falcon.graph.preserve.history", "false"));
        entityGraphBuilder = new EntityRelationshipGraphBuilder(graph, preserveHistory);
        instanceGraphBuilder = new InstanceRelationshipGraphBuilder(graph, preserveHistory);

        ConfigurationStore.get().registerListener(this);
        Services.get().<WorkflowJobEndNotificationService>getService(
                WorkflowJobEndNotificationService.SERVICE_NAME).registerListener(this);
        try {
            transactionRetries = Integer.parseInt(StartupProperties.get().getProperty(
                    "falcon.graph.transaction.retry.count", "3"));
            transactionRetryDelayInMillis = Long.parseLong(StartupProperties.get().getProperty(
                    "falcon.graph.transaction.retry.delay", "5"));
        } catch (NumberFormatException e) {
            throw new FalconException("Invalid values for graph transaction retry delay/count " + e);
        }
    }

    public static Graph initializeGraphDB() {
        LOG.info("Initializing graph db");
        Configuration graphConfig = getConfiguration();
        validateConfiguration(graphConfig);
        return GraphFactory.open(graphConfig);
    }

    private static void validateConfiguration(Configuration graphConfig) {
        // check if storage backend if configured
        if (!graphConfig.containsKey(PROPERTY_KEY_STORAGE_BACKEND)) {
            throw new FalconRuntimException("Titan GraphDB storage backend is not configured. "
                    + "You need to choose either hbase or berkeleydb."
                    + "Please check Configuration twiki or "
                    + "the section Graph Database Properties in startup.properties "
                    + "on how to configure Titan GraphDB backend.");
        }

        String backend = graphConfig.getString(PROPERTY_KEY_STORAGE_BACKEND);
        switch (backend) {
        case STORAGE_BACKEND_BDB:
            // check required parameter for Berkeley DB backend
            for (String key : PROPERTY_KEYS_BDB) {
                if (!graphConfig.containsKey(key)) {
                    throw new FalconRuntimException("Required parameter " + FALCON_PREFIX + key
                            + " not found in startup.properties."
                            + "Please check Configuration twiki or "
                            + "the section Graph Database Properties in startup.properties "
                            + "on how to configure Berkeley DB storage backend.");
                }
            }
            break;
        case STORAGE_BACKEND_HBASE:
            // check required parameter for HBase backend
            for (String key : PROPERTY_KEYS_HBASE) {
                if (!graphConfig.containsKey(key)) {
                    throw new FalconRuntimException("Required parameter " + FALCON_PREFIX + key
                            + " not found in startup.properties."
                            + "Please check Configuration twiki or "
                            + "the section Graph Database Properties in startup.properties "
                            + "on how to configure HBase storage backend.");
                }
            }
            break;
        default:
            throw new FalconRuntimException("Invalid graph storage backend: " + backend + ". "
                    + "You need to choose either hbase or berkeleydb."
                    + "Please check Configuration twiki or "
                    + "the section Graph Database Properties in startup.properties "
                    + "on how to configure Titan GraphDB backend.");
        }
    }

    public static Configuration getConfiguration() {
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

    /**
     * This unfortunately requires a handle to Titan implementation since
     * com.tinkerpop.blueprints.KeyIndexableGraph#createKeyIndex does not create an index.
     */
    protected void createIndicesForVertexKeys() {
        if (!((KeyIndexableGraph) graph).getIndexedKeys(Vertex.class).isEmpty()) {
            LOG.info("Indexes already exist for graph");
            return;
        }

        LOG.info("Indexes does not exist, Creating indexes for graph");
        // todo - externalize this
        makeNameKeyIndex();
        makeKeyIndex(RelationshipProperty.TYPE.getName());
        makeKeyIndex(RelationshipProperty.TIMESTAMP.getName());
        makeKeyIndex(RelationshipProperty.VERSION.getName());
        makeInstanceIndex();
    }

    private void makeInstanceIndex() {
        // build index for instance search
        TitanManagement titanManagement = getTitanGraph().getManagementSystem();
        PropertyKey statusKey = makePropertyKey(titanManagement, RelationshipProperty.STATUS.getName());
        PropertyKey nominalTimeKey = makePropertyKey(titanManagement, RelationshipProperty.NOMINAL_TIME.getName());
        EdgeLabel edgeLabel = titanManagement.makeEdgeLabel(RelationshipLabel.INSTANCE_ENTITY_EDGE.getName()).make();
        titanManagement.buildEdgeIndex(edgeLabel, "indexInstanceN", Direction.OUT, Order.DESC, nominalTimeKey);
        titanManagement.buildEdgeIndex(edgeLabel, "indexInstanceSN", Direction.OUT, Order.DESC,
                statusKey, nominalTimeKey);
        titanManagement.commit();
    }

    private void makeNameKeyIndex() {
        TitanManagement titanManagement = getTitanGraph().getManagementSystem();
        PropertyKey nameKey = makePropertyKey(titanManagement, RelationshipProperty.NAME.getName());
        titanManagement.buildIndex("indexByVertexName", Vertex.class).addKey(nameKey).buildCompositeIndex();
        titanManagement.buildIndex("indexByEdgeName", Edge.class).addKey(nameKey).buildCompositeIndex();
        titanManagement.commit();
    }

    private void makeKeyIndex(String key) {
        TitanManagement titanManagement = getTitanGraph().getManagementSystem();
        PropertyKey propertyKey = makePropertyKey(titanManagement, key);
        titanManagement.buildIndex("indexBy" + key, Vertex.class).addKey(propertyKey).buildCompositeIndex();
        titanManagement.commit();
    }

    private PropertyKey makePropertyKey(TitanManagement titanManagement, String key) {
        if (titanManagement.containsPropertyKey(key)) {
            return titanManagement.getPropertyKey(key);
        }
        return titanManagement.makePropertyKey(key).dataType(String.class).make();
    }

    public Graph getGraph() {
        return graph;
    }

    public KeyIndexableGraph getIndexableGraph() {
        return (KeyIndexableGraph) graph;
    }

    public TransactionalGraph getTransactionalGraph() {
        return (TransactionalGraph) graph;
    }

    public TitanBlueprintsGraph getTitanGraph() {
        return (TitanBlueprintsGraph) graph;
    }

    public Set<String> getVertexIndexedKeys() {
        return vertexIndexedKeys;
    }

    public Set<String> getEdgeIndexedKeys() {
        return edgeIndexedKeys;
    }

    @Override
    public void destroy() throws FalconException {
        Services.get().<WorkflowJobEndNotificationService>getService(
                WorkflowJobEndNotificationService.SERVICE_NAME).unregisterListener(this);

        LOG.info("Shutting down graph db");
        graph.shutdown();
    }

    @Override
    public void onAdd(final Entity entity) throws FalconException {
        EntityType entityType = entity.getEntityType();
        LOG.info("Adding lineage for entity: {}, type: {}", entity.getName(), entityType);
        try {
            new TransactionRetryHelper.Builder<Void>(getTransactionalGraph())
                    .perform(new TransactionWork<Void>() {
                        @Override
                        public Void execute(TransactionalGraph transactionalGraph) throws Exception {
                            entityGraphBuilder.addEntity(entity);
                            transactionalGraph.commit();
                            return null;
                        }
                    }).build().exponentialBackoff(transactionRetries, transactionRetryDelayInMillis);

        } catch (Exception e) {
            getTransactionalGraph().rollback();
            throw new FalconException(e);
        }
    }

    @Override
    public void onRemove(Entity entity) throws FalconException {
        // do nothing, we'd leave the deleted entities as-is for historical purposes
        // should we mark 'em as deleted?
    }

    @Override
    public void onChange(final Entity oldEntity, final Entity newEntity) throws FalconException {
        EntityType entityType = newEntity.getEntityType();
        LOG.info("Updating lineage for entity: {}, type: {}", newEntity.getName(), entityType);
        try {
            new TransactionRetryHelper.Builder<Void>(getTransactionalGraph())
                    .perform(new TransactionWork<Void>() {
                        @Override
                        public Void execute(TransactionalGraph transactionalGraph) throws Exception {
                            entityGraphBuilder.updateEntity(oldEntity, newEntity);
                            transactionalGraph.commit();
                            return null;
                        }
                    }).build().exponentialBackoff(transactionRetries, transactionRetryDelayInMillis);

        } catch (Exception e) {
            getTransactionalGraph().rollback();
            throw new FalconException(e);
        }
    }

    @Override
    public void onReload(Entity entity) throws FalconException {
        onAdd(entity);
    }

    @Override
    public void onStart(final WorkflowExecutionContext context) throws FalconException {
        LOG.info("onStart {}", context);
        onInstanceExecutionUpdate(context);
    }

    @Override
    public void onSuccess(final WorkflowExecutionContext context) throws FalconException {
        LOG.info("onSuccess {}", context);
        onInstanceExecutionUpdate(context);
    }

    @Override
    public void onFailure(final WorkflowExecutionContext context) throws FalconException {
        LOG.info("onFailure {}", context);
        onInstanceExecutionUpdate(context);
    }

    @Override
    public void onSuspend(final WorkflowExecutionContext context) throws FalconException {
        LOG.info("onSuspend {}", context);
        onInstanceExecutionUpdate(context);
    }

    @Override
    public void onWait(final WorkflowExecutionContext context) throws FalconException {
        LOG.info("onWait {}", context);
        onInstanceExecutionUpdate(context);
    }

    private void onInstanceExecutionUpdate(final WorkflowExecutionContext context) throws FalconException {
        try {
            new TransactionRetryHelper.Builder<Void>(getTransactionalGraph())
                    .perform(new TransactionWork<Void>() {
                        @Override
                        public Void execute(TransactionalGraph transactionalGraph) throws Exception {
                            updateInstanceStatus(context);
                            transactionalGraph.commit();
                            return null;
                        }
                    }).build().exponentialBackoff(transactionRetries, transactionRetryDelayInMillis);
        } catch (Exception e) {
            getTransactionalGraph().rollback();
            throw new FalconException(e);
        }
    }

    private void updateInstanceStatus(final WorkflowExecutionContext context) throws FalconException {
        if (context.getContextType() == WorkflowExecutionContext.Type.COORDINATOR_ACTION) {
            // TODO(yzheng): FALCON-1776 Instance update on titan DB based on JMS notifications on coordinator actions
            return;
        }

        WorkflowExecutionContext.EntityOperations entityOperation = context.getOperation();
        switch (entityOperation) {
        case GENERATE:
            updateProcessInstance(context);
            break;
        case REPLICATE:
            updateReplicatedFeedInstance(context);
            break;
        case DELETE:
            updateEvictedFeedInstance(context);
            break;
        case IMPORT:
            updateImportedFeedInstance(context);
            break;
        case EXPORT:
            updateExportedFeedInstance(context);
            break;
        default:
            throw new IllegalArgumentException("Invalid EntityOperation - " + entityOperation);
        }
    }

    private void updateProcessInstance(WorkflowExecutionContext context) throws FalconException {
        LOG.info("Updating process instance: {}", context.getNominalTimeAsISO8601());
        Vertex processInstance = instanceGraphBuilder.addProcessInstance(context);
        instanceGraphBuilder.addOutputFeedInstances(context, processInstance);
        instanceGraphBuilder.addInputFeedInstances(context, processInstance);
    }

    private void updateReplicatedFeedInstance(WorkflowExecutionContext context) throws FalconException {
        LOG.info("Updating replicated feed instance: {}", context.getNominalTimeAsISO8601());
        instanceGraphBuilder.addReplicatedInstance(context);
    }

    private void updateEvictedFeedInstance(WorkflowExecutionContext context) throws FalconException {
        LOG.info("Updating evicted feed instance: {}", context.getNominalTimeAsISO8601());
        instanceGraphBuilder.addEvictedInstance(context);
    }
    private void updateImportedFeedInstance(WorkflowExecutionContext context) throws FalconException {
        LOG.info("Updating imported feed instance: {}", context.getNominalTimeAsISO8601());
        instanceGraphBuilder.addImportedInstance(context);
    }
    private void updateExportedFeedInstance(WorkflowExecutionContext context) throws FalconException {
        LOG.info("Updating export feed instance: {}", context.getNominalTimeAsISO8601());
        instanceGraphBuilder.addExportedInstance(context);
    }
}
